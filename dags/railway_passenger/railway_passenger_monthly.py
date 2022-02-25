from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import camelot.io as camelot
import os 
import dateparser
import pandas as pd 
import PyPDF2 
import numpy as np
from bs4 import BeautifulSoup
import requests
from lxml import html

from bipp.sharepoint.uploads import upload_file  



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['snehal_bhartiya@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=1),
}
with DAG(
    'railway_passenger_monthly',
    default_args=default_args,
    description='railway_passenger_monthly',
    schedule_interval = '@monthly',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=12, day=18, hour=12, minute=0),
    catchup = True,
    tags=['railway_passenger'],

) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'electricity')
    data_path = os.path.join(dir_path, 'daily')
    os.makedirs(data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    # finding the page number using the the function fnPDF_FindText 
    def fnPDF_FindText(xFile, xString):
    #     xfile : the PDF file in which to look
    #     xString : the string to look for
        pdfDoc = pyPdf.PdfFileReader(xFile)
        for i in range(0, pdfDoc.getNumPages()):
            content = ""
            try:
                content += pdfDoc.getPage(i).extractText() 
                ResSearch=content.find(xString)
            except KeyError:
                continue
            if ResSearch !=-1:
                page_found = i+1
                return page_found

    #get a list of file names
    link= 'https://indianrailways.gov.in/railwayboard/view_section.jsp?lang=0&id=0,1,304,366,554,1361'

    def download_railway_data(ds,**context):
        try:
            # Extracting links
            session = requests.Session()
            response = session.get(link)
            tree = html.fromstring(response.content)
            # extracting all the links for the data available
            all_links = [a for a in tree.xpath('//*[@id="table57"]//a/@href')]
            all_links

            jan_2019_data_index=all_links.index( '/railwayboard/uploads/directorate/stat_econ/MTHSTAT/2019/MER_Jan_2019.pdf')
            new_link_list=[]
            for links in all_links[:jan_2019_data_index+1]:
                new_link='https://indianrailways.gov.in'+links
                new_link_list.append(new_link)
            new_link_list

             #downloading all files- needs to be run once
            local_files=[]
            for link in new_link_list: 
                r=requests.Session()
                response=r.get(link)
                local_path=os.path.join(raw_data_path,os.path.basename(link))
                with open(local_path, 'wb') as f:
                    f.write(response.content)    
                session.close()
                local_files.append(local_path)
            return local_files

        except Exception as e: 
            return ValueError(e)

    download_railway_data_task=PythonOperator(
        task_id='download_railway_data',
        python_callable=download_railway_data,
        depends_on_past=True)

    def process_railway_passenger_monthly(ds,**context):
        df_list=[]
        try:
            for file in local_files: 
                file_path=os.path.join(os.getcwd(),file)
                page_found=fnPDF_FindText(xFile=file_path,xString='NUMBER OF PASSENGERS BOOKED (MILLIONS)')
                tables=camelot.read_pdf(file_path,pages=str(page_found),flavor='stream',table_areas=[f'{x1},{y1},{x2},{y2}'])
                df=tables[0].df
                date_string=df[df.apply(lambda row: row.astype(str).str.contains('For the month',case=False).any(), axis=1)].dropna(axis=1).values[0][11]
                y=date_string.replace('For the month of ','')
                month=re.findall("[a-zA-Z]+",y)[0]
                month
                year=re.findall(r"\d+",y)[0]
                date='01-'+month+'-'+year
                df_1=df[[0,5]]
                df_1['date']=date
                df_1=df_1.iloc[11:27]
                df_1.columns=['railway_zone','number_of_passengers','date']
                df_1=df_1.replace('C','central_zone')
                df_1=df_1.replace(['E','EC','E.CO.'],'eastern_zone')
                df_1=df_1.replace(['N','NC','NE','NF','NW'],'north_zone')
                df_1=df_1.replace(['S','SC','SE','SEC','SW'],'south_zone')
                df_1=df_1.replace(['W','WC'],'western_zone')
                df_1['number_of_passengers']=pd.to_numeric(df_1['number_of_passengers'])
                df_sum=df_1.pivot_table(index='date',values='number_of_passengers',columns='railway_zone',aggfunc=np.sum).reset_index()
                df_sum.date=df_sum.date.str.strip()
            #     df_sum['date']=pd.to_datetime(df_sum['date'],format='%d-%B-%Y')
                df_sum['date']=pd.to_datetime(df_sum['date'])
                df_list.append(df_sum)
            dataframe=pd.concat(df_list)
            dataframe= dataframe.sort_values(by='date')

            last_date=dataframe['date'].max().strftime('%d_%m_%Y')
            dataframe['date']=dataframe['date'].dt.strftime('%d-%m-%y')
            dataframe_copy=dataframe.copy()

            column_names=[]
            for column in dataframe_copy.columns:
                if column=='date':
                    column_names.append(column)
                    pass
                else:
                    x=column+'_india'
                    column_names.append(x)
            column_names
            dataframe_copy.columns=column_names
            dataframe_copy=dataframe_copy[['date', 'central_zone_india','north_zone_india',
                'south_zone_india', 'eastern_zone_india',  'western_zone_india']]

            dataframe_copy.to_csv(os.path.join(dir_path,f'railyway_passengers_monthly_{last_date}.csv'),index=False)

        except Exception as e:
            print('Caught an error:',e)

    process_railway_passenger_monthly_task=PythonOperator(
        task_id='process_railway_passenger_monthly_task',
        python_callable=process_railway_passenger_monthly,
        depends_on_past=True
    )

#setting task dependencies

    download_railway_data_task>>process_railway_passenger_monthly_task