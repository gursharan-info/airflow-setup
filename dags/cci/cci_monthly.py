from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests, os, glob
import PyPDF2 as pypdf
from tabula import read_pdf
from bipp.sharepoint.uploads import upload_file  
import pandas as pd
import os 
from tabula import read_pdf
import requests
from lxml import html
from bs4 import BeautifulSoup
from datetime import datetime 
import pendulum



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['gursharan_singh@isb.edu'],
    'email': ['snehal_bhartiya@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=1),
}

with DAG(
    'cci_monthly',
    default_args=default_args,
    description='Consumer Confidence Index',
    schedule_interval = '@once',
    start_date = datetime(year=2021, month=12, day=29, hour=7, minute=38),
    catchup = True,
    tags=['consumer_confidence_index'],

) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'cci')
    daily_data_path = os.path.join(dir_path, 'daily')
    os.makedirs(daily_data_path, exist_ok = True)

    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
        
    def cci_processing(ds, **context):
    # curr_date = context['execution_date']
    # print("Scraping for: ",curr_date)   
       

        try:
            curr_date = pendulum.today()
            print("Scraping for: ",curr_date)   
            start = curr_date.start_of('month')
            end = curr_date.end_of('month')
            dates = pd.date_range(start=start.strftime("%m/%d/%Y"), end=end.strftime("%m/%d/%Y"))
            headers ={'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.28 Safari/537.36'}

            for date in dates:
                main_session = requests.session()
                main_session.headers = headers       # <-- set default headers here
                link = f"https://rbidocs.rbi.org.in/rdocs/content/docs/CCSH{date.strftime('%d%m%Y')}.xlsx"
                if not main_session.get(link, verify=False).status_code == 404:
                    excel_file=link  
                    print(link)
                main_session.close()
            
            sheet_names={'T1 Economic Situation':'economic_situation','T2 Employment':'employment',
            'T3 Prices':'prices','T5 Income':'income' ,'T7 Essential spending':'essential_spending',
            'T8 Non-essential spending':'non-essential_spending','T9 Indices':'csi'}
            dataframe=pd.DataFrame()
            dataframe['date']=[]
            for sheets in list(sheet_names.keys()):
                print(sheets)
                column_names=sheet_names[sheets]
                if sheets=='T9 Indices':
                    df2=pd.read_excel(excel_file,sheet_name=sheets)
                    df2=df2[[df2.columns[1],df2.columns[2]]]
                    df2=df2[2:-5]
                    df2.columns=['date',column_names]
                    dataframe=pd.merge(dataframe,df2,how='outer', on='date')
                else:
                    df=pd.read_excel(excel_file,sheet_name=sheets)        
                    filtered = df[[df.columns.tolist()[1], df.columns.tolist()[5]]].copy()
                    filtered=filtered[4:]  
                    filtered.columns=['date',column_names]
                    filtered=filtered.drop(filtered.index[len(filtered)-2])
                    dataframe=pd.merge(dataframe,filtered,how='outer', on='date')
            dataframe_copy=dataframe.copy()
            dataframe_copy=dataframe_copy[:-2]
            dataframe_copy['date']=pd.to_datetime(dataframe_copy['date'])
            last_date=dataframe_copy['date'].max().strftime('%d_%m_%Y')
            dataframe_copy['date']=dataframe_copy['date'].dt.strftime('%d-%m-%Y')
            start=dataframe_copy[dataframe_copy['date']=='01-03-2019'].index[0]
            dataframe_copy=dataframe_copy.loc[start:]
            column_names=[]

            for column in dataframe_copy.columns:
                if column=='date':
                    column_names.append(column)
                    pass
                else:
                    x=column+'_india'
                    column_names.append(x)

            dataframe_copy.columns=column_names
            dataframe_copy=dataframe_copy[['date',  'csi_india','economic_situation_india', 'employment_india', 'prices_india',
                'income_india', 'essential_spending_india',
                'non-essential_spending_india']]

            filename = os.path.join(monthly_data_path, f'consumer_confidence_index_{last_date}.csv')
            print(filename)
            # dataframe_copy.to_csv(filename, index=False)
            dataframe_copy.to_csv(r'C:\Users\31460\idp_hfi_automation\data\IndiaPulse\cci\monthly\test.csv', index=False)

        except Exception as e:
            raise ValueError(e)

    process_cci_monthly_task=PythonOperator(
        task_id='cci_processing',
        python_callable=cci_processing,
        depends_on_past=True)  



    # Upload data file 
    def upload_cci_monthly(ds, **context): 
        print('the second task!') 
        # '''
        # Upload the process monthly data file on sharepoint
        # '''
        # curr_date = context['execution_date']
        # print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        # try:
        #     filename = os.path.join(monthly_data_path, f"cci_{curr_date.strftime('%m-%Y')}.csv")
        #     upload_file(filename, DATASET_NAME, f"cci_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

        #     return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

            # except requests.exceptions.RequestException as e:
            #     print(e)


    upload_cci_monthly_task = PythonOperator(
        task_id = 'upload_cci_monthly',
        python_callable = upload_cci_monthly,
        depends_on_past = True
    )

    process_cci_monthly_task>>upload_cci_monthly_task