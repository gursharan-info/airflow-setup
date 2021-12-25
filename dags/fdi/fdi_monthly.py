from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os, re
from lxml import html
from calendar import month_name
from tabula import read_pdf
from bipp.sharepoint.uploads import upload_file  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(months=1),
}
with DAG(
    'fdi_monthly',
    default_args=default_args,
    description='FDI Monthly',
    schedule_interval = '0 20 15 * *',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=11, day=2, hour=12, minute=0),
    catchup = True,
    tags=['fdi'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'fdi')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    SECTOR_NAME = 'Industry and Trade'
    DATASET_NAME = 'fdi_monthly'
    day_lag = 1
    

    def get_df(df_list):
        for df in df_list:
            columns = [col.lower() for col in df.columns.tolist()]
            print(columns)
            if 'amount of fdi equity inflows' in columns:
                return df.copy()    
        return None


    def scrape_fdi_monthly(ds, **context):  
        '''
        Scrape the monthly FDI data
        '''
        # print(context['execution_date'])
        curr_date = context['data_interval_start']
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",context['data_interval_start'])   

        # try:
        main_url = "https://dpiit.gov.in/publications/fdi-statistics"

        session = requests.Session()
        response = session.get(main_url, verify = False)
        tree = html.fromstring(response.content)

        session.close()

        text_list = [d.text for d in tree.xpath('//table/tbody/tr/td[@class="views-field views-field-php"]/div')]
        all_links = [a for a in tree.xpath('//table/tbody/tr/td[@class="views-field views-field-php-1"]/div/a/@href')]
        months = {m.lower() for m in month_name[1:]}

        curr_link = None
        for text, link in zip(text_list, all_links):
            try:
                print(text, link)
                start = text.split(' to ')[0]
                start_dict = dict(year = re.sub('[^0-9]', '', start), month=next((word for word in start.split() if word.lower() in months), None))
                end = text.split(' to ')[1]
                end_dict = dict(year = re.sub('[^0-9]', '', end), month=next((word for word in end.split() if word.lower() in months), None))
            #     print(start, start_year, "---", end)
                print(start_dict, end_dict)

                start_date = datetime.strptime(start_dict['month']+"-"+start_dict['year'], '%B-%Y')
                end_date = datetime.strptime(f"30-{end_dict['month']}-{end_dict['year']}", '%d-%B-%Y')
                print(start_date,end_date)
                
                if start_date <= curr_date <= end_date:
                    print("in between")
                    curr_link = link
        #             curr_link = dict(link = link, date = dict(start=start_date, end=end_date))
                    break
            except Exception as e:
                print(e, "link not correct")
                continue
        
        if curr_link:
            print(curr_link)
            r = requests.get(curr_link, stream = True, verify = False)
            raw_file_name = os.path.join(raw_data_path, curr_link.split('/')[-1])
            
            with open(raw_file_name,"wb") as pdf:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        pdf.write(chunk)
            
            # Upload raw file to Sharepoint
            upload_file(raw_file_name, f"{DATASET_NAME}/raw_data", curr_link.split('/')[-1], SECTOR_NAME, "india_pulse")

            tables = read_pdf(raw_file_name,pages='all')
            extracted_df = get_df(tables)

            filtered_df = extracted_df.drop(['Unnamed: 0'], axis=1)
            filtered_df = filtered_df.iloc[:-3]
            filtered_df = filtered_df.drop([0])
            filtered_df.columns = (['date', 'fdi_equity_india'])
            filtered_df['date'] = filtered_df['date'].str.split(".", expand=True)[1].str.strip()
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], format="%B, %Y")
            filtered_df['fdi_equity_india'] = filtered_df['fdi_equity_india'].str.replace(',', '').astype(int)

            # min_mnth = extracted_df['date'].min().strftime("%m%Y")
            # max_mnth = extracted_df['date'].max().strftime("%m%Y")
            filtered_df = filtered_df[filtered_df['date'] == curr_date.strftime("%Y-%m-01")]
            filtered_df['date'] = filtered_df['date'].dt.strftime("%d-%m-%Y")

            filename = os.path.join(monthly_data_path, f"fdi_{curr_date.strftime('%m-%Y')}.csv")
            filtered_df.to_csv(filename, index=False)

            upload_file(filename, DATASET_NAME, f"fdi_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
            # gupload.upload(filename, f"fdi_monthly_{min_mnth}-{max_mnth}.csv", gdrive_fdi_monthly_folder)
            return f"Scraped and processed data for: {curr_date.strftime('%m-%Y')}"

        else:
            print(f"No Data available for: {curr_date.strftime('%m-%Y')}")
            raise ValueError(f"No Data available for: {curr_date.strftime('%m-%Y')}")

        # except requests.exceptions.RequestException as e:
        #     print(e)

    scrape_fdi_monthly_task = PythonOperator(
        task_id='scrape_fdi_monthly',
        python_callable=scrape_fdi_monthly,
        depends_on_past=True
    )



    # # Upload data file 
    # def process_fdi_monthly(ds, **context):  
    #     '''
    #     Upload the process monthly data file on sharepoint
    #     '''
    #     # print(context)
    #     curr_date = context['data_interval_start']
    #     print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

    #     try:
    #         filename = os.path.join(monthly_data_path, f"electricity_{curr_date.strftime('%m-%Y')}.csv")
    #         upload_file(filename, DATASET_NAME, f"electricity_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

    #         return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

    #     except requests.exceptions.RequestException as e:
    #         print(e)


    # process_fdi_monthly_task = PythonOperator(
    #     task_id = 'process_fdi_monthly',
    #     python_callable = process_fdi_monthly,
    #     depends_on_past = True
    # )
    
    # scrape_fdi_monthly_task >> process_fdi_monthly_task

