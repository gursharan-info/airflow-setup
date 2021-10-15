import pendulum, os, re, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from lxml import html
from calendar import month_name
from tabula import read_pdf

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/fdi'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_fdi_monthly_folder = '1M-zWzTnIv-9qqFQGmiYJodSb8q9xmhZx'
gdrive_fdi_raw_folder = '1BTRowwRXPSnWajvqI6JrE-g6wsFdRx-3'


def get_df(df_list):
    for df in df_list:
        columns = [col.lower() for col in df.columns.tolist()]
        print(columns)
        if 'amount of fdi equity inflows' in columns:
            return df.copy()    
    return None
    

def fdi_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        main_url = "https://dpiit.gov.in/publications/fdi-statistics"

        session = requests.Session()
        response = session.get(main_url, verify = False)
        tree = html.fromstring(response.content)

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
            except Exception as e:
                print(e, "link not correct")
                continue

        if curr_link:
            print(curr_link)
            r = requests.get(curr_link, stream = True, verify = False)
            raw_file_name = os.path.join(raw_path, curr_link.split('/')[-1])
            
            with open(raw_file_name,"wb") as pdf:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        pdf.write(chunk)
            
            gupload.upload(raw_file_name, curr_link.split('/')[-1], gdrive_fdi_raw_folder)

            tables = read_pdf(raw_file_name,pages='all')
            extracted_df = get_df(tables)

            extracted_df = extracted_df.drop(['Unnamed: 0'], axis=1)
            extracted_df = extracted_df.iloc[:-3]
            extracted_df = extracted_df.drop([0])
            
            extracted_df.columns = (['date', 'fdi_equity_india'])
            extracted_df['date'] = extracted_df['date'].str.split(".", expand=True)[1].str.strip()
            extracted_df['date'] = pd.to_datetime(extracted_df['date'], format="%B, %Y")
            extracted_df['fdi_equity_india'] = extracted_df['fdi_equity_india'].str.replace(',', '').astype(int)

            min_mnth = extracted_df['date'].min().strftime("%m%Y")
            max_mnth = extracted_df['date'].max().strftime("%m%Y")
            extracted_df['date'] = extracted_df['date'].dt.strftime("%d-%m-%Y")

            filename = os.path.join(data_path, f"fdi_monthly_{min_mnth}-{max_mnth}.csv")
            extracted_df.to_csv(filename, index=False)

            gupload.upload(filename, f"fdi_monthly_{min_mnth}-{max_mnth}.csv", gdrive_fdi_monthly_folder)
        session.close()

    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=6, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

fdi_monthly_dag = DAG("fdiMonthlyScraping", default_args=default_args, schedule_interval='0 20 15 * *')

fdi_monthly_task = PythonOperator(task_id='fdi_monthly',
                                       python_callable = fdi_monthly,
                                       dag = fdi_monthly_dag,
                                       provide_context = True,
                                    )