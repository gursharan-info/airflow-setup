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
from airflow.utils.state import State
from helpers import google_upload as gupload
from helpers import sharepoint_upload as sharepoint

dir_path = r'/usr/local/airflow/data/hfi/petroleum_cnsm'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_petroleum_monthly_folder = '18ctChEyc8fP9LHtYncFa6Rm_Xg4hw4no'
gdrive_petroleum_raw_folder = '1cchPXv3FMzN0D4iLXTaW0-UTBdatGigc'
SECTOR_NAME = 'Consumption'
DATASET_NAME = 'petrol_consumption_monthly'

def petr_cnsm_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        print("Scraping for: ",curr_date.strftime("%d-%m-%Y"))
        main_url = "https://www.ppac.gov.in/content/147_1_ConsumptionPetroleum.aspx"

        session = requests.Session()
        response = session.get(main_url)
        tree = html.fromstring(response.content)
        # tree

        curr_link = None

        for a in tree.xpath('//*[@id="contentid"]//a'):
            if 'current' in a.text.lower():
                print(a.get("href"))
                curr_link = f"https://www.ppac.gov.in{a.get('href')}"
                print(curr_link)
        session.close()

        if curr_link:
            # Read source raw file
            df = pd.read_excel(curr_link)
            
            # Save raw file on local and upload to Drive
            raw_file_loc = os.path.join(raw_path, f"petroleum_cnsm_raw_{curr_date.strftime('%m%Y')}.{curr_link.split('.')[-1]}")
            resp = requests.get(curr_link)
            with open(raw_file_loc, 'wb') as output:
                output.write(resp.content)
            output.close()
            gupload.upload(raw_file_loc, f"petroleum_cnsm_raw_{curr_date.strftime('%m%Y')}.csv", gdrive_petroleum_raw_folder)
            sharepoint.upload(raw_file_loc, f"petroleum_cnsm_raw_{curr_date.strftime('%m%Y')}.csv", SECTOR_NAME, f"{DATASET_NAME}/raw_data")

            # Data Processing
            start_idx = df.index[df[df.columns[0]].str.lower().str.contains('products', na=False,case=False)].tolist()[-1]
            last_idx = df.index[df[df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()[-1]

            df = df.iloc[start_idx:last_idx]
            df.columns = df.iloc[0]
            reshaped_df = df[1:].T.reset_index()     

            r_last_idx = reshaped_df.index[reshaped_df[reshaped_df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()[0]
            reshaped_df.columns = reshaped_df.iloc[0]
            reshaped_df = reshaped_df.iloc[1:r_last_idx] 

            filtered_df = reshaped_df[['PRODUCTS','HSD','LPG','MS']].copy()
            filtered_df.columns = ['date','hsd_consm_india','lpg_consm_india','ms_consm_india']
            filtered_df['date'] = filtered_df['date'] + "-2021"
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], format="%b-%Y").dt.strftime("%d-%m-%Y")
            filtered_df = filtered_df.dropna(subset=['hsd_consm_india','lpg_consm_india','ms_consm_india'])
            filtered_df = filtered_df[filtered_df['date'] == curr_date.strftime("01-%m-%Y")]        

            if not filtered_df.empty:
                filename = os.path.join(data_path, f"petroleum_cnsm_monthly_{curr_date.strftime('%m%Y')}.csv")
                filtered_df.to_csv(filename, index=False)
                gupload.upload(filename, f"petroleum_cnsm_monthly_{curr_date.strftime('%m%Y')}.csv", gdrive_petroleum_monthly_folder)
                sharepoint.upload(filename, f"petroleum_cnsm_monthly_{curr_date.strftime('%m%Y')}.csv", SECTOR_NAME, DATASET_NAME)
            else:
                context['task_instance']=State.UP_FOR_RETRY 
                raise ValueError('No Data:  No data avaiable on source for the month yet')
                return False
        else:
            print('No link:  No Data available for this month yet')
            context['task_instance']=State.UP_FOR_RETRY 
            # raise ValueError('No link:  No data avaiable on source for the month yet')
            return False
    # except ValueError:
        # print('No link: No Data available for this month yet')
        # return False
    except Exception as e:
        print(e)
        return False
        

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=6, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(days=2),
}

petr_cnsm_monthly_dag = DAG("petroleumMonthlyScraping", default_args=default_args, schedule_interval='0 20 13 * *')

petr_cnsm_monthly_task = PythonOperator(task_id='petr_cnsm_monthly',
                                       python_callable = petr_cnsm_monthly,
                                       dag = petr_cnsm_monthly_dag,
                                       provide_context = True,
                                    )