from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os, re, requests, time
from lxml import html, etree
from requests.utils import requote_uri

from bipp.sharepoint.uploads import upload_file



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=1),
}
with DAG(
    'petroleum_consumption_monthly',
    default_args=default_args,
    description='Petroleum Consumption Monthly',
    schedule_interval = '0 20 10 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['consumption'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'petroleum_consumption')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    

    SECTOR_NAME = 'Consumption'
    DATASET_NAME = 'petroleum_consumption_monthly'


    def scrape_petroleum_consumption_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)
        
        main_url = "https://www.ppac.gov.in/content/147_1_ConsumptionPetroleum.aspx"

        try:
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
                raw_file_loc = os.path.join(raw_data_path, f"petroleum_cnsm_raw_{curr_date.strftime('%m-%Y')}.xls")
                resp = requests.get(curr_link)
                with open(raw_file_loc, 'wb') as output:
                    output.write(resp.content)
                output.close()
                
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
                    filename = os.path.join(monthly_data_path, f"petroleum_consumption_{curr_date.strftime('%m-%Y')}.csv")
                    filtered_df.to_csv(filename, index=False)
                else:
                    raise ValueError("No link:  No Data available for this month yet")
            else:
                raise ValueError("No link:  No Data available for this month yet")
            

            return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

            # else:
                # raise ValueError(f"No data available yet for: : {curr_date.strftime('%m-%Y')}")

        except Exception as e:
            raise ValueError(e)


    scrape_petroleum_consumption_monthly_task = PythonOperator(
        task_id = 'scrape_petroleum_consumption_monthly',
        python_callable = scrape_petroleum_consumption_monthly,
        depends_on_past = True
    )

    

    # Upload data file 
    def upload_petroleum_consumption_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            raw_file_loc = os.path.join(raw_data_path, f"petroleum_cnsm_raw_{curr_date.strftime('%m-%Y')}.xls")
            upload_file(raw_file_loc, f"{DATASET_NAME}/raw_data", f"petroleum_cnsm_raw_{curr_date.strftime('%m-%Y')}.xls", SECTOR_NAME, "india_pulse")

            filename = os.path.join(monthly_data_path, f"petroleum_consumption_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"petroleum_consumption_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except Exception as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_petroleum_consumption_monthly_task = PythonOperator(
        task_id = 'upload_petroleum_consumption_monthly',
        python_callable = upload_petroleum_consumption_monthly,
        depends_on_past = True
    )

    scrape_petroleum_consumption_monthly_task >> upload_petroleum_consumption_monthly_task

