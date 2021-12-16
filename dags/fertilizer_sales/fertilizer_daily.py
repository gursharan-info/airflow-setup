from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests, pendulum, os, re, urllib3
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
    'fertilizer_daily',
    default_args=default_args,
    description='Fertilizer Sales Daily',
    schedule_interval=timedelta(days=1),
    start_date = days_ago(6),
    catchup = True,
    tags=['fertilizer_sales'],
) as dag:
    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'india_pulse'), 'fertilizer_sales')
    data_path = os.path.join(dir_path, 'daily')
    raw_data_path = os.path.join(dir_path, 'raw_data')
    SECTOR_NAME = 'Agriculture'
    DATASET_NAME = 'fertilizer_daily'
    day_lag = 2
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def scrape_fertilizer_daily(ds, **kwargs):
        
        print(kwargs['execution_date'])
        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='scrape_fertilizer_daily',
        python_callable=scrape_fertilizer_daily,
    )
    
