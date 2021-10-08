from posix import PRIO_PGRP
import pendulum, os, glob
import PyPDF2 as pypdf
from tabula import read_pdf
import pandas as pd
from datetime import datetime, timedelta
from urllib.request import urlopen

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'hfi'), 'electricity')
daily_data_path = os.path.join(dir_path, 'daily')
monthly_data_path = os.path.join(dir_path, 'monthly')
gdrive_electricity_monthly_folder = '1J9Xb7cQ5JrBhogAX7E7cvYVVl48t0eua'


def process_electricity_monthly(**context):
    # today = datetime.fromtimestamp(context['execution_date'].timestamp())
    curr_date = context['execution_date']
    # scrape_dateString = curr_date.strftime("%d-%m-%y")

    files= [i for i in glob.glob(daily_data_path+'/*.{}'.format('csv'))]

    merged= pd.concat([pd.read_csv(f) for f in files]) 
    merged['date'] = pd.to_datetime(merged['date'], format="%d-%m-%Y")
    # print(merged.columns)
    merged.columns = ['state_name','max_demand_met_state','energy_met_mu_state','date','max_demand_met_india','energy_met_india','state_code']
    merged = merged[['date','state_name','state_code','max_demand_met_state','energy_met_mu_state','max_demand_met_india','energy_met_india']].sort_values(by=['date','state'])
    merged['date'] = pd.to_datetime(merged['date'], format="%d-%m-%Y")
    merged['month'] = merged['date'].dt.to_period('M')
    
    grouped =  merged.groupby(['month','state_name','state_code'], sort=False)[
                'max_demand_met_state', 'energy_met_state', 'max_demand_met_india','energy_met_india',
                                  ].sum().reset_index()
    grouped = grouped[grouped['month'].dt.strftime('%Y-%m') == curr_date.strftime('%Y-%m')].reset_index(drop=True)
    grouped.insert(0, 'date', "01-"+grouped['month'].dt.strftime("%m-%Y"))
    grouped = grouped.drop(columns=['month'])
    print(grouped.head(5))
    # filename = os.path.join(monthly_data_path, f"electricity_monthly_{curr_date.strftime('%m%Y')}.csv")
    # grouped.to_csv(filename, index=False)

    # gupload.upload(filename, f"electricity_monthly_{curr_date.strftime('%m%Y')}.csv", gdrive_electricity_monthly_folder)


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=8, day=1, hour=18, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

process_electricity_monthly_dag = DAG("electricitMonthlyProcessing", default_args=default_args, schedule_interval="0 20 3 * *")

process_electricity_monthly_task = PythonOperator(task_id = 'process_electricity_monthly',
                                       python_callable = process_electricity_monthly,
                                       dag = process_electricity_monthly_dag,
                                       provide_context = True,
                                    )