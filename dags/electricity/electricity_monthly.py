from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os, glob
import PyPDF2 as pypdf
from tabula import read_pdf
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
    'electricity_monthly',
    default_args=default_args,
    description='Electricity Supply Daily',
    schedule_interval = '0 20 3 * *',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=11, day=2, hour=12, minute=0),
    catchup = True,
    tags=['consumption'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'electricity')
    daily_data_path = os.path.join(dir_path, 'daily')
    os.makedirs(daily_data_path, exist_ok = True)

    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    main_url = "https://posoco.in/reports/daily-reports/daily-reports-2021-22/"
    states = ['Punjab', 'Haryana', 'Rajasthan', 'Delhi', 'UP', 'Uttarakhand', 'HP', 'J&K(UT) & Ladakh(UT)','J&K(UT)','Ladakh(UT)','J&K','Ladakh', 'Chandigarh', 'Chhattisgarh', 'Gujarat', 'MP', 'Maharashtra', 'Goa', 'DD', 'DNH',"Essar steel", 'AMNSIL', 'Andhra Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Puducherry','Pondy','Bihar','DVC', 'Jharkhand', 'Odisha', 'West Bengal', 'Sikkim', 'Arunachal Pradesh', 'Assam', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Tripura']
    elec_columns = ['state','max_demand_met_state','shortage','energy_met_state','drawal_schedule_mu','od_ud_mu','max_od_mw','energy_storage']

    SECTOR_NAME = 'Consumption'
    DATASET_NAME = 'electricity_monthly'
    day_lag = 1
    

    def process_electricity_monthly(ds, **context):  
        '''
        Processes the monthly data from existing daily data of electricity supply
        '''
        # print(context['execution_date'])
        # The current date would be derived from the execution date using the lag parameter. 
        # Lag is the delay which the source website has to upload data for that particular date
        # print(context)
        curr_date = context['data_interval_start']
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",context['data_interval_start'])   

        try:
            files= [i for i in glob.glob(daily_data_path+'/*.{}'.format('csv'))]
            print(daily_data_path)
            print(files)
            mnth_files = [file for file in files if curr_date.strftime("%m-%Y") in file]

            merged= pd.concat([pd.read_csv(f) for f in mnth_files]) 
            merged['date'] = pd.to_datetime(merged['date'], format="%d-%m-%Y").reset_index(drop=True)
            print(merged.columns)
            # merged.columns = ['state_name','max_demand_met_state','energy_met_state','date','max_demand_met_india','energy_met_india','state_code']
            # ['date','state_name', 'state_code','max_demand_met_state', 'energy_met_state',  'max_demand_met_india', 'energy_met_india']
            merged = merged[['date','state_name','state_code','max_demand_met_state','energy_met_state','max_demand_met_india','energy_met_india']].sort_values(by=['date','state_name'])
            merged['date'] = pd.to_datetime(merged['date'], format="%d-%m-%Y")
            merged['month'] = merged['date'].dt.to_period('M')
            
            grouped =  merged.groupby(['month','state_name','state_code'], sort=False)[
                        'max_demand_met_state', 'energy_met_state', 'max_demand_met_india','energy_met_india',
                                        ].sum().reset_index()
            grouped = grouped[grouped['month'].dt.strftime('%Y-%m') == curr_date.strftime('%Y-%m')].reset_index(drop=True)
            grouped.insert(0, 'date', "01-"+grouped['month'].dt.strftime("%m-%Y"))
            grouped = grouped.drop(columns=['month'])
            # print(grouped.head(5))
            filename = os.path.join(monthly_data_path, f"electricity_{curr_date.strftime('%m-%Y')}.csv")
            grouped.to_csv(filename, index=False)

            return f"Scraped data for: {curr_date.strftime('%d-%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)

    process_electricity_monthly_task = PythonOperator(
        task_id='process_electricity_monthly',
        python_callable=process_electricity_monthly,
        depends_on_past=True
    )



    # Upload data file 
    def upload_electricity_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = context['data_interval_start']
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(monthly_data_path, f"electricity_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"electricity_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    upload_electricity_monthly_task = PythonOperator(
        task_id = 'upload_electricity_monthly',
        python_callable = upload_electricity_monthly,
        depends_on_past = True
    )
    
    process_electricity_monthly_task >> upload_electricity_monthly_task

