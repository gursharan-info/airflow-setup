from bs4 import BeautifulSoup
import requests, json, io, re, pendulum, urllib3, os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_v2_17Sep21_fertilizer.csv'
dir_path = '/usr/local/airflow/data/hfi/fertilizer_sales'
data_path = os.path.join(dir_path, 'monthly')
gdrive_fertilizer_folder = '1Qeb78cX68-Xs51cajXs2zeRIVj0QKZOA'

def fertilizer_monthly(**context):
    curr_date = context['execution_date']
    print(curr_date)

    try:
        hist_df = pd.read_csv(os.path.join(dir_path,'data_historical.csv'))
        hist_df['date'] = pd.to_datetime(hist_df['date'], format='%d-%m-%Y')
        hist_df = hist_df.sort_values(by=['date','state_name','district_name'])

        hist_df['month'] = hist_df['date'].dt.to_period('M')
        df_grouped_dist = hist_df.groupby(['month','state_name','state_code','district_name','district_code']
                                    )[['quantity_sold_daily_district',
                                'number_of_sale_transactions_daily_district']].sum().reset_index()
        df_grouped_dist.columns = ['month','state_name','state_code','district_name','district_code','quantity_sold_roll_district',
                            'number_of_sale_transactions_roll_district']
        
        df_grouped_state = df_grouped_dist.groupby(['month','state_name','state_code'])[['quantity_sold_roll_district',
                                    'number_of_sale_transactions_roll_district']].sum().reset_index()
        df_grouped_state.columns = ['month','state_name','state_code','quantity_sold_roll_state',
                                    'number_of_sale_transactions_roll_state']

        df_grouped_india = df_grouped_state.groupby(['month'])[['quantity_sold_roll_state',
                                'number_of_sale_transactions_roll_state']].sum().reset_index()
        df_grouped_india.columns = ['month','quantity_sold_roll_india','number_of_sale_transactions_roll_india']

        df_grouped_final = df_grouped_dist.merge(
                                df_grouped_state.merge(df_grouped_india,
                                    on=['month'], how='left' 
                                ), on=['month','state_name','state_code'], how='left')

        df_grouped_final = df_grouped_final[df_grouped_final['month'] == curr_date.strftime("%Y-%m")]
        df_grouped_final.insert(0, 'date', "01-"+df_grouped_final['month'].dt.strftime("%m-%Y"))
        df_grouped_final = df_grouped_final.drop(columns=['month']).reset_index(drop=True)

        filename = os.path.join(data_path, f"fertilizer_sales_monthly_{curr_date.strftime('%m%Y')}.csv")
        df_grouped_final.to_csv(filename,index=False)
        
        # filename = os.path.join(data_path, currentDate.strftime("%d-%m-%Y")+'.csv')
        # final_df.to_csv(filename,index=False)
        gupload.upload(filename, f"fertilizer_sales_monthly_{curr_date.strftime('%m%Y')}.csv",gdrive_fertilizer_folder)
    
    except requests.exceptions.RequestException as e:
        print(e)
        pass


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=8, day=10, hour=12, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(hours=3),
}

fertilizer_monthly_dag = DAG("fertilizerMonthlyScraping", default_args=default_args, schedule_interval="0 20 5 * *")

fertilizer_monthly_task = PythonOperator(task_id='fertilizer_monthly',
                                       python_callable=fertilizer_monthly,
                                       dag=fertilizer_monthly_dag,
                                       provide_context = True,
                                       depends_on_past=True,
                                       wait_for_downstream=True
                                    )