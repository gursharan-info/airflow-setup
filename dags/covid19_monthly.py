import requests, json, csv, os, pendulum
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_covid_monthly_01Aug21.csv'
dir_path = '/usr/local/airflow/data/hfi/covid19'
monthly_data_path = os.path.join(dir_path, 'monthly')
gdrive_covid_monthly_folder = '1TKRs11piOD9iSGNb8xh-7_rEhrds-L0z'

def scrape_covid_monthly(**context):
    print(context['execution_date'], type(context['execution_date']))
    # The current date would be previous day from date of execution
    # now = datetime.fromtimestamp(context['execution_date'].timestamp())
    curr_date = datetime.fromtimestamp(context['execution_date'].timestamp())
    # last_day = date(curr_date.year, curr_date.month, 1) - relativedelta(days=1)
    print(curr_date)

    state_codes_df = pd.read_csv(lgd_codes_file)
    state_codes_df['district_name_covid'] = state_codes_df['district_name_covid'].fillna("")
    state_codes_df['district_name'] = state_codes_df['district_name'].fillna("")
    state_codes_df[['district_name','state_name']] = state_codes_df[['district_name','state_name']].apply(lambda x: x.str.strip())
    state_codes_df['state_name_lower'] = state_codes_df['state_name'].str.lower()
    state_codes_df['district_name_covid_lower'] = state_codes_df['district_name_covid'].str.lower()
    state_codes_df['state_district_lower'] = state_codes_df['state_name_lower'] + "_" + state_codes_df['district_name_covid_lower']
    
    dist_df = pd.read_csv("https://api.covid19india.org/csv/latest/districts.csv")[['Date','State','District',
                                                                                'Confirmed','Recovered','Deceased']]
    dist_df['Date'] = pd.to_datetime(dist_df['Date'], format='%Y-%m-%d')
    dist_df['District'] = dist_df['District'].str.replace('Unknown','', case=False)

    dist_df['month'] = dist_df['Date'].dt.to_period('M')
    dist_grouped =  dist_df.groupby(['month','State','District'])['Confirmed', 'Recovered', 'Deceased'].max().reset_index()
    dist_grouped.columns = ['month','state_name','district_name','total_confirmed_district',
                            'total_recovered_district','total_deceased_district']
    
    final_dist_df_list = []
    dist_groups =  dist_grouped.groupby(['state_name','district_name'])
    for name, dist_group in dist_groups:
        df = dist_group.reset_index(drop=True)
        df['confirmed_district'] = df['total_confirmed_district'].diff().fillna(df['total_confirmed_district']).astype(int)
        df['recovered_district'] = df['total_recovered_district'].diff().fillna(df['total_recovered_district']).astype(int)
        df['deceased_district'] = df['total_deceased_district'].diff().fillna(df['total_deceased_district']).astype(int)
        df[['confirmed_district','recovered_district',
            'deceased_district']] = df[['confirmed_district','recovered_district','deceased_district']].clip(lower=0)
        final_dist_df_list.append(df)
    #     display(df)
    final_dist_df = pd.concat(final_dist_df_list).reset_index(drop=True)
    final_dist_df = final_dist_df.sort_values(by=['month','state_name','district_name'])
    final_dist_df = final_dist_df[['month','state_name','district_name','confirmed_district','recovered_district',
                                'deceased_district','total_confirmed_district','total_recovered_district',
                                'total_deceased_district']]
                                
    final_dist_df['state_district_lower'] = final_dist_df['state_name'].str.strip().str.lower() + "_" \
                                    + final_dist_df['district_name'].str.strip().str.lower()
    final_dist_df.drop(columns=['district_name'], inplace=True)
    final_dist_df = pd.merge(final_dist_df, state_codes_df[['district_code','district_name','state_district_lower']].drop_duplicates(),
                            how='left', on='state_district_lower')
    district_code_column = final_dist_df.pop('district_code')
    final_dist_df.insert(2, 'district_code', district_code_column)
    district_name_column = final_dist_df.pop('district_name')
    final_dist_df.insert(2, 'district_name', district_name_column)
    final_dist_df.drop(columns=['state_district_lower'], inplace=True)

    state_group_df =  final_dist_df[['month','state_name','confirmed_district','recovered_district','deceased_district']
                         ].groupby(['month','state_name'], sort=False).sum().reset_index()
    state_group_cumm_df =  final_dist_df[['month','state_name','confirmed_district','recovered_district','deceased_district']
                            ].groupby(['state_name','month']).sum().groupby(level=0).cumsum().reset_index()

    state_group_df.rename(columns={'confirmed_district':'confirmed_state','recovered_district':'recovered_state',
                                'deceased_district':'deceased_state'}, inplace=True)
    state_group_cumm_df.rename(columns={'confirmed_district':'total_confirmed_state','recovered_district':
                                        'total_recovered_state','deceased_district':'total_deceased_state'}, inplace=True)
    # state_group_df['total_confirmed_state'] = state_group_df['confirmed_state'].cumsum()
    state_group_df = state_group_df.merge(state_group_cumm_df, on=['month','state_name'],how='left')
    state_group_df = state_group_df.sort_values(by=['state_name','month'])
    
    state_group_df['state_name_lower'] = state_group_df['state_name'].str.strip().str.lower()
    state_group_df = pd.merge(state_group_df, state_codes_df[['state_code','state_name_lower']].drop_duplicates(),
                                how='left', on='state_name_lower')
    state_group_df.drop(columns=['state_name_lower'], inplace=True)
    india_group_df =  state_group_df.groupby(['month'], sort=False)[
                'confirmed_state', 'recovered_state', 'deceased_state'].sum().reset_index()
    india_group_df.rename(columns={'confirmed_state':'confirmed_india','recovered_state':'recovered_india',
                                'deceased_state':'deceased_india'}, inplace=True)
    india_group_df['total_confirmed_india'] = india_group_df['confirmed_india'].cumsum()
    india_group_df['total_recovered_india'] = india_group_df['recovered_india'].cumsum()
    india_group_df['total_deceased_india'] = india_group_df['deceased_india'].cumsum()

    combined_df = pd.merge(
                pd.merge(final_dist_df,state_group_df, on=['month','state_name'],how='left'),
                india_group_df, on='month', how='left'
                ).reset_index(drop=True)
        
    combined_df = combined_df[combined_df['month'] == curr_date.strftime('%Y-%m')].reset_index(drop=True)
    combined_df.insert(0, 'date', "01-"+combined_df['month'].dt.strftime("%m-%Y"))
    combined_df.drop(columns=['month'], inplace=True)
    state_code_column = combined_df.pop('state_code')
    combined_df.insert(2, 'state_code', state_code_column)

    filename = os.path.join(monthly_data_path, 'covid_monthly_'+curr_date.strftime('%m%Y')+'.csv')
    combined_df.to_csv(filename,index=False)
    gupload.upload(filename, 'covid_monthly_'+curr_date.strftime('%m%Y')+'.csv',gdrive_covid_monthly_folder)
    print('Done')


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    # 'start_date': datetime(2021, 2, 1, 6, 0),
    'start_date': pendulum.datetime(year=2021, month=8, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(hours=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

scrape_covid_monthly_dag = DAG("covid19CasesMonthlyScraping", default_args=default_args, schedule_interval='0 20 2 * *')

scrape_covid_monthly_task = PythonOperator(task_id='scrape_covid_monthly',
                                       python_callable = scrape_covid_monthly,
                                       dag = scrape_covid_monthly_dag,
                                       provide_context = True,
                                    )

