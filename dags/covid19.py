import requests, json, csv, os, pendulum
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_covid_20Oct19.csv'
dir_path = '/usr/local/airflow/data/hfi'
gdrive_covid_folder = '1Ey0Lv4sftSlPXC_Obc7LyGiWfg89etXj'

def read_covid_data(**context):
    print(context['execution_date'], type(context['execution_date']))
    # The current date would be previous day from date of execution
    now = datetime.fromtimestamp(context['execution_date'].timestamp())
    print(now)
    district_wise_daily = pd.read_csv('https://api.covid19india.org/csv/latest/districts.csv')
    district_wise_daily['Date'] = pd.to_datetime(district_wise_daily['Date'], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
    district_wise_daily['District'] = district_wise_daily['District'].str.replace('Unknown','', case=False)
    
    yday=str(datetime.strftime(now - timedelta(1), '%d-%m-%Y'))
    db_yday=str(datetime.strftime(now - timedelta(2), '%d-%m-%Y'))
    yday_filtered = district_wise_daily[district_wise_daily['Date'].isin([yday])].iloc[:,0:6].reset_index(drop=True)
    db_yday_filtered = district_wise_daily[district_wise_daily['Date'].isin([db_yday])].iloc[:,1:6].reset_index(drop=True)

    delta_df = pd.DataFrame(yday_filtered[['Date','State','District']])
    df1 = yday_filtered[['Confirmed','Recovered','Deceased']]
    df2 = db_yday_filtered[['Confirmed','Recovered','Deceased']]
    delta_df[['Confirmed','Recovered','Deceased']] = ( df1 - df2).combine_first(df1).reindex_like(df1).astype(int).clip(lower=0)
    delta_df.reset_index(drop=True, inplace=True)
    delta_df['state_name_lower'] = delta_df['State'].str.lower().str.strip()
    delta_df['district_lower'] = delta_df['District'].str.lower().str.strip()
    delta_df['state_district_lower'] = delta_df['state_name_lower'] + "_" + delta_df['district_lower']

    state_group_df = delta_df.groupby(['Date','State'], as_index=False).sum()
    state_group_df.rename(columns={'State': 'state_name', 'Confirmed': 'confirmed_state', 'Recovered': 'recovered_state', 'Deceased': 'deceased_state'}, inplace=True)
    state_group_df['state_name_lower'] = state_group_df['state_name'].str.lower()

    state_codes_df = pd.read_csv(lgd_codes_file)
    state_codes_df[['district_name','state_name']] = state_codes_df[['district_name','state_name']].apply(lambda x: x.str.strip())
    state_codes_df['state_name_lower'] = state_codes_df['state_name'].str.lower()
    state_codes_df['district_lower'] = state_codes_df['district_name'].str.lower()
    state_codes_df['state_district_lower'] = state_codes_df['state_name_lower'] + "_" + state_codes_df['district_lower']

    india_df =pd.read_csv('https://api.covid19india.org/csv/latest/case_time_series.csv')[['Date_YMD','Daily Confirmed','Daily Recovered','Daily Deceased']]
    india_df.rename(columns={'Date_YMD': 'Date', 'Daily Confirmed': 'confirmed_india', 'Daily Recovered': 'recovered_india', 'Daily Deceased': 'deceased_india'}, inplace=True)
    india_df['Date'] = pd.to_datetime(india_df['Date'], format='%Y-%m-%d')
    india_df = india_df[ india_df['Date'] <= (now - timedelta(1)) ]
    india_df['Date'] = india_df['Date'].dt.strftime('%d-%m-%Y')

    district_group_df = pd.merge(delta_df, 
                             state_codes_df[['state_code','state_name_lower']].drop_duplicates(),
                             how='left', on='state_name_lower')
    district_group_df = pd.merge(district_group_df, state_codes_df[['district_code','district_lower']],
                             how='left', on='district_lower')
    district_group_df = district_group_df.drop_duplicates(subset='state_district_lower', keep="first")

    district_group_df.rename(columns={'Confirmed': 'confirmed_district', 'Recovered': 'recovered_district', 'Deceased': 'deceased_district'}, inplace=True)

    district_group_df = district_group_df[~district_group_df['District'].isin([
        'Foreign Evacuees','Other State','Capital Complex','Others','Upper Dibang Valley',
        'Gaurela Pendra Marwahi', 'State Pool','Hnahthial','Khawzawl','Saitual','BSF Camp',
        'Chengalpattu','Kallakurichi','Evacuees','Ranipet','Tenkasi',
        'Italians','Tirupathur','Airport Quarantine','Railway Quarantine'
    ])]

    district_data_df = district_group_df.merge(yday_filtered.drop_duplicates(subset=['State','District']), how='left',on=['Date','State','District'])
    district_data_df.rename(columns={'Confirmed': 'total_confirmed_district', 'Recovered': 'total_recovered_district', 
                                    'Deceased': 'total_deceased_district', 'State':'state_name', 'District':'district_name'},
                            inplace=True)
    district_data_df = district_data_df[['Date','state_name','state_code','district_name','district_code',
         'confirmed_district','recovered_district','deceased_district',
         'total_confirmed_district','total_recovered_district','total_deceased_district','state_name_lower']]


    final_merged_data = pd.merge(district_data_df[['Date','district_name','district_code','confirmed_district','recovered_district',
                                               'deceased_district','total_confirmed_district','total_recovered_district',
                                               'total_deceased_district','state_code','state_name_lower']], 
                             state_group_df[['state_name','confirmed_state','recovered_state','deceased_state',
                                             'state_name_lower']],
                             how='left',on=['state_name_lower'])

    state_total_df = yday_filtered.groupby(['State'],as_index=False).sum()
    state_total_df.rename(columns={'State': 'state_name', 'Confirmed': 'total_confirmed_state', 'Recovered': 'total_recovered_state', 'Deceased': 'total_deceased_state'}, inplace=True)
    
    final_merged_data = pd.merge(final_merged_data, state_total_df, how='left',on=['state_name'])
    final_merged_data = pd.merge(final_merged_data, india_df.tail(1), how='left',on=['Date'])
    
    india_total_df = yday_filtered.groupby(['Date'],as_index=False).sum()
    india_total_df.rename(columns={'Confirmed': 'total_confirmed_india', 'Recovered': 'total_recovered_india', 'Deceased': 'total_deceased_india'}, inplace=True)

    final_merged_data = pd.merge(final_merged_data, india_total_df, how='left',on=['Date'])
    final_merged_data = final_merged_data[['Date','state_name','state_code','district_name','district_code',
         'confirmed_district','recovered_district','deceased_district',
         'total_confirmed_district','total_recovered_district','total_deceased_district',
         'confirmed_state','recovered_state','deceased_state',
         'total_confirmed_state','total_recovered_state','total_deceased_state',
         'confirmed_india','recovered_india','deceased_india',
         'total_confirmed_india','total_recovered_india','total_deceased_india'
         ]]
    final_merged_data = final_merged_data.fillna("") 
    final_merged_data['district_name'] = final_merged_data['district_name'].str.replace('Unknown','', case=False)
    
    filename = os.path.join(dir_path, 'covid19/covid_'+yday+'.csv')
    final_merged_data.to_csv(filename,index=False)
    gupload.upload(filename, 'covid_'+yday+'.csv',gdrive_covid_folder)

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    # 'start_date': datetime(2021, 2, 1, 6, 0),
    'start_date': pendulum.datetime(year=2021, month=2, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    # "owner": "airflow",
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    # "depends_on_past": False,
    # "start_date": datetime(2020, 12, 18),
    # "email": ["airflow@airflow.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("covid19", default_args=default_args, schedule_interval="@daily")

read_covid_data_task = PythonOperator(task_id='read_covid_data',
                                       python_callable=read_covid_data,
                                       dag=dag,
                                       provide_context = True,
                                    )

