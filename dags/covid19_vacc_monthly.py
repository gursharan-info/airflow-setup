import os, pendulum, re
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_covid_vacc_28Jul21.csv'
dir_path = '/usr/local/airflow/data/hfi/covid19_vacc'
monthly_data_path = os.path.join(dir_path, 'monthly')
gdrive_covid_vacc_monthly_folder = '1bbCD743koSOlZ4adfOmjicxuuKhzm5IG'
day_lag = 1

def scrape_covid_vacc_monthly(**context):
    print(context['execution_date'], type(context['execution_date']))
    # The current date would be previous day from date of execution
    curr_date = date.fromtimestamp(context['execution_date'].timestamp())
    print(curr_date)
    cowin_vacc_df = pd.read_csv("http://api.covid19india.org/csv/latest/cowin_vaccine_data_districtwise.csv", header=[0,1], low_memory=False)
    
    cowin_vacc_df = cowin_vacc_df.dropna(axis=1, how='all')
    cowin_vacc_df = cowin_vacc_df.drop(cowin_vacc_df.iloc[:, 0:2], axis = 1)
    cowin_vacc_df = cowin_vacc_df.drop(cowin_vacc_df.iloc[:, 1:3], axis = 1)
    cowin_vacc_df.rename(columns={'Unnamed: 2_level_1': '', 'Unnamed: 5_level_1': ''}, inplace=True)
    cowin_vacc_df = cowin_vacc_df.set_index(['State','District'])

    wide_df = cowin_vacc_df.stack().reset_index()
    long_df = wide_df.set_index(['State','District','level_2']).stack().reset_index()
    long_df.columns = ['state_name','district_name','dose_type','date','value']

    pivoted = long_df.pivot_table(index=['state_name', 'district_name','date'], columns=['dose_type'], values=['value'],
                              aggfunc='sum')
    pivoted.columns = [col[-1] for col in pivoted.columns]
    pivoted = pivoted.reset_index()

    pivoted['date'] = pd.to_datetime(pivoted['date'], format="%d/%m/%Y")
    pivoted = pivoted[['date','state_name','district_name'] + pivoted.columns[3:].tolist()]
    pivoted.columns = ['date','state_name','district_name'] + [re.sub(' +', '_', re.sub(r'[^a-zA-Z ]', ' ', col.lower()).strip()) for col in pivoted.columns[3:].tolist()]
    pivoted = pivoted.sort_values(by=['date','state_name','district_name']).reset_index(drop=True)

    filtered_df = pivoted[['date','state_name','district_name', 'first_dose_administered','second_dose_administered']].copy()
    filtered_df.rename(columns={'first_dose_administered': 'first_dose_admn','second_dose_administered': 'second_dose_admn'},
                  inplace=True)
    filtered_df['total_doses_admn'] = filtered_df.first_dose_admn + filtered_df.second_dose_admn

    # last_day_of_prev_month = pd.to_datetime(curr_date.replace(day=1) - timedelta(days=1))
    # start_day_of_prev_month = pd.to_datetime(curr_date.replace(day=1) - timedelta(days=last_day_of_prev_month.day))
    # prev_month = start_day_of_prev_month.strftime("%Y-%m")
    prev_month = curr_date.strftime("%Y-%m")

    ### Make sure granularity is unque at district level
    dist_grouped = filtered_df.groupby(['state_name','district_name'])
    dist_list = []               

    for name, group in dist_grouped:
        group['first_dose_admn_daily'] = group['first_dose_admn'].diff().fillna(group['first_dose_admn'])
        group['second_dose_admn_daily'] = group['second_dose_admn'].diff().fillna(group['second_dose_admn'])
        group['total_doses_admn_daily'] = group['total_doses_admn'].diff().fillna(group['total_doses_admn'])
        group = group[['date','state_name','district_name','first_dose_admn_daily','second_dose_admn_daily','total_doses_admn_daily']]
        group.columns = ['date','state_name','district_name','first_dose_admn_district','second_dose_admn_district',
                    'total_doses_admn_district']
        group = group.reset_index(drop=True)
        dist_list.append(group)

    delta_df = pd.concat(dist_list).sort_values(by=['date','state_name','district_name']).reset_index(drop=True)
    month = delta_df['date'].dt.to_period("M")
    monthly_df = delta_df.groupby([month,'state_name','district_name']).sum().reset_index()
    monthly_df = monthly_df[monthly_df['date'] == prev_month]
    monthly_df['date'] = monthly_df['date'].dt.strftime("01-%m-%Y")

    state_group_df = monthly_df.groupby(['date','state_name'], as_index=False).sum()
    state_group_df.columns = ['date','state_name','first_dose_admn_state','second_dose_admn_state','total_doses_admn_state']

    india_group_df = monthly_df.groupby(['date'], as_index=False).sum()
    india_group_df.columns = ['date','first_dose_admn_india','second_dose_admn_india','total_doses_admn_india']

    final_df = monthly_df.merge(state_group_df, on=['date','state_name'], how='left')
    final_df = final_df.merge(india_group_df, on=['date'], how='left')

    final_df['state_district_lower'] = final_df['state_name'].str.lower().str.strip() + "_" + \
                    final_df['district_name'].str.lower().str.strip()

    state_codes_df = pd.read_csv(lgd_codes_file)
    state_codes_df['state_district_lower'] = state_codes_df['state_name_covid'].str.strip().str.lower() + "_" + \
                    state_codes_df['district_name_covid'].str.lower().str.strip()
    
    mapped_df = pd.merge(final_df, state_codes_df[['state_code','district_code','state_district_lower']].drop_duplicates(),
                                 how='left', on='state_district_lower')
    mapped_df = mapped_df[['date','state_name','state_code','district_name','district_code']+mapped_df.columns.tolist()[3:-3]]
  
    filename = os.path.join(monthly_data_path, 'covid_vacc_'+curr_date.strftime("%m-%Y")+'.csv')
    mapped_df.to_csv(filename,index=False)
    gupload.upload(filename, 'covid_vacc_'+curr_date.strftime("%m%Y")+'.csv',gdrive_covid_vacc_monthly_folder)


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=1, day=2, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

scrape_covid_vacc_monthly_dag = DAG("covid19VaccMonthlyScraping", default_args=default_args, schedule_interval='0 20 2 * *')

scrape_covid_vacc_monthly_task = PythonOperator(task_id = 'scrape_covid_vacc_monthly',
                                       python_callable = scrape_covid_vacc_monthly,
                                       dag = scrape_covid_vacc_monthly_dag,
                                       provide_context = True,
                                    )

