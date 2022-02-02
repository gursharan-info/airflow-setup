from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import os
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
    'covid_monthly',
    default_args=default_args,
    description='Covid 19 Monthly',
    schedule_interval = '0 20 2 * *',
    start_date = datetime(year=2021, month=11, day=1, hour=12, minute=0),
    catchup = True,
    tags=['health'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'covid')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_covid_20Oct19.csv'

    SECTOR_NAME = 'Health'
    DATASET_NAME = 'covid_monthly'

    def scrape_covid_monthly(ds, **context):  
        '''
        Scrapes the monthly raw data of Covid 19 Cases
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date.strftime('%m-%Y'))

        try:
            
            state_codes_df = pd.read_csv(lgd_codes_file)
            state_codes_df['state_name'] = state_codes_df['state_name'].str.strip().str.title().str.replace(r'\bAnd\b', 'and')
            state_codes_df['state_lower'] = state_codes_df['state_name'].str.strip().str.lower()
            state_codes_df['district_lower'] = state_codes_df['district_name'].str.strip().str.lower()
            state_codes_df['state_district_lower'] = state_codes_df['state_lower'] + "_" + state_codes_df['district_lower']

            district_df = pd.read_csv('https://data.incovid19.org/csv/latest/districts.csv')[['Date','State','District','Confirmed','Recovered','Deceased']].copy().sort_values(by='Date').reset_index(drop=True)
            district_df['Date'] = pd.to_datetime(district_df['Date'], format='%Y-%m-%d').dt.to_period('M')
            district_df['District'] = district_df['District'].str.replace('Unknown','', case=False)
            district_df.columns = [col.lower() for col in district_df.columns.tolist()]
            district_df.columns = ['date','state_name','district_name'] + [f"total_{col}_district" for col in district_df.columns.tolist()[3:] ]
            district_monthly_df = district_df.groupby(['date','state_name','district_name'])[['total_confirmed_district','total_recovered_district','total_deceased_district']].max().reset_index()

            district_monthly_df['confirmed_district'] = district_monthly_df.groupby(['state_name','district_name'])[['total_confirmed_district']].diff(1).fillna(0)
            district_monthly_df['recovered_district'] = district_monthly_df.groupby(['state_name','district_name'])[['total_recovered_district']].diff(1).fillna(0)
            district_monthly_df['deceased_district'] = district_monthly_df.groupby(['state_name','district_name'])[['total_deceased_district']].diff(1).fillna(0)
            district_monthly_df['state_district_lower'] = district_monthly_df['state_name'].str.strip().str.lower() + "_" + district_monthly_df['district_name'].str.strip().str.lower()

            district_merged_df = pd.merge(district_monthly_df, state_codes_df[['state_district_lower','state_code','district_code']].drop_duplicates(), on='state_district_lower', how='left')
            district_merged_df = district_merged_df.drop(columns='state_district_lower').dropna(subset=['district_code']).reset_index(drop=True)
            district_merged_df['district_code'] = district_merged_df['district_code'].astype(int)
            district_merged_df['state_code'] = district_merged_df['state_code'].astype(int)
            

            state_df = pd.read_csv('https://data.incovid19.org/csv/latest/states.csv')[['Date','State','Confirmed','Recovered','Deceased']].copy().sort_values(by='Date').reset_index(drop=True)
            state_df = state_df[ ~(state_df['State'].str.contains('india', case=False, na=False) )]
            state_df['Date'] = pd.to_datetime(state_df['Date'], format='%Y-%m-%d').dt.to_period('M')
            state_df.columns = ['date', 'state_name', 'total_confirmed_state', 'total_recovered_state', 'total_deceased_state']
            state_df = state_df.groupby(['date','state_name'])[['total_confirmed_state','total_recovered_state','total_deceased_state']].max().reset_index()

            state_df['confirmed_state'] = state_df.groupby(['state_name'])[['total_confirmed_state']].diff(1).fillna(0)
            state_df['recovered_state'] = state_df.groupby(['state_name'])[['total_recovered_state']].diff(1).fillna(0)
            state_df['deceased_state'] = state_df.groupby(['state_name'])[['total_deceased_state']].diff(1).fillna(0)

            state_df['state_lower'] = state_df['state_name'].str.lower()
            state_df = pd.merge(state_df, state_codes_df[['state_lower','state_code']].drop_duplicates(), on='state_lower', how='left')
            state_df = state_df.drop(columns='state_lower').dropna(subset=['state_code']).reset_index(drop=True)
            state_df['state_code'] = state_df['state_code'].astype(int)


            india_df =pd.read_csv('https://data.incovid19.org/csv/latest/case_time_series.csv')[['Date_YMD','Daily Confirmed','Daily Recovered','Daily Deceased']]
            india_df.columns= ['date','confirmed_india', 'recovered_india', 'deceased_india']
            india_df['date'] = pd.to_datetime(india_df['date'], format='%Y-%m-%d').dt.to_period('M')
            india_df = india_df.groupby(['date'])[['confirmed_india','recovered_india','deceased_india']].sum().reset_index()
            india_df['total_confirmed_india'] = india_df['confirmed_india'].cumsum()
            india_df['total_recovered_india'] = india_df['recovered_india'].cumsum()
            india_df['total_deceased_india'] = india_df['deceased_india'].cumsum()

            merged_df = district_merged_df.merge(
                    state_df.drop(columns='state_name'), on=['date','state_code'], how='left'
            ).merge(india_df, on='date', how='left')
            merged_df = merged_df[['date','state_name','state_code','district_name','district_code','confirmed_district','recovered_district','deceased_district',
                                'total_confirmed_district','total_recovered_district','total_deceased_district','confirmed_state','recovered_state','deceased_state',
                                'total_confirmed_state','total_recovered_state','total_deceased_state','confirmed_india','recovered_india','deceased_india',
                                'total_confirmed_india','total_recovered_india','total_deceased_india'
                        ]]
            numeric_columns = merged_df.select_dtypes(include=['number']).columns
            merged_df[numeric_columns] = merged_df[numeric_columns].fillna("")
            merged_df = merged_df[ merged_df['date'] == curr_date.strftime("%Y-%m") ].copy()
            merged_df['date'] = merged_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(monthly_data_path, f"covid_{curr_date.strftime('%m-%Y')}.csv")
            merged_df.to_csv(filename,index=False)                                 

            return f"Downloaded data file till: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            raise ValueError(e)

    scrape_covid_monthly_task = PythonOperator(
        task_id = 'scrape_covid_monthly',
        python_callable = scrape_covid_monthly,
        depends_on_past = True
    )



    # Upload data file 
    def upload_covid_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp()) 
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(monthly_data_path, f"covid_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"covid_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
        
        except Exception as e:
            raise ValueError(e)

        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"
        

    upload_covid_monthly_task = PythonOperator(
        task_id = 'upload_covid_monthly',
        python_callable = upload_covid_monthly,
        depends_on_past = True
    )

    scrape_covid_monthly_task >> upload_covid_monthly_task

