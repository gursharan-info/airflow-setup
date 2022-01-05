from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from lxml import html

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
    'covid_daily',
    default_args=default_args,
    description='Covid 19 Daily',
    schedule_interval = '@daily',
    start_date = datetime(year=2021, month=11, day=1, hour=12, minute=0),
    catchup = True,
    tags=['health'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'covid')
    daily_data_path = os.path.join(dir_path, 'daily')
    os.makedirs(daily_data_path, exist_ok = True)
    # raw_data_path = os.path.join(dir_path, 'raw_data')
    # os.makedirs(raw_data_path, exist_ok = True)
    lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_covid_20Oct19.csv'

    SECTOR_NAME = 'Health'
    DATASET_NAME = 'covid_daily'
    day_lag = 0


    def scrape_covid_daily(ds, **context):  
        '''
        Scrapes the daily raw data of digital payments
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:

            district_wise_daily = pd.read_csv('https://data.incovid19.org/csv/latest/districts.csv')
            district_wise_daily['Date'] = pd.to_datetime(district_wise_daily['Date'], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
            district_wise_daily['District'] = district_wise_daily['District'].str.replace('Unknown','', case=False)
            
            yday=str(datetime.strftime(curr_date - timedelta(1), '%d-%m-%Y'))
            db_yday=str(datetime.strftime(curr_date - timedelta(2), '%d-%m-%Y'))
            yday_filtered = district_wise_daily[district_wise_daily['Date'].isin([yday])].iloc[:,0:6].reset_index(drop=True)
            db_yday_filtered = district_wise_daily[district_wise_daily['Date'].isin([db_yday])].iloc[:,1:6].reset_index(drop=True)

            delta_df = pd.DataFrame(yday_filtered[['Date','State','District']])
            df1 = yday_filtered[['Confirmed','Recovered','Deceased']]
            df2 = db_yday_filtered[['Confirmed','Recovered','Deceased']]
            delta_df[['Confirmed','Recovered','Deceased']] = ( df1 - df2).combine_first(df1).reindex_like(df1).fillna(0).astype(int).clip(lower=0)
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

            india_df =pd.read_csv('https://data.incovid19.org/csv/latest/case_time_series.csv')[['Date_YMD','Daily Confirmed','Daily Recovered','Daily Deceased']]
            india_df.rename(columns={'Date_YMD': 'Date', 'Daily Confirmed': 'confirmed_india', 'Daily Recovered': 'recovered_india', 'Daily Deceased': 'deceased_india'}, inplace=True)
            india_df['Date'] = pd.to_datetime(india_df['Date'], format='%Y-%m-%d')
            india_df = india_df[ india_df['Date'] <= (curr_date - timedelta(1)) ]
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
            state_total_df.rename(columns={'State': 'state_name', 'Confirmed': 'total_confirmed_state', 'Recovered': 'total_recovered_state',
             'Deceased': 'total_deceased_state'}, inplace=True)
            
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
            final_merged_data = final_merged_data.rename(columns={'Date':'date'})
            final_merged_data['district_name'] = final_merged_data['district_name'].str.replace('Unknown','', case=False)
            
            filename = os.path.join(daily_data_path, 'covid_'+yday+'.csv')
            final_merged_data.to_csv(filename,index=False)                                 

            return f"Downloaded data file till: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            raise ValueError(e)

    scrape_covid_daily_task = PythonOperator(
        task_id = 'scrape_covid_daily',
        python_callable = scrape_covid_daily,
        depends_on_past = True
    )



    # Upload data file 
    def upload_covid_daily(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)  
        print("Uploading data file for: ",curr_date.strftime('%d-%m-%Y'))

        try:

            filename = os.path.join(daily_data_path, f"covid_{curr_date.strftime('%d-%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"covid_{curr_date.strftime('%d-%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
        
        except Exception as e:
            raise ValueError(e)

        return f"Uploaded final data for: {curr_date.strftime('%d-%m-%Y')}"
        

    upload_covid_daily_task = PythonOperator(
        task_id = 'upload_covid_daily',
        python_callable = upload_covid_daily,
        depends_on_past = True
    )

    scrape_covid_daily_task >> upload_covid_daily_task

