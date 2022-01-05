from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os
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
    'google_mobility_daily',
    default_args=default_args,
    description='Google Mobility Daily',
    schedule_interval = '@daily',
    start_date = datetime(year=2021, month=12, day=8, hour=12, minute=0),
    catchup = True,
    tags=['mobility'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'google_mobility')
    daily_data_path = os.path.join(dir_path, 'daily')
    os.makedirs(daily_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    source_file_url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'

    SECTOR_NAME = 'Mobility'
    DATASET_NAME = 'google_mobility_daily'
    day_lag = 3


    def scrape_google_mobility_daily(ds, **context):  
        '''
        Scrapes the daily raw data of digital payments
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            raw_df = pd.read_csv(source_file_url)
            india_df = raw_df[raw_df['country_region_code'] == 'IN'].copy()

            raw_filename = os.path.join(raw_data_path, f"Global_Mobility_Report_IN.csv")
            india_df.to_csv(raw_filename, index=False)
            
            upload_file(raw_filename, f"{DATASET_NAME}/raw_data", f"Global_Mobility_Report_IN.csv", SECTOR_NAME, "india_pulse")

            return f"Downloaded data file till this month: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            print(e)

    scrape_google_mobility_daily_task = PythonOperator(
        task_id = 'scrape_google_mobility_daily',
        python_callable = scrape_google_mobility_daily,
        depends_on_past = True
    )



    def process_google_mobility_daily(ds, **context):  
        '''
        Process the scraped daily raw data. 
        '''
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            raw_filename = os.path.join(raw_data_path, f"Global_Mobility_Report_IN.csv")
            raw_df = pd.read_csv(raw_filename)
            india_df = raw_df.drop(["country_region_code","country_region",'metro_area','iso_3166_2_code','census_fips_code',
                          'place_id'],axis=1).reset_index(drop=True)
            india_df = india_df.rename({'sub_region_2':'district_name_data', 'sub_region_1':'state_name_data'},axis=1)
            india_df['date'] = pd.to_datetime(india_df['date'], format="%Y-%m-%d")

            daily_df = india_df.groupby(['date','state_name_data','district_name_data'])['retail_and_recreation_percent_change_from_baseline',
                                'grocery_and_pharmacy_percent_change_from_baseline','parks_percent_change_from_baseline',
                                'transit_stations_percent_change_from_baseline','workplaces_percent_change_from_baseline',
                                'residential_percent_change_from_baseline'].mean().reset_index()
            daily_df.columns = ['date','state_name_data','district_name_data','retail_and_recreation',      'grocery_and_pharmacy','parks',
                    'transit_stations','workplaces','residential_places']

            #LGD codes
            man_corr_lgd_codes = pd.read_csv('https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_google_mobility_02-09-2021.csv')
            man_corr_lgd_codes['state_name'] = man_corr_lgd_codes['state_name'].str.title().str.replace(r'\bAnd\b','and')
            man_corr_lgd_codes['district_name'] = man_corr_lgd_codes['district_name'].str.title().str.replace(r'\bAnd\b','and')

            mapped_df = daily_df.merge(man_corr_lgd_codes, on=['state_name_data','district_name_data'], how='left')
            mapped_df = mapped_df[['date','state_name','state_code','district_name','district_code',
                                'retail_and_recreation','grocery_and_pharmacy','parks',
                                'transit_stations','workplaces','residential_places']]
            mapped_df.columns = ['date','state_name','state_code','district_name', 'district_code',    
                        'retail_and_recreation_district','grocery_and_pharmacy_district','parks_district','transit_stations_district','workplaces_district','residential_places_district']

            stategroup_df = mapped_df.groupby(['date','state_code'], sort=False)[['retail_and_recreation_district','grocery_and_pharmacy_district',
                                    'parks_district','transit_stations_district','workplaces_district',
                                    'residential_places_district']].mean().reset_index()
            stategroup_df.columns = ['date','state_code'] + ["_".join(col.split("_")[:-1])+'_state' for col in stategroup_df.columns.tolist()[2:]] 

            indiagroup_df = mapped_df.groupby(['date'], sort=False)[['retail_and_recreation_district','grocery_and_pharmacy_district',
                                    'parks_district','transit_stations_district','workplaces_district',
                                    'residential_places_district']].mean().reset_index()
            indiagroup_df.columns = ['date'] + ["_".join(col.split("_")[:-1])+'_india' for col in stategroup_df.columns.tolist()[2:]] 

            finaldf = mapped_df.merge(stategroup_df, on=['date','state_code'], how='left')
            finaldf = finaldf.merge(indiagroup_df, on=['date'], how='left')
            finaldf = finaldf[['date','state_name','state_code','district_name','district_code','retail_and_recreation_district', 
                            'grocery_and_pharmacy_district', 'parks_district','transit_stations_district','workplaces_district',
                            'residential_places_district','retail_and_recreation_state','grocery_and_pharmacy_state', 'parks_state', 'transit_stations_state',
                'workplaces_state', 'residential_places_state','retail_and_recreation_india', 'grocery_and_pharmacy_india',
                'parks_india', 'transit_stations_india', 'workplaces_india','residential_places_india']]
            finaldf = finaldf.sort_values(by=['date','state_name','district_name']).reset_index(drop=True)

            # Filter to current date
            filtered_df = finaldf[finaldf['date'] == curr_date].copy().reset_index(drop=True)

            if not filtered_df.empty:
                filtered_df['date'] = filtered_df['date'].dt.strftime("%d-%m-%Y")
                filtered_df.to_csv(os.path.join(daily_data_path, f"google_mobility_{curr_date.strftime('%d-%m-%Y')}.csv"), index=False)
            else:
                print(f"No Data available for: {curr_date.strftime('%d-%m-%Y')}")
                raise ValueError(f"No Data available for: {curr_date.strftime('%d-%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            print(e)


    process_google_mobility_daily_task = PythonOperator(
        task_id = 'process_google_mobility_daily',
        python_callable = process_google_mobility_daily,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_google_mobility_daily(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)  
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(daily_data_path, f"google_mobility_{curr_date.strftime('%d-%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"google_mobility_{curr_date.strftime('%d-%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            print(e)


    upload_google_mobility_daily_task = PythonOperator(
        task_id = 'upload_google_mobility_daily',
        python_callable = upload_google_mobility_daily,
        depends_on_past = True
    )

    scrape_google_mobility_daily_task >> process_google_mobility_daily_task >> upload_google_mobility_daily_task

