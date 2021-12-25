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
    'google_mobility_monthly',
    default_args=default_args,
    description='Google Mobility Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=11, day=2, hour=12, minute=0),
    catchup = True,
    tags=['google_mobility'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'google_mobility')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    SECTOR_NAME = 'Mobility'
    DATASET_NAME = 'google_mobility_monthly'


    def process_google_mobility_monthly(ds, **context):  
        '''
        Process the scraped monthly raw data. 
        '''
        curr_date = context['data_interval_start']
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            raw_filename = os.path.join(raw_data_path, f"Global_Mobility_Report_IN.csv")
            raw_df = pd.read_csv(raw_filename)
            india_df = raw_df.drop(["country_region_code","country_region",'metro_area','iso_3166_2_code','census_fips_code',
                                    'place_id'],axis=1).reset_index(drop=True)
            india_df = india_df.rename({'sub_region_2':'district_name_data', 'sub_region_1':'state_name_data'},axis=1)
            india_df['date'] = pd.to_datetime(india_df['date'], format="%Y-%m-%d")
            india_df['date'] = india_df['date'].dt.to_period('M')

            monthly_df = india_df.groupby(['date','state_name_data','district_name_data'])['retail_and_recreation_percent_change_from_baseline',
                                'grocery_and_pharmacy_percent_change_from_baseline','parks_percent_change_from_baseline',
                                'transit_stations_percent_change_from_baseline','workplaces_percent_change_from_baseline',
                                'residential_percent_change_from_baseline'].mean().reset_index()
            monthly_df.columns = ['date','state_name_data','district_name_data','retail_and_recreation','grocery_and_pharmacy','parks',
                                'transit_stations','workplaces','residential_places']

            # LGD Codes
            man_corr_lgd_codes = pd.read_csv('https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_google_mobility_02-09-2021.csv')
            man_corr_lgd_codes['state_name'] = man_corr_lgd_codes['state_name'].str.title().str.replace(r'\bAnd\b','and')
            man_corr_lgd_codes['district_name'] = man_corr_lgd_codes['district_name'].str.title().str.replace(r'\bAnd\b','and')

            mapped_df = monthly_df.merge(man_corr_lgd_codes, on=['state_name_data','district_name_data'], how='left')
            mapped_df = mapped_df[['date','state_name','state_code','district_name','district_code','retail_and_recreation','grocery_and_pharmacy','parks',
                                'transit_stations','workplaces','residential_places']]

            mapped_df.columns = ['date','state_name','state_code','district_name', 'district_code','retail_and_recreation_district',
                                'grocery_and_pharmacy_district','parks_district','transit_stations_district','workplaces_district',
                                'residential_places_district']

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

            filtered_df = finaldf[finaldf['date'] == curr_date.strftime('%Y-%m')].copy().reset_index(drop=True)

            if not filtered_df.empty:
                filtered_df['date'] = filtered_df['date'].dt.strftime("01-%m-%Y")
                filtered_df.to_csv(os.path.join(monthly_data_path, f"google_mobility_{curr_date.strftime('%m-%Y')}.csv"), index=False)
            else:
                print(f"No Data available for: {curr_date.strftime('%m-%Y')}")
                raise ValueError(f"No Data available for: {curr_date.strftime('%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    process_google_mobility_monthly_task = PythonOperator(
        task_id = 'process_google_mobility_monthly',
        python_callable = process_google_mobility_monthly,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_google_mobility_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = context['data_interval_start']
        print("Uploading data file for: ", curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(monthly_data_path, f"google_mobility_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"google_mobility_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    upload_google_mobility_monthly_task = PythonOperator(
        task_id = 'upload_google_mobility_monthly',
        python_callable = upload_google_mobility_monthly,
        depends_on_past = True
    )

    process_google_mobility_monthly_task >> upload_google_mobility_monthly_task

