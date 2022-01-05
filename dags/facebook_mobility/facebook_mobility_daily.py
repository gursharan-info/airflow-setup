from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from lxml import html

from bipp.sharepoint.uploads import upload_file, upload_large_file


def get_data_file(url, raw_path, curr_date):
    r = requests.get(url)
    myzipfile = ZipFile(BytesIO(r.content))
    
    for name in myzipfile.namelist():
        if 'movement' in name:
            print(name)
            data_file = myzipfile.read(name)
            file_loc = os.path.join(raw_path, f"movement-range-{curr_date.strftime('%m-%Y')}.txt")
            with open (file_loc,'wb') as f:
                f.write(data_file)
            f.close()
            return file_loc
        

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
    'facebook_mobility_daily',
    default_args=default_args,
    description='Google Mobility Daily',
    schedule_interval = '@daily',
    start_date = datetime(year=2021, month=12, day=10, hour=12, minute=0),
    catchup = True,
    tags=['mobility'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'facebook_mobility')
    daily_data_path = os.path.join(dir_path, 'daily')
    os.makedirs(daily_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    main_url = "https://data.humdata.org/dataset/movement-range-maps"

    SECTOR_NAME = 'Mobility'
    DATASET_NAME = 'facebook_mobility_daily'
    day_lag = 3


    def scrape_facebook_mobility_daily(ds, **context):  
        '''
        Scrapes the daily raw data of digital payments
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            session = requests.Session()
            response = session.get(main_url)
            tree = html.fromstring(response.content)

            curr_link = None

            for a in tree.xpath('//*[@id="data-resources-0"]//a'):
                href_link = str(a.get("href"))
                if curr_date.strftime("%Y-%m") in href_link:
                    curr_link = "https://data.humdata.org" + href_link
                    print(curr_link)
            session.close()

            raw_file = get_data_file(curr_link, raw_data_path, curr_date)
            raw_df = pd.read_csv( raw_file, sep="\t", low_memory=False)
            ind_df = raw_df[raw_df['country'] == 'IND'][['ds','polygon_id','polygon_name','all_day_bing_tiles_visited_relative_change',
                                                            'all_day_ratio_single_tile_users']].copy()
            raw_file_IN = os.path.join(raw_data_path, f"movement-range-IN_{curr_date.strftime('%m-%Y')}.csv")
            ind_df.to_csv(raw_file_IN, index=False)

            if(os.path.isfile(raw_file)):
                os.remove(raw_file)                                          
            
            # upload_large_file(raw_file_IN, f"{DATASET_NAME}/raw_data", f"movement-range-IN_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Downloaded data file till: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            raise ValueError(e)

    scrape_facebook_mobility_daily_task = PythonOperator(
        task_id = 'scrape_facebook_mobility_daily',
        python_callable = scrape_facebook_mobility_daily,
        depends_on_past = True
    )



    def process_facebook_mobility_daily(ds, **context):  
        '''
        Process the scraped daily raw data. 
        '''
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            ind_df = pd.read_csv( os.path.join(raw_data_path, f"movement-range-IN_{curr_date.strftime('%m-%Y')}.csv"))
            ind_df.columns = ['date','district_id','district_name_data','bng_tile_visited_district','rat_sngl_tile_usr_district']
            ind_df['date'] = pd.to_datetime(ind_df['date'], format="%Y-%m-%d")
            ind_df['dist_lower'] = ind_df['district_name_data'].str.strip().str.lower()

            lgd_codes = pd.read_csv('https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_facebook_mobility_23-09-2021.csv')
            lgd_codes = lgd_codes[['dist_lower','state_name','state_code','district_name','district_code']]

            merged_df = ind_df.merge(lgd_codes, on='dist_lower', how='left')
            merged_df = merged_df[['date','state_name','state_code','district_name','district_code','bng_tile_visited_district','rat_sngl_tile_usr_district']]
            merged_df = merged_df.sort_values(by=['date','state_name','district_name'])
            merged_df = merged_df[~merged_df['district_code'].isna()].reset_index(drop=True)
            merged_df['state_name'] = merged_df['state_name'].str.strip()
            merged_df['district_name'] = merged_df['district_name'].str.strip()


            grouped_df = merged_df.groupby(['date','state_name','state_code','district_name','district_code']).mean().reset_index()
            grouped_df = grouped_df[grouped_df['date'] == curr_date.strftime("%Y-%m-%d")].copy()

            if not grouped_df.empty:
                grouped_df = grouped_df.sort_values(by=['date','state_name','district_name']).reset_index(drop=True)
                grouped_df['date'] = grouped_df['date'].dt.strftime("%d-%m-%Y")

                grouped_df['bng_tile_visited_district'] = grouped_df['bng_tile_visited_district']*100
                grouped_df['rat_sngl_tile_usr_district'] = grouped_df['rat_sngl_tile_usr_district']*100

                stategroup_df = grouped_df.groupby(['date','state_code'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
                stategroup_df.columns = ['date','state_code'] + ["_".join(col.split("_")[:-1])+'_state' for col in stategroup_df.columns.tolist()[2:]] 

                ind_grp_df = grouped_df.groupby(['date'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
                ind_grp_df.columns = ['date'] + ["_".join(col.split("_")[:-1])+'_india' for col in ind_grp_df.columns.tolist()[1:]] 

                finaldf = grouped_df.merge(stategroup_df, on=['date','state_code'], how='left')
                finaldf = finaldf.merge(ind_grp_df, on=['date'], how='left')
            
                finaldf.to_csv(os.path.join(daily_data_path, f"facebook_mobility_{curr_date.strftime('%d-%m-%Y')}.csv"), index=False)
            else:
                print(f"No Data available for: {curr_date.strftime('%d-%m-%Y')}")
                raise ValueError(f"No Data available for: {curr_date.strftime('%d-%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    process_facebook_mobility_daily_task = PythonOperator(
        task_id = 'process_facebook_mobility_daily',
        python_callable = process_facebook_mobility_daily,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_facebook_mobility_daily(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)  
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            raw_file_IN = os.path.join(raw_data_path, f"movement-range-IN_{curr_date.strftime('%m-%Y')}.csv")
            upload_large_file(raw_file_IN, f"{DATASET_NAME}/raw_data", f"movement-range-IN_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            filename = os.path.join(daily_data_path, f"facebook_mobility_{curr_date.strftime('%d-%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"facebook_mobility_{curr_date.strftime('%d-%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%d-%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    upload_facebook_mobility_daily_task = PythonOperator(
        task_id = 'upload_facebook_mobility_daily',
        python_callable = upload_facebook_mobility_daily,
        depends_on_past = True
    )

    scrape_facebook_mobility_daily_task >> process_facebook_mobility_daily_task >> upload_facebook_mobility_daily_task

