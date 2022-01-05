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
    'facebook_mobility_monthly',
    default_args=default_args,
    description='Facebook Mobility Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=11, day=2, hour=12, minute=0),
    catchup = True,
    tags=['mobility'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'facebook_mobility')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    main_url = "https://data.humdata.org/dataset/movement-range-maps"

    SECTOR_NAME = 'Mobility'
    DATASET_NAME = 'facebook_mobility_monthly'


    def process_facebook_mobility_monthly(ds, **context):  
        '''
        Process the scraped monthly raw data. 
        '''
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        source_file_date = datetime.fromtimestamp(context['data_interval_end'].timestamp())
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            ind_df = pd.read_csv( os.path.join(raw_data_path, f"movement-range-IN_{source_file_date.strftime('%m-%Y')}.csv"))
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

            merged_df['date'] = merged_df['date'].dt.to_period('M')
            merged_df = merged_df[merged_df['date'] == curr_date.strftime("%Y-%m")].copy()


            if not merged_df.empty:
                grouped_df = merged_df.groupby(['date','state_name','state_code','district_name','district_code']).mean().reset_index()
                grouped_df['date'] = grouped_df['date'].dt.strftime("01-%m-%Y")

                grouped_df['bng_tile_visited_district'] = grouped_df['bng_tile_visited_district']*100
                grouped_df['rat_sngl_tile_usr_district'] = grouped_df['rat_sngl_tile_usr_district']*100

                stategroup_df = grouped_df.groupby(['date','state_code'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
                stategroup_df.columns = ['date','state_code'] + ["_".join(col.split("_")[:-1])+'_state' for col in stategroup_df.columns.tolist()[2:]] 

                ind_grp_df = grouped_df.groupby(['date'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
                ind_grp_df.columns = ['date'] + ["_".join(col.split("_")[:-1])+'_india' for col in ind_grp_df.columns.tolist()[1:]] 

                finaldf = grouped_df.merge(stategroup_df, on=['date','state_code'], how='left')
                finaldf = finaldf.merge(ind_grp_df, on=['date'], how='left')
            
                finaldf.to_csv(os.path.join(monthly_data_path, f"facebook_mobility_{curr_date.strftime('%m-%Y')}.csv"), index=False)

            else:
                print(f"No Data available for: {curr_date.strftime('%m-%Y')}")
                raise ValueError(f"No Data available for: {curr_date.strftime('%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    process_facebook_mobility_monthly_task = PythonOperator(
        task_id = 'process_facebook_mobility_monthly',
        python_callable = process_facebook_mobility_monthly,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_facebook_mobility_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:

            filename = os.path.join(monthly_data_path, f"facebook_mobility_{curr_date.strftime('%m-%Y')}.csv")
            if os.path.exists(filename):
                upload_file(filename, DATASET_NAME, f"facebook_mobility_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
            else:
                raise ValueError("No file available:", filename)

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    upload_facebook_mobility_monthly_task = PythonOperator(
        task_id = 'upload_facebook_mobility_monthly',
        python_callable = upload_facebook_mobility_monthly,
        depends_on_past = True
    )

    process_facebook_mobility_monthly_task >> upload_facebook_mobility_monthly_task

