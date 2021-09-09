import pendulum, os, requests, zipfile, io
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_facebook_mobility_08-09-2021.csv'
source_data_url = 'https://data.humdata.org/dataset/c3429f0e-651b-4788-bb2f-4adbf222c90e/resource/55a51014-0d27-49ae-bf92-c82a570c2c6c/download/movement-range-data-2021-09-06.zip'

dir_path = os.path.join('/usr/local/airflow/data/hfi/', 'facebook_mobility')
raw_data_path = os.path.join(dir_path, 'raw_data')
daily_data_path = os.path.join(dir_path, 'daily')

gdrive_fb_mob_daily_folder = '1zh9T0OEzzc-v9FQTDyHOkvIPnWdkMFrP'
day_lag = 1

def get_data_file(url, raw_path):
    r = requests.get(url)
    myzipfile = zipfile.ZipFile(io.BytesIO(r.content))
    
    for name in myzipfile.namelist():
        if 'movement' in name:
            print(name)
            data_file = myzipfile.read(name)
            file_loc = os.path.join(raw_path, name)
            with open (file_loc,'wb') as f:
                f.write(data_file)
            f.close()
            return file_loc
        

def facebook_mobility_daily(**context):
    # Load the main page
    try:
        print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date = (datetime.fromtimestamp(context['execution_date'].timestamp())- timedelta(day_lag))
        curr_date_str = curr_date.strftime("%Y-%m-%d")

        data_file = get_data_file(source_data_url, raw_data_path)

        raw_data = pd.read_csv(data_file, sep="\t", low_memory=False)

        ind_df = raw_data[raw_data['country'] == 'IND'][['ds','polygon_name','all_day_bing_tiles_visited_relative_change',
                                                 'all_day_ratio_single_tile_users']].copy()
        ind_df.columns = ['date','district_name_data','bng_tile_visited','rat_sngl_tile_usr']
        ind_df['date'] = pd.to_datetime(ind_df['date'], format="%Y-%m-%d")
        ind_df = ind_df[ind_df['date'] == curr_date_str].reset_index(drop=True)
        ind_df['dist_lower'] = ind_df['district_name_data'].str.strip().str.lower()

        lgd_codes = pd.read_csv(lgd_codes_file)
        lgd_codes = lgd_codes[['dist_lower','state_name','state_code','district_name','district_code']]

        merged_df = ind_df.merge(lgd_codes, on='dist_lower', how='left')
        merged_df = merged_df[['date','state_name','state_code','district_name','district_code','bng_tile_visited','rat_sngl_tile_usr']]

        grouped_df = merged_df.groupby(['date','state_name','state_code','district_name','district_code']).mean().reset_index()
        grouped_df['date'] = grouped_df['date'].dt.strftime("%d-%m-%Y")

        filename = os.path.join(daily_data_path, 'fb_mobility_'+curr_date.strftime("%d-%m-%Y")+'.csv')
        grouped_df.to_csv(filename,index=False)
        gupload.upload(filename, 'fb_mobility_'+curr_date.strftime("%d-%m-%Y")+'.csv',gdrive_fb_mob_daily_folder)


    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=9, day=4, hour=2, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

facebook_mobility_daily_dag = DAG("facebookMobilityDailyScraping", default_args=default_args, schedule_interval="@daily")

facebook_mobility_daily_task = PythonOperator(task_id='facebook_mobility_daily',
                                       python_callable = facebook_mobility_daily,
                                       dag = facebook_mobility_daily_dag,
                                       provide_context = True,
                                    )