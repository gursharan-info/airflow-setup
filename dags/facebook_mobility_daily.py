import pendulum, os, requests, re, io
import pandas as pd
from datetime import datetime, timedelta
from zipfile import ZipFile, BadZipFile
from lxml import html

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_facebook_mobility_23-09-2021.csv'
main_url = 'https://data.humdata.org/dataset/movement-range-maps'

dir_path = os.path.join('/usr/local/airflow/data/hfi/', 'facebook_mobility')
raw_data_path = os.path.join(dir_path, 'raw_data')
daily_data_path = os.path.join(dir_path, 'daily')

gdrive_fb_mob_daily_folder = '1zh9T0OEzzc-v9FQTDyHOkvIPnWdkMFrP'
gdrive_fb_mob_raw_folder = '10QexGYwRtXzzwElbnAeItg-TrZcx7Gfi'
day_lag = 1


def get_latest_data(url, data_path):

    r = requests.get(url)
    if r.status_code == 200:
        myzipfile = ZipFile(io.BytesIO(r.content))

        for name in myzipfile.namelist():
            if 'movement' in name:
                print(name)
                data_file = myzipfile.read(name)
                file_loc = os.path.join(data_path, name)
                with open (file_loc,'wb') as f:
                    f.write(data_file)
                f.close()
                return file_loc
    else:
        return None
        

def facebook_mobility_daily(**context):
    # Load the main page
    try:
        print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        exec_date = (datetime.fromtimestamp(context['execution_date'].timestamp())- timedelta(day_lag))
        dates_list = [dat.strftime('%Y-%m-%d') for dat in pendulum.period(exec_date-timedelta(6), exec_date).range('days')]
        
        session = requests.Session()
        response = session.get(main_url)
        tree = html.fromstring(response.content)
        all_links = [a for a in tree.xpath('//div[@id="data-resources-0"]//a[contains(@class, "ga-download")]/@href')]
        session.close()
        curr_obj = [dict(link=link, date=date_str) for date_str in dates_list
                                                        for link in all_links
                                                            if date_str in link ]

        if curr_obj:
            data_url = f"https://data.humdata.org{curr_obj[0]['link']}"
            curr_date =  datetime.strptime(curr_obj[0]['date'], "%Y-%m-%d")
            print(curr_date, data_url)
        else:
            data_url = None

        raw_file_path = os.path.join(raw_data_path, f"movement-range-{re.sub(r'[^0-9-]','', data_url.split('/')[-1]).replace('-',' ').strip().replace(' ','-')}.txt" )

        if not os.path.exists( os.path.join(raw_data_path, f"movement-range-{re.sub(r'[^0-9-]','', data_url.split('/')[-1]).replace('-',' ').strip().replace(' ','-')}.txt" ) ):
            file_loc = get_latest_data(data_url, raw_data_path)
            
        if file_loc:
            raw_data = pd.read_csv(file_loc, sep="\t", low_memory=False)
            curr_date_str = curr_date.strftime("%Y-%m-%d")
            print('Got the data file', curr_date_str)

            ind_df = raw_data[raw_data['country'] == 'IND'][['ds','polygon_id','polygon_name','all_day_bing_tiles_visited_relative_change',
                                                 'all_day_ratio_single_tile_users']].copy()
            ind_df.columns = ['date','district_id','district_name_data','bng_tile_visited_district','rat_sngl_tile_usr_district']
            ind_df['date'] = pd.to_datetime(ind_df['date'], format="%Y-%m-%d")
            ind_df = ind_df[ind_df['date'] == curr_date.strftime('%Y-%m-%d')].reset_index(drop=True)
            ind_df['dist_lower'] = ind_df['district_name_data'].str.strip().str.lower()
            
            lgd_codes = pd.read_csv(lgd_codes_file)
            lgd_codes = lgd_codes[['dist_lower','state_name','state_code','district_name','district_code']]
            
            merged_df = ind_df.merge(lgd_codes, on='dist_lower', how='left')
            merged_df = merged_df[['date','state_name','state_code','district_name','district_code','bng_tile_visited_district','rat_sngl_tile_usr_district']]
            merged_df = merged_df.sort_values(by=['date','state_name','district_name'])

            grouped_df = merged_df.groupby(['date','state_name','state_code','district_name','district_code']).mean().reset_index()
            grouped_df['date'] = grouped_df['date'].dt.strftime("%d-%m-%Y")

            stategroup_df = grouped_df.groupby(['date','state_code'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
            stategroup_df.columns = ['date','state_code'] + ["_".join(col.split("_")[:-1])+'_state' for col in stategroup_df.columns.tolist()[2:]] 

            ind_grp_df = grouped_df.groupby(['date'], sort=False)[['bng_tile_visited_district','rat_sngl_tile_usr_district']].mean().reset_index()
            ind_grp_df.columns = ['date'] + ["_".join(col.split("_")[:-1])+'_india' for col in ind_grp_df.columns.tolist()[1:]] 

            finaldf = grouped_df.merge(stategroup_df, on=['date','state_code'], how='left')
            finaldf = finaldf.merge(ind_grp_df, on=['date'], how='left')

            filename = os.path.join(daily_data_path, 'fb_mobility_'+curr_date.strftime("%d-%m-%Y")+'.csv')
            finaldf.to_csv(filename,index=False)
            gupload.upload(filename, 'fb_mobility_'+curr_date.strftime("%d-%m-%Y")+'.csv',gdrive_fb_mob_daily_folder)

        else:
            print('No data available now. Please try and run the script later')

    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass
    

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=11, day=21, hour=18, minute=00 ).astimezone('Asia/Kolkata'),
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