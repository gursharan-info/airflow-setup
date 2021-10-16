import pendulum,  os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_google_mobility_02-09-2021.csv'
dir_path = '/usr/local/airflow/data/hfi/google_mobility'
data_path = os.path.join(dir_path, 'monthly')
gdrive_mobility_monthly_folder = '1ABKP8kgY_0qrEEqTmBp-BhBDKJtJeNoW'


def read_mobility_data_monthly(**context):
    # Load the main page
    try:
        print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        # prev_mnth_date = context['execution_date'].subtract(months=1)
        prev_mnth_date = context['execution_date']
        prev_month = prev_mnth_date.strftime('%Y-%m')

        raw_df = pd.read_csv('https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv')
        india_df = raw_df[raw_df['country_region_code'] == 'IN'].copy()
        india_df = india_df.drop(["country_region_code","country_region",'metro_area','iso_3166_2_code','census_fips_code',
                          'place_id'],axis=1).reset_index(drop=True)
        india_df = india_df.rename({'sub_region_2':'district_name_data', 'sub_region_1':'state_name_data'},axis=1)
        india_df['date'] = pd.to_datetime(india_df['date'], format="%Y-%m-%d")
        india_df['date'] = india_df['date'].dt.to_period('M')

        test_df = india_df[india_df['date'] == prev_month].copy()

        if test_df.empty: # Check that is empty, which means data for date is not available
            raise Exception(f"No data available for {prev_month}")
        else:
            monthly_df = india_df.groupby(['date','state_name_data','district_name_data'])['retail_and_recreation_percent_change_from_baseline',
                                'grocery_and_pharmacy_percent_change_from_baseline','parks_percent_change_from_baseline',
                                'transit_stations_percent_change_from_baseline','workplaces_percent_change_from_baseline',
                                'residential_percent_change_from_baseline'].mean().reset_index()
            monthly_df.columns = ['date','state_name_data','district_name_data','retail_and_recreation','grocery_and_pharmacy','parks',
                                'transit_stations','workplaces','residential_places']

            man_corr_lgd_codes = pd.read_csv(lgd_codes_file)
            man_corr_lgd_codes['state_name'] = man_corr_lgd_codes['state_name'].str.title().str.replace(r'\bAnd\b','and')
            man_corr_lgd_codes['district_name'] = man_corr_lgd_codes['district_name'].str.title().str.replace(r'\bAnd\b','and')

            mapped_df = monthly_df.merge(man_corr_lgd_codes, on=['state_name_data','district_name_data'], how='left')
            mapped_df = mapped_df[['date','state_name','state_code','district_name','district_code','retail_and_recreation','grocery_and_pharmacy','parks',
                                'transit_stations','workplaces','residential_places']]
            # from_date = mapped_df['date'].min().strftime("%d%m%Y")
            # to_date = mapped_df['date'].max().strftime("%d%m%Y")

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

            filtered_df = finaldf[finaldf['date'] == prev_month].copy()
            filtered_df['date'] = filtered_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(data_path, 'google_mobility_'+prev_mnth_date.strftime('%m%Y')+'.csv')
            filtered_df.to_csv(filename,index=False)
            gupload.upload(filename, 'google_mobility_'+prev_mnth_date.strftime('%m%Y')+'.csv',gdrive_mobility_monthly_folder)

    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2020, month=3, day=6, hour=00, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

read_mobility_data_monthly_dag = DAG("googleMobilityMonthlyScraping", default_args=default_args, schedule_interval='0 20 5 * *')

read_mobility_data_monthly_task = PythonOperator(task_id='read_mobility_data_monthly',
                                       python_callable = read_mobility_data_monthly,
                                       dag = read_mobility_data_monthly_dag,
                                       provide_context = True,
                                    )