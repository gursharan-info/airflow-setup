from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import os
import requests
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
    'covid_vacc_monthly',
    default_args=default_args,
    description='Covid Vaccination Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=11, day=1, hour=12, minute=0),
    catchup = True,
    tags=['health'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'covid_vacc')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(os.getcwd(), 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    source_url = 'https://data.incovid19.org/v4/min/timeseries.min.json'
    lgd_codes_file = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_covid_vacc_08-01-2022.csv"
    state_codes_url = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/state_two_digit.csv"

    SECTOR_NAME = 'Health'
    DATASET_NAME = 'covid_vaccination_monthly'
    day_lag = 1


    def scrape_covid_vacc_monthly(ds, **context):  
        '''
        Scrapes the monthly raw data of Covid Vaccination
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp()) - timedelta(day_lag)
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)
        
        try:
            
            main_session = requests.Session()
            response = main_session.get(source_url)
            source_data = response.json()
            main_session.close()
            two_letter_codes = [d for d in source_data]

            state_list = []
            for code in two_letter_codes:
                main_state_df = pd.json_normalize(source_data[code]['dates'], meta=['dates'], sep="_").melt()
                main_state_df[['date','count_type','indicator_type']] = main_state_df['variable'].str.split("_", expand=True)
                main_state_df = main_state_df[['date','count_type','indicator_type','value']]
                main_state_df.insert(1, 'state', code) 
                
                state_list.append(main_state_df)
            combined_df = pd.concat(state_list).reset_index(drop=True)
            state_long_df = combined_df[(combined_df['count_type'] == 'delta')].reset_index(drop=True)

            state_wide_df = state_long_df.pivot_table(index=['date', 'state'], columns=['indicator_type'], values=['value'],
                              aggfunc='sum')
            state_wide_df.columns = [col[-1] for col in state_wide_df.columns]
            state_wide_df = state_wide_df.reset_index().fillna(0)

            state_df_grp = state_wide_df[~(state_wide_df['state'] == 'TT')][['date','state','vaccinated1','vaccinated2']].copy().reset_index(drop=True)
            state_df_grp['total_doses_admn'] = state_df_grp['vaccinated1'] + state_df_grp['vaccinated2']
            state_df_grp.columns = ['date','state','first_dose_admn_state','second_dose_admn_state','total_doses_admn_state']
            state_df_grp['date'] = pd.to_datetime(state_df_grp['date'], format="%Y-%m-%d").dt.to_period('M')
            state_df_grp = state_df_grp[ state_df_grp['date'] == curr_date.strftime("%Y-%m") ].copy().reset_index(drop=True)
            state_df_grp = state_df_grp.groupby(['date','state']).sum().reset_index()
            

            ind_df = state_wide_df[state_wide_df['state'] == 'TT'][['date','vaccinated1','vaccinated2']].copy().reset_index(drop=True)
            ind_df['total_doses_admn'] = ind_df['vaccinated1'] + ind_df['vaccinated2']
            ind_df.columns = ['date','first_dose_admn_india','second_dose_admn_india','total_doses_admn_india']
            ind_df['date'] = pd.to_datetime(ind_df['date'], format="%Y-%m-%d").dt.to_period('M')
            ind_df = ind_df[ind_df['date'] == curr_date.strftime("%Y-%m")].copy().reset_index(drop=True)
            ind_df = ind_df.groupby(['date']).sum().reset_index()


            lgd_df = pd.read_csv(lgd_codes_file)
            lgd_df['state_dist_lower'] = lgd_df['state_name'].str.strip().str.lower() + "_" + lgd_df['district_name'].str.strip().str.lower()
            state_codes_df = pd.read_csv(state_codes_url)

            dist_list = []
            ## Ignore two state code, TT stands for total India values of date, UN is for unkown state
            state_codes = [d for d in source_data if not d in ['TT','UN']]

            for state in state_codes:
                session = requests.Session()
                state_url = f"https://data.incovid19.org/v4/min/timeseries-{state}.min.json"
                print(state_url)
                resp = session.get(state_url)
                if resp.status_code == 200:
                    state_data = resp.json()
                    # print(state_data[state]['dates'])
                    dist_df = pd.json_normalize(state_data[state]['dates'], meta=['dates'], sep="_").melt()
                    dist_df = pd.json_normalize(state_data[state]['districts'], meta=['dates'], sep="_").melt()
                    dist_df[['district','dates','date','count_type','indicator_type']] = dist_df['variable'].str.split("_", expand=True)
                    dist_df = dist_df[['date','district','count_type','indicator_type','value']]
                    dist_df.insert(2, 'state', state) 
                    dist_list.append(dist_df)
                else:
                    print("No data available for thise URL: ", state_url)
                session.close()
                
            dist_combined_df = pd.concat(dist_list).reset_index(drop=True)
            dist_long_df = dist_combined_df[(dist_combined_df['count_type'] == 'delta')].reset_index(drop=True)

            dist_wide_df = dist_long_df.pivot_table(index=['date', 'state', 'district'], columns=['indicator_type'],
                        values=['value'], aggfunc='sum')
            dist_wide_df.columns = [col[-1] for col in dist_wide_df.columns]
            dist_wide_df = dist_wide_df.reset_index().fillna(0)

            dist_df_grp = dist_wide_df[~(dist_wide_df['state'] == 'TT')][['date','state','district','vaccinated1','vaccinated2']].copy().reset_index(drop=True)
            dist_df_grp['total_doses_admn'] = dist_df_grp['vaccinated1'] + dist_df_grp['vaccinated2']
            dist_df_grp.columns = ['date','state','district','first_dose_admn_district','second_dose_admn_district','total_doses_admn_district']
            dist_df_grp['date'] = pd.to_datetime(dist_df_grp['date'], format="%Y-%m-%d").dt.to_period('M')
            dist_df_grp = dist_df_grp[dist_df_grp['date'] == curr_date.strftime("%Y-%m")].copy().reset_index(drop=True)
            dist_df_grp = dist_df_grp.groupby(['date','state','district']).sum().reset_index()

            merged_df = pd.merge(
                pd.merge(dist_df_grp, state_df_grp, on=['date','state'], how='left'),
            ind_df, on='date', how='left')
            merged_df = pd.merge(merged_df, state_codes_df, on='state', how='left')
            # merged_df = merged_df[ ['date','state_name','state_code','district_name'] ]
            merged_df['state_dist_lower'] = merged_df['state_name'].str.strip().str.lower() + "_" + merged_df['district'].str.strip().str.lower()
            merged_df = merged_df.merge(lgd_df[['district_name','district_code','state_dist_lower']], on='state_dist_lower', how='left')
            merged_df = merged_df.dropna(subset=['district_code']).reset_index(drop=True)
            merged_df = merged_df[ ['date','state_name','state_code','district_name','district_code'] + [col for col in merged_df.columns.tolist() if 'dose' in col] ]
            merged_df['district_code'] = merged_df['district_code'].astype(int)
            merged_df['date'] = merged_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(monthly_data_path, f"covid_vacc_{curr_date.strftime('%m-%Y')}.csv")
            merged_df.to_csv(filename,index=False)                                 

            return f"Downloaded data file for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            raise ValueError(e)

    scrape_covid_vacc_monthly_task = PythonOperator(
        task_id = 'scrape_covid_vacc_monthly',
        python_callable = scrape_covid_vacc_monthly,
        # depends_on_past = True
    )



    # Upload data file 
    def upload_covid_vacc_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp()) - timedelta(day_lag)  
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(monthly_data_path, f"covid_vacc_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"covid_vacc_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
        
        except Exception as e:
            raise ValueError(e)

        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"
        

    upload_covid_vacc_monthly_task = PythonOperator(
        task_id = 'upload_covid_vacc_monthly',
        python_callable = upload_covid_vacc_monthly,
        # depends_on_past = True
    )

    scrape_covid_vacc_monthly_task >> upload_covid_vacc_monthly_task

