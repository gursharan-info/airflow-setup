from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os, re, requests, time
import fiscalyear
from bs4 import BeautifulSoup

from bipp.sharepoint.uploads import upload_file



def get_states_data(url, raw_path, data_type, fiscal_year):
    session = requests.Session()
    req = session.get(url)
    ind_soup = BeautifulSoup(req.content, 'html.parser')
    session.close()
    ind_table_html = ind_soup.find('table', id='t1')
    states_table_list = pd.read_html(str(ind_table_html))
    
    if len(states_table_list) > 0:
        states_df = states_table_list[0]
        states_df.columns = states_df.iloc[1]
        states_df = states_df.iloc[4:].copy()
        fname = f"states_{data_type}_{fiscal_year}.csv"
        states_df.to_csv(os.path.join(raw_path, fname), index=False)
        # gupload.upload(os.path.join(raw_path, fname), fname, gdrive_mnrega_raw_folder)
        upload_file(os.path.join(raw_path, fname), f"{DATASET_NAME}/raw_data", fname, SECTOR_NAME, "india_pulse")

    
    links = ind_table_html.findAll('a', href=True)
    return links


def get_districts_data(state_links, raw_path, data_type, fiscal_year, curr_date, DATASET_NAME):
    dist_files_list = []

    for state in state_links:
        while True:
            try:
                state_url = f"http://mnregaweb4.nic.in/netnrega/{state['href']}"
                # print(state_url)
                session = requests.Session()
                state_req = session.get(state_url)
                time.sleep(1)
                state_soup = BeautifulSoup(state_req.content, 'html.parser')
                session.close()

                state_table_html = state_soup.find('table', id='t1')
        #         print(state_table_html)
                districts_table_list = pd.read_html(str(state_table_html))

                dist_path = os.path.join(raw_path, r'dist_'+data_type)
                if not os.path.exists(dist_path):
                    os.mkdir(dist_path)
                
                # folder_type = gdrive_mnrega_dist_persons_folder if data_type == 'persons' else gdrive_mnrega_dist_hh_folder
                if data_type == 'persons':
                    target_folder_path = f"{DATASET_NAME}/raw_data/districts/persons"
                else:
                    target_folder_path = f"{DATASET_NAME}/raw_data/districts/households"

                if len(districts_table_list) > 0:
                    dists_df = districts_table_list[0]
                    dists_df.columns = dists_df.iloc[1]
                    dists_df = dists_df.iloc[4:].copy()
                    dists_df.insert(1, 'State',state.contents[0])
                    dists_df.loc[dists_df.index[-1], 'State']= np.nan
        #             display(dists_df)
                    filename = os.path.join(dist_path, f"{state.contents[0].title()}_{data_type}_{fiscal_year}.csv")
                    dists_df.to_csv(filename, index=False)
                    # gupload.upload(filename, f"{state.contents[0].title()}_{data_type}_{fiscal_year}_{curr_date.strftime('%m-%Y')}.csv", folder_type)
                    upload_file(filename, target_folder_path, f"{state.contents[0].title()}_{data_type}_{fiscal_year}_{curr_date.strftime('%m-%Y')}.csv", 
                                    SECTOR_NAME, "india_pulse")

                    dist_files_list.append(filename)
                del state_req, state_soup, state_table_html, districts_table_list
                break
            except Exception as e:
                print(e)
    return dist_files_list



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
    'mnrega_demand_monthly',
    default_args=default_args,
    description='MNREGA Demand Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['employment'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'mca')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    lgd_codes_file = "https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_MNREGA_demand_10152021.csv"

    SECTOR_NAME = 'Employment'
    DATASET_NAME = 'nrega_monthly'


    def scrape_mnrega_demand_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            
            fiscalyear.setup_fiscal_calendar(start_month=4)
            fiscal_year_start = fiscalyear.FiscalYear.current().start
            fiscal_year_end = fiscalyear.FiscalYear.current().end
            fiscal_year = f"{fiscal_year_start.strftime('%Y')}-{fiscal_year_end.strftime('%Y')}"
            
            hh_url = f"http://mnregaweb4.nic.in/netnrega/demand_emp_demand.aspx?lflag=eng&file1=dmd&fin={fiscal_year}&fin_year={fiscal_year}&source=national&Digest=wc5i3wD4B1WhRXz2egmK8Q"
            persons_url = f"http://mnregaweb4.nic.in/netnrega/demand_emp_demand.aspx?lflag=eng&file1=dmd&fin={fiscal_year}&fin_year={fiscal_year}&source=national&rblhpb=Persons&Digest=wc5i3wD4B1WhRXz2egmK8Q"

            # Scrape Households and District data
            hh_state_links = get_states_data(hh_url, raw_data_path, 'households', fiscal_year)
            hh_dist_files = get_districts_data(hh_state_links, raw_data_path, 'households', fiscal_year, curr_date, DATASET_NAME)

            hh_dist_df = pd.concat([pd.read_csv(f).iloc[:-1 , 1:] for f in hh_dist_files]).reset_index(drop=True)
            hh_dist_df.columns = ["State","District","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                    "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
            hh_dist_df = hh_dist_df.set_index(['State','District'])
            hh_dist_long_df = hh_dist_df.stack().reset_index()
            hh_dist_long_df.columns = ['state_name','district_name','date','demand_hh_district']
            hh_dist_long_df['date'] = pd.to_datetime(hh_dist_long_df['date'], format="%d-%m-%Y").dt.to_period('M')

            # Scrape Persons District data
            person_state_links = get_states_data(persons_url, raw_data_path, 'persons', fiscal_year)
            person_dist_files = get_districts_data(person_state_links, raw_data_path, 'persons', fiscal_year, curr_date, DATASET_NAME)

            person_dist_df = pd.concat([pd.read_csv(f).iloc[:-1 , 1:] for f in person_dist_files]).reset_index(drop=True)
            person_dist_df.columns = ["State","District","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                    "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
            person_dist_df = person_dist_df.set_index(['State','District'])
            person_dist_long_df = person_dist_df.stack().reset_index()
            person_dist_long_df.columns = ['state_name','district_name','date','demand_persons_district']
            person_dist_long_df['date'] = pd.to_datetime(person_dist_long_df['date'], format="%d-%m-%Y").dt.to_period('M')

            ## Combine district level data and Map LGD codes
            dist_df = person_dist_long_df.merge(hh_dist_long_df, on=['date','state_name','district_name'], how='left')
            lgd_dist = pd.read_csv(lgd_codes_file)

            mapped_dist_df = dist_df.merge(lgd_dist, on=['state_name','district_name'],how='left')
            mapped_dist_df = mapped_dist_df[['date','state_name_lgd','state_code','district_name_lgd','district_code','demand_persons_district','demand_hh_district']]
            mapped_dist_df.columns=['date','state_name','state_code','district_name','district_code','demand_persons_district','demand_hh_district']
            grouped_dist_df = mapped_dist_df.groupby(['date','state_name','state_code','district_name','district_code'])[
                                ['demand_persons_district','demand_hh_district']].sum().reset_index()

            # Get Persons State data
            person_state_df = pd.read_csv(os.path.join(raw_data_path, f"states_persons_{fiscal_year}.csv")).iloc[:-1 , 1:]
            person_state_df.columns = ["State","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                    "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
            person_state_df = person_state_df.set_index(['State'])
            person_st_long_df = person_state_df.stack().reset_index()
            person_st_long_df.columns = ['state_name','date','demand_persons_state']
            person_st_long_df['date'] = pd.to_datetime(person_st_long_df['date'], format="%d-%m-%Y").dt.to_period('M')
            
            # Get Households State data
            hh_state_df = pd.read_csv(os.path.join(raw_data_path, f"states_households_{fiscal_year}.csv")).iloc[:-1 , 1:]
            hh_state_df.columns = ["State","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                    "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
            hh_state_df = hh_state_df.set_index(['State'])
            hh_st_long_df = hh_state_df.stack().reset_index()
            hh_st_long_df.columns = ['state_name','date','demand_hh_state']
            hh_st_long_df['date'] = pd.to_datetime(hh_st_long_df['date'], format="%d-%m-%Y").dt.to_period('M')
            
            ## Combine state level data and Map LGD codes
            state_df = person_st_long_df.merge(hh_st_long_df, on=['date','state_name'], how='left')
            
            lgd_state = pd.read_csv(lgd_codes_file)
            lgd_state = lgd_state[['state_name','state_name_lgd','state_code']].drop_duplicates().reset_index(drop=True)
            mapped_state_df = state_df.merge(lgd_state, on=['state_name'],how='left')
            mapped_state_df = mapped_state_df[['date','state_code','demand_persons_state','demand_hh_state']]

            india_group_df = grouped_dist_df.groupby(['date'], as_index=False)[['demand_persons_district','demand_hh_district']].sum().reset_index(drop=True)
            india_group_df.columns = ['date','demand_persons_india','demand_hh_india']

            merged_df = grouped_dist_df.merge(mapped_state_df, on=['date','state_code'], how='left').drop_duplicates()
            final_df = merged_df.merge(india_group_df, on='date', how='left')
            final_df = final_df[final_df['date'] == curr_date.strftime('%Y-%m')].reset_index(drop=True)
            final_df['date'] = final_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(monthly_data_path, f"mnrega_demand_{curr_date.strftime('%m-%Y')}.csv")
            final_df.to_csv(filename, index=False)


            return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

            # else:
                # raise ValueError(f"No data available yet for: : {curr_date.strftime('%m-%Y')}")

        except Exception as e:
            raise ValueError(e)


    scrape_mnrega_demand_monthly_task = PythonOperator(
        task_id = 'scrape_mnrega_demand_monthly',
        python_callable = scrape_mnrega_demand_monthly,
        depends_on_past = True
    )

    

    # Upload data file 
    def upload_mnrega_demand_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:

            filename = os.path.join(monthly_data_path, f"mnrega_demand_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"mnrega_demand_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except Exception as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_mnrega_demand_monthly_task = PythonOperator(
        task_id = 'upload_mnrega_demand_monthly',
        python_callable = upload_mnrega_demand_monthly,
        depends_on_past = True
    )

    scrape_mnrega_demand_monthly_task >> upload_mnrega_demand_monthly_task

