import pendulum, os, re, requests, time
import pandas as pd
import numpy as np
import fiscalyear
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/mnrega_demand'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_mnrega_monthly_folder = '1QQiwbev9G29UsRGJKB76Mg1r2El8-Z_t'
gdrive_mnrega_raw_folder = '1UvrwhHRU71fpTcd2yaosn5fCLWGDVw13'
gdrive_mnrega_dist_hh_folder = '1IykW-U0HNTupLzHQlWWP6qa68PA3F4qA'
gdrive_mnrega_dist_persons_folder = '1wA4mTcsetGPvhsGHQIv3GoIAzZf819vG'


def get_states_data(url, raw_path, data_type, fiscal_year):
    req = requests.get(url)
    ind_soup = BeautifulSoup(req.content, 'html.parser')
    
    ind_table_html = ind_soup.find('table', id='t1')
    states_table_list = pd.read_html(str(ind_table_html))
    
    if len(states_table_list) > 0:
        states_df = states_table_list[0]
        states_df.columns = states_df.iloc[1]
        states_df = states_df.iloc[4:].copy()
        fname = f"states_{data_type}_{fiscal_year}.csv"
        states_df.to_csv(os.path.join(raw_path, fname), index=False)
        gupload.upload(os.path.join(raw_path, fname), fname, gdrive_mnrega_raw_folder)
    
    links = ind_table_html.findAll('a', href=True)
    return links


def get_districts_data(state_links, raw_path, data_type, fiscal_year, curr_date):
    dist_files_list = []

    for state in state_links:
        while True:
            try:
                state_url = f"http://mnregaweb4.nic.in/netnrega/{state['href']}"
                # print(state_url)
                state_req = requests.get(state_url)
                time.sleep(1)
                state_soup = BeautifulSoup(state_req.content, 'html.parser')

                state_table_html = state_soup.find('table', id='t1')
        #         print(state_table_html)
                districts_table_list = pd.read_html(str(state_table_html))

                dist_path = os.path.join(raw_path, r'dist_'+data_type)
                if not os.path.exists(dist_path):
                    os.mkdir(dist_path)
                
                folder_type = gdrive_mnrega_dist_persons_folder if data_type == 'persons' else gdrive_mnrega_dist_hh_folder

                if len(districts_table_list) > 0:
                    dists_df = districts_table_list[0]
                    dists_df.columns = dists_df.iloc[1]
                    dists_df = dists_df.iloc[4:].copy()
                    dists_df.insert(1, 'State',state.contents[0])
                    dists_df.loc[dists_df.index[-1], 'State']= np.nan
        #             display(dists_df)
                    filename = os.path.join(dist_path, f"{state.contents[0].title()}_{data_type}_{fiscal_year}.csv")
                    dists_df.to_csv(filename, index=False)
                    gupload.upload(filename, f"{state.contents[0].title()}_{data_type}_{fiscal_year}_{curr_date.strftime}.csv", folder_type)
                    dist_files_list.append(filename)
                del state_req, state_soup, state_table_html, districts_table_list
                break
            except Exception as e:
                print(e)
    return dist_files_list



def mnrega_demand_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        prev_mnth_date = curr_date
        # prev_mnth_date = curr_date.subtract(months=1)
        
        fiscalyear.setup_fiscal_calendar(start_month=4)
        fiscal_year_start = fiscalyear.FiscalYear.current().start
        fiscal_year_end = fiscalyear.FiscalYear.current().end
        fiscal_year = f"{fiscal_year_start.strftime('%Y')}-{fiscal_year_end.strftime('%Y')}"
        
        hh_url = f"http://mnregaweb4.nic.in/netnrega/demand_emp_demand.aspx?lflag=eng&file1=dmd&fin={fiscal_year}&fin_year={fiscal_year}&source=national&Digest=wc5i3wD4B1WhRXz2egmK8Q"
        persons_url = f"http://mnregaweb4.nic.in/netnrega/demand_emp_demand.aspx?lflag=eng&file1=dmd&fin={fiscal_year}&fin_year={fiscal_year}&source=national&rblhpb=Persons&Digest=wc5i3wD4B1WhRXz2egmK8Q"

        hh_state_links = get_states_data(hh_url, raw_path, 'households', fiscal_year)
        hh_dist_files = get_districts_data(hh_state_links, raw_path, 'households', fiscal_year, curr_date)

        hh_dist_df = pd.concat([pd.read_csv(f).iloc[:-1 , 1:] for f in hh_dist_files]).reset_index(drop=True)
        hh_dist_df.columns = ["State","District","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
        hh_dist_df = hh_dist_df.set_index(['State','District'])
        hh_dist_long_df = hh_dist_df.stack().reset_index()
        hh_dist_long_df.columns = ['state_name','district_name','date','demand_hh_district']
        hh_dist_long_df['date'] = pd.to_datetime(hh_dist_long_df['date'], format="%d-%m-%Y").dt.to_period('M')

        hh_state_df = pd.read_csv(os.path.join(raw_path, f"states_households_{fiscal_year}.csv")).iloc[:-1 , 1:]
        hh_state_df.columns = ["State","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
        hh_state_df = hh_state_df.set_index(['State'])
        hh_st_long_df = hh_state_df.stack().reset_index()
        hh_st_long_df.columns = ['state_name','date','demand_hh_state']
        hh_st_long_df['date'] = pd.to_datetime(hh_st_long_df['date'], format="%d-%m-%Y").dt.to_period('M')

        hh_df = hh_dist_long_df.merge(hh_st_long_df, on=['date','state_name'], how='left')

        person_state_links = get_states_data(persons_url, raw_path, 'persons', fiscal_year)
        person_dist_files = get_districts_data(person_state_links, raw_path, 'persons', fiscal_year, curr_date)

        person_dist_df = pd.concat([pd.read_csv(f).iloc[:-1 , 1:] for f in person_dist_files]).reset_index(drop=True)
        person_dist_df.columns = ["State","District","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
        person_dist_df = person_dist_df.set_index(['State','District'])
        person_dist_long_df = person_dist_df.stack().reset_index()
        person_dist_long_df.columns = ['state_name','district_name','date','demand_persons_district']
        person_dist_long_df['date'] = pd.to_datetime(person_dist_long_df['date'], format="%d-%m-%Y").dt.to_period('M')

        person_state_df = pd.read_csv(os.path.join(raw_path, f"states_persons_{fiscal_year}.csv")).iloc[:-1 , 1:]
        person_state_df.columns = ["State","01-04-2021","01-05-2021","01-06-2021","01-07-2021","01-08-2021","01-09-2021","01-10-2021","01-11-2021",
                                "01-12-2021","01-01-2022","01-02-2022","01-03-2022"]
        person_state_df = person_state_df.set_index(['State'])
        person_st_long_df = person_state_df.stack().reset_index()
        person_st_long_df.columns = ['state_name','date','demand_persons_state']
        person_st_long_df['date'] = pd.to_datetime(person_st_long_df['date'], format="%d-%m-%Y").dt.to_period('M')  

        persons_df = person_dist_long_df.merge(person_st_long_df, on=['date','state_name'], how='left')
        merged_df = persons_df.merge(hh_df, on=['date','state_name','district_name'], how='left').drop_duplicates()
        india_group_df = merged_df.groupby(['date'], as_index=False)[['demand_persons_district','demand_hh_district']].sum().reset_index(drop=True)
        india_group_df.columns = ['date','demand_persons_india','demand_hh_india']

        final_df = merged_df.merge(india_group_df, on='date', how='left')
        final_df = final_df[final_df['date'] == prev_mnth_date.strftime('%Y-%m')].reset_index(drop=True)
        final_df['date'] = final_df['date'].dt.strftime("01-%m-%Y")

        lgd_dist = pd.read_csv("https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_MNREGA_demand_10152021.csv")
        mapped_df = final_df.merge(lgd_dist, on=['state_name','district_name'],how='left')
        mapped_df = mapped_df[['date','state_name_lgd','state_code','district_name_lgd','district_code','demand_persons_district','demand_hh_district',
                               'demand_persons_state','demand_hh_state','demand_persons_india','demand_hh_india']]

        filename = os.path.join(data_path, f"mnrega_demand_monthly_{prev_mnth_date.strftime('%m%Y')}.csv")
        mapped_df.to_csv(filename, index=False)

        gupload.upload(filename, f"mnrega_demand_monthly_{prev_mnth_date.strftime('%m%Y')}.csv", gdrive_mnrega_monthly_folder)


    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=5, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

mnrega_demand_monthly_dag = DAG("mnregaDemandMonthlyScraping", default_args=default_args, schedule_interval='0 20 3 * *')

mnrega_demand_monthly_task = PythonOperator(task_id='mnrega_demand_monthly',
                                       python_callable = mnrega_demand_monthly,
                                       dag = mnrega_demand_monthly_dag,
                                       provide_context = True,
                                    )