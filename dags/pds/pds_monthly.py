from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os, re, requests, time
import fiscalyear
from bs4 import BeautifulSoup
from requests.utils import requote_uri

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
    'pds_monthly',
    default_args=default_args,
    description='PDS Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['agriculture'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'pds')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    state_codes = "https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/pds_state_name_codes.json"
    lgd_codes_file = "https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_pds_monthly_05102021.csv"

    SECTOR_NAME = 'Agriculture'
    DATASET_NAME = 'pds_monthly'


    def scrape_pds_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            
             # print(context['execution_date'], type(context['execution_date']))
            curr_month = curr_date.strftime('%Y-%m')
            headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,image/apng,*/*;q=0.8"}

            state_name_codes = requests.get(state_codes).json()
            total_data = []
            col_names = ['dfso','total_rice_allocated','total_wheat_allocated','total_qty_allocated',
                        'total_rice_distributed_unautomated','total_wheat_distributed_unautomated','total_qty_distributed_unautomated',
                        'percent_qty_distributed_unautomated','total_rice_distributed_automated','total_wheat_distributed_automated',
                        'total_qty_distributed_automated','percentage_qty_distributed_automated','total_qty_allocated_automated']

            session = requests.Session()
            year_num = int(curr_date.strftime("%Y"))
            month_num = int(curr_date.strftime("%m"))
        #     print(month_num, year_num)

            for state in state_name_codes:
                print(month_num, year_num, state)
                # https://annavitran.nic.in/unautomatedStDetailMonthDFSOWiseRpt?m=1&y=2020&s=12&sn=ARUNACHAL%20PRADESH
                url = requote_uri(f"https://annavitran.nic.in/unautomatedStDetailMonthDFSOWiseRpt?m={month_num}&y={year_num}&s={str(state['id']).zfill(2)}&sn={state['name']}")
                # print(url)
                html_content = session.get(url, headers=headers, verify=False).content
                soup = BeautifulSoup(html_content, 'html.parser')

                try:
                    table_str = str(soup.select('table[id="example"]')[0])
                    # print(table_str)
                    df = pd.read_html(table_str)[0]
                    df.columns = range(df.shape[1])
                    df = df.replace("-", 0)
                    df.drop(0, axis=1, inplace = True)
        #                 display(df)
                    df.columns = col_names
                    df = df.head(-1)
                    df.insert(0, 'state_name', state['name'])
                    df.insert(0, 'state_code', state['id'])
                    df.insert(0, 'month_year',  date(year_num,month_num,1).strftime("%b-%Y"))

                    num_cols = col_names[1:]
                    df[num_cols] = pd.to_numeric(df[num_cols].stack()).unstack()
                    df['total_rice_distributed'] = df['total_rice_distributed_unautomated'] + df['total_rice_distributed_automated'] 
                    df['total_wheat_distributed'] = df['total_wheat_distributed_unautomated'] + df['total_wheat_distributed_automated'] 
        #                 display(df)
        #                 print(df.dtypes)
                    total_data.append(df)
                except IndexError as e:
                    raise ValueError(e)
            session.close()

            master_df = pd.concat(total_data).reset_index(drop=True)
            master_df['state_dfso'] = master_df['state_name'].str.lower().str.strip() + master_df['dfso'].str.lower().str.strip()
            master_df.rename(columns={'state_name':'state_name_data','state_code':'state_code_data'}, inplace=True)

            data_subset = master_df[['state_name_data','dfso','state_dfso']].drop_duplicates()
            dfso_mapping = pd.read_csv(lgd_codes_file)
            dfso_mapping = dfso_mapping[~dfso_mapping['district_code'].isna()].reset_index(drop=True)
            dfso_mapping['state_dfso'] = dfso_mapping['state_name_data'].str.lower().str.strip() + dfso_mapping['dfso'].str.lower().str.strip()
            dfso_mapping = dfso_mapping[['state_dfso','district_code','district_name','state_code','state_name']].drop_duplicates(subset=['state_dfso']).reset_index(drop=True)

            final_codes = data_subset.merge(dfso_mapping, on='state_dfso', how='left')
            final_codes = final_codes[['state_dfso','state_name','state_code','district_name','district_code']]

            merged_df = master_df.merge(final_codes, on='state_dfso', how='left')
            merged_df = merged_df[ ['month_year','state_name','state_code','district_name','district_code','total_rice_allocated','total_wheat_allocated',
                                    'total_rice_distributed','total_wheat_distributed'] ].copy()
            merged_df['state_name'] = merged_df['state_name'].str.title().str.replace(r'\bAnd\b','and').str.replace('\s+', ' ', regex=True).str.strip()
            merged_df['district_name'] = merged_df['district_name'].str.title().str.replace('\s+', ' ', regex=True).str.strip()

            merged_df = merged_df[~merged_df['district_code'].isna()]
            merged_df['district_code'] = merged_df['district_code'].astype('int64')
            merged_df['state_code'] = merged_df['state_code'].astype('int64')

            grouped_df = merged_df.groupby(['month_year','state_name','state_code','district_name','district_code'])[['total_rice_allocated','total_wheat_allocated',
                                                                    'total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()
            grouped_df.columns = grouped_df.columns.tolist()[:5] + [col+"_district" for col in grouped_df.columns.tolist()[5:]]

            state_df = merged_df.groupby(['month_year','state_name','state_code'])[['total_rice_allocated','total_wheat_allocated',
                                                                            'total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()
            state_df.columns = state_df.columns.tolist()[:3] + [col+"_state" for col in state_df.columns.tolist()[3:]]

            india_df = merged_df.groupby(['month_year'])[['total_rice_allocated','total_wheat_allocated',
                                                                            'total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()
            india_df.columns = india_df.columns.tolist()[:1] + [col+"_india" for col in india_df.columns.tolist()[1:]]

            final_df = grouped_df.merge(state_df, on=['month_year','state_name','state_code'], how='left')
            final_df = final_df.merge(india_df, on=['month_year'], how='left')
            final_df.insert(0, 'date', pd.to_datetime(final_df['month_year'], format= "%b-%Y"))
            final_df = final_df.sort_values(by=['date','state_name','district_name'])
            final_df['date'] = final_df['date'].dt.strftime("%d-%m-%Y")
            final_df = final_df.drop(columns='month_year')

            filename = os.path.join(monthly_data_path, f"pds_{curr_date.strftime('%m-%Y')}.csv")
            final_df.to_csv(filename, index=False)
          

            return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

            # else:
                # raise ValueError(f"No data available yet for: : {curr_date.strftime('%m-%Y')}")

        except Exception as e:
            raise ValueError(e)


    scrape_pds_monthly_task = PythonOperator(
        task_id = 'scrape_pds_monthly',
        python_callable = scrape_pds_monthly,
        depends_on_past = True
    )

    

    # Upload data file 
    def upload_pds_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:

            filename = os.path.join(monthly_data_path, f"pds_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"pds_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except Exception as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_pds_monthly_task = PythonOperator(
        task_id = 'upload_pds_monthly',
        python_callable = upload_pds_monthly,
        depends_on_past = True
    )

    scrape_pds_monthly_task >> upload_pds_monthly_task

