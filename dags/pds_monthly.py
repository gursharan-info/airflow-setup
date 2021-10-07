import pendulum, os, re, requests
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from requests.utils import requote_uri
from urllib import request
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

state_codes = "https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/pds_state_name_codes.json"
lgd_codes_file = "https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_pds_monthly_05102021.csv"
dir_path = r'/usr/local/airflow/data/hfi/pds'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_pds_monthly_folder = '1g39AxRBt3MsUU_kvoye-8IfhHN1CB44R'


def pds_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        prev_mnth_date =  context['execution_date']
        prev_month = prev_mnth_date.strftime('%Y-%m')
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,image/apng,*/*;q=0.8"}

        state_name_codes = requests.get(state_codes).json()
        total_data = []
        col_names = ['dfso','total_rice_allocated','total_wheat_allocated','total_qty_allocated',
                      'total_rice_distributed_unautomated','total_wheat_distributed_unautomated','total_qty_distributed_unautomated',
                      'percent_qty_distributed_unautomated','total_rice_distributed_automated','total_wheat_distributed_automated',
                      'total_qty_distributed_automated','percentage_qty_distributed_automated','total_qty_allocated_automated']

        session = requests.Session()
        year_num = int(prev_mnth_date.strftime("%Y"))
        month_num = int(prev_mnth_date.strftime("%m"))
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
            except IndexError:
                pass
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

        filename = os.path.join(data_path, f"pds_monthly_{prev_mnth_date.strftime('%m%Y')}.csv")
        final_df.to_csv(filename, index=False)

        gupload.upload(filename, f"pds_monthly_{prev_mnth_date.strftime('%m%Y')}.csv", gdrive_pds_monthly_folder)


    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=9, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

pds_monthly_dag = DAG("pdsMonthlyScraping", default_args=default_args, schedule_interval='0 20 3 * *')

pds_monthly_task = PythonOperator(task_id='pds_monthly',
                                       python_callable = pds_monthly,
                                       dag = pds_monthly_dag,
                                       provide_context = True,
                                    )