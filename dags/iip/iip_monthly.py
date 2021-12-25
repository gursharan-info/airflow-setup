from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os
from lxml import html
from string import digits
import urllib.parse
from bipp.sharepoint.uploads import upload_file  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=15),
}
with DAG(
    'iip_monthly',
    default_args=default_args,
    description='IIP / Industrial Activity Monthly',
    schedule_interval = '0 20 10 * *',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=7, day=2, hour=12, minute=0),
    catchup = True,
    tags=['iip'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'iip')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    SECTOR_NAME = 'Industry and Trade'
    DATASET_NAME = 'iip_monthly'

    def scrape_iip_monthly(ds, **context):  
        '''
        Scrape the monthly IIP data
        '''
        # print(context['execution_date'])
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",context['data_interval_start'])   

        main_url = main_url = "https://www.mospi.gov.in/web/mospi/download-tables-data/-/reports/view/templateFour/23802?q=TBDCAT"
        session = requests.Session()
        response = session.get(main_url)
        tree = html.fromstring(response.content)
        # tree

        curr_link = None
        remote_file_name = None
        for a in tree.xpath('//p[@class="sdg_text"]/a'):
            if 'iip' in a.text.lower():
                print(a.text, a.get("href"))
                curr_link = f"https://www.mospi.gov.in{urllib.parse.quote(a.get('href'))}"
                remote_file_name = [n for n in a.get('href').split('/') if 'xlsx' in n][0].lstrip(digits+'_')
                # Sample link:  http://mospi.nic.in/documents/213904//551441//1639138522507_Indices IIP2011-12 Monthly_annual Oct 21.xlsx//4499b14e-12f2-ce42-bf1b-f7caedf31315
                print(curr_link)
                break
        session.close()

        if curr_link:
            # Fetch and Scrape the raw file
            raw_file_loc = os.path.join(raw_data_path, remote_file_name)
            print(raw_file_loc)
            session = requests.Session()
            resp = session.get(curr_link, allow_redirects=True)
            with open(raw_file_loc, 'wb') as output:
                output.write(resp.content)
            output.close()
            session.close()
            # Upload raw file to Sharepoint
            upload_file(raw_file_loc, f"{DATASET_NAME}/raw_data", remote_file_name, SECTOR_NAME, "india_pulse")

            # Sectoral sheet data
            sector_raw_df = pd.read_excel(raw_file_loc, sheet_name="NIC 2d, sectoral monthly")
            start_idx = sector_raw_df.index[sector_raw_df[sector_raw_df.columns[1]].str.lower().str.contains('description', na=False,case=False)].tolist()[-1]
            last_idx = sector_raw_df.index[sector_raw_df[sector_raw_df.columns[0]].str.lower().str.contains('growth rate', na=False,case=False)].tolist()[-1]

            sector_df = sector_raw_df.iloc[start_idx:last_idx].reset_index(drop=True)
            sector_df.columns = sector_df.iloc[0]
            sector_df = sector_df[sector_df['NIC 2008'].isin(['Mining','Manufacturing','Electricity','General'])].dropna(how='all', axis=1).drop(columns='Weights')
            sector_df.columns = ['sector'] + sector_df.columns.tolist()[1:]

            sector_data_df = sector_df.T.reset_index()
            sector_data_df.columns = sector_data_df.iloc[0]
            sector_data_df = sector_data_df.iloc[1:]
            sector_data_df.columns = ['date'] + ["overall_india" if 'general' in col.lower() else col.lower()+"_india" for col in sector_data_df.columns.tolist()[1:]]
            sector_data_df['date'] = pd.to_datetime(sector_data_df['date']).dt.to_period('M')

            # UBC Monthly data
            ubc_raw_df = pd.read_excel(raw_file_loc, sheet_name="UBC monthly")
            ubc_start_idx = ubc_raw_df.index[ubc_raw_df[ubc_raw_df.columns[0]].str.lower().str.contains('category', na=False,case=False)].tolist()[-1]
            ubc_last_idx = ubc_raw_df.index[ubc_raw_df[ubc_raw_df.columns[0]].str.lower().str.contains('growth rate', na=False,case=False)].tolist()[-1]

            ubc_df = ubc_raw_df.iloc[ubc_start_idx:ubc_last_idx].reset_index(drop=True)
            ubc_df.columns = ubc_df.iloc[0]
            ubc_df = ubc_df[ubc_df['Use-based category'].str.contains('consumer', case=False, na=False)].dropna(how='all', axis=1).drop(columns='Weight')
            ubc_df.columns = ['category'] + ubc_df.columns.tolist()[1:]

            ubc_data_df = ubc_df.T.reset_index()
            ubc_data_df.columns = ubc_data_df.iloc[0]
            ubc_data_df = ubc_data_df.iloc[1:]
            ubc_data_df.columns = ['date'] + [col.lower().replace(' ','_').replace('-','_')+"_india" for col in ubc_data_df.columns.tolist()[1:]]
            ubc_data_df['date'] = pd.to_datetime(ubc_data_df['date']).dt.to_period('M')    

            final_df = pd.merge(sector_data_df, ubc_data_df, on='date', how='left')
            final_df = final_df[final_df['date'] == curr_date.strftime("%Y-%m")].copy()

            if not final_df.empty:
                final_df['date'] = final_df['date'].dt.strftime("01-%m-%Y")
                filename = os.path.join(monthly_data_path, f"iip_{curr_date.strftime('%m-%Y')}.csv")
                final_df.to_csv(filename, index=False)
                
                upload_file(filename, DATASET_NAME, f"iip_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
            else:
                print(f"No Data available for: {curr_date.strftime('%m-%Y')}")
                raise ValueError(f"No Data available for: {curr_date.strftime('%m-%Y')}")

        else:
            print(f"No Data available for: {curr_date.strftime('%m-%Y')}")
            raise ValueError(f"No Data available for: {curr_date.strftime('%m-%Y')}")

        # except requests.exceptions.RequestException as e:
        #     print(e)

    scrape_iip_monthly_task = PythonOperator(
        task_id='scrape_iip_monthly',
        python_callable=scrape_iip_monthly,
        depends_on_past=True
    )


