import pendulum, os, requests
import pandas as pd
from datetime import timedelta
from urllib.parse import urlparse, parse_qs, urlencode
from lxml import html

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/iip'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_iip_monthly_folder = '130ggsfdryL4OGJYEpXOVNOwkpSBFecWQ'
gdrive_iip_raw_folder = '1gcAz3kn23SuxzC0qCqCAMC9D0mT9q81I'


def iip_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        print("Scraping for: ",curr_date.strftime("%d-%m-%Y"))
        
        main_url = "http://mospi.nic.in/iip"
        session = requests.Session()
        response = session.get(main_url)
        tree = html.fromstring(response.content)

        curr_link = None
        for a in tree.xpath('//*[@id="MainMenu1_11"]/li/ul/li/a'):
            if 'iip' in a.text.lower():
                print(a.text, a.get("href"))
                curr_link = f"http://mospi.nic.in{a.get('href')}"
                # Sample link:  http://mospi.nic.in/sites/default/files/iipdataaug21.xlsx
                print(curr_link)
        session.close()

        if curr_link:
            # Fetch and Scrape the raw file
            raw_file_loc = os.path.join(raw_path, curr_link.split('/')[-1])
            print(raw_file_loc)
            resp = requests.get(curr_link)
            with open(raw_file_loc, 'wb') as output:
                output.write(resp.content)
            output.close()

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
            final_df = final_df[final_df['date'] == curr_date.strftime("%Y-%m")]

            if not final_df.empty:
                final_df['date'] = final_df['date'].dt.strftime("01-%m-%Y")
                filename = os.path.join(data_path, f"iip_monthly_{curr_date.strftime('%m%Y')}.csv")
                final_df.to_csv(filename, index=False)
                gupload.upload(filename, f"iip_monthly_{curr_date.strftime('%m%Y')}.csv", gdrive_iip_monthly_folder)
            else:
                print("No data available for this month")

        else:
            print('No Data available for this month yet')
    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=6, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

iip_monthly_dag = DAG("industrialActivityScraping", default_args=default_args, schedule_interval='0 20 6 * *')

iip_monthly_task = PythonOperator(task_id='iip_monthly',
                                       python_callable = iip_monthly,
                                       dag = iip_monthly_dag,
                                       provide_context = True,
                                    )