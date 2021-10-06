import pendulum, os, re, requests
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/fpi'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_fpi_monthly_folder = '1IQof0mHBzQtiPucmm8sGGXfv2M7sJKZ7'
gdrive_fpi_raw_folder = '1WcgpYAfv3eJovlOOonpoKvRj3_b9UCe4'
day_lag = 1
base_url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_"


def fpi_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        # prev_mnth_date = curr_date.subtract(months=1)
        prev_mnth_date = curr_date
        prev_month_mid = prev_mnth_date.strftime('%B15%Y')
        prev_month_end = (date(curr_date.year, curr_date.month, 1) - relativedelta(days=1)).strftime('%B%d%Y')
        date_list = [prev_month_mid, prev_month_end]
        # print(prev_month_mid, prev_month_end)
        
        headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '3600',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }

        df_list = []

        for dt in date_list:
            full_url = f"{base_url}{dt}.html"
            print(full_url)
            req = requests.get(full_url, headers)
            soup = BeautifulSoup(req.content, 'html.parser')
            
            div=soup.select_one("div#dvFortnightly")
            table_list = pd.read_html(str(div))
            if len(table_list) > 0:
                table = table_list[0]
                
                raw_filename = os.path.join(raw_path, f"{datetime.strptime(dt, '%B%d%Y').strftime('%d-%m-%Y')}.csv")
                table.to_csv(raw_filename, index=False)
                gupload.upload(raw_filename, f"raw_{datetime.strptime(dt, '%B%d%Y').strftime('%d-%m-%Y')}.csv", gdrive_fpi_raw_folder)

            table = table.iloc[2:]
            table.columns = table.iloc[0]

            cols = []
            count = 1
            for column in table.columns:
                if column == 'Total':
                    cols.append(f'Total_{count}')
                    count+=1
                    continue
                cols.append(column)
            table.columns = cols

            #     display(table)
            filtered = table[['Sectors','Total_5' ]]
            filtered.columns = ['Sectors','Total']
            #     display(filtered)
            FPI = pd.read_csv(os.path.join(dir_path, 'FPI_FII_categories.csv'))[['Sectors', 'Broad Sector for JSI']]
            # FPI
            merged_df = pd.merge(filtered, FPI, on='Sectors')[['Broad Sector for JSI','Total']]
            merged_df = merged_df.sort_values(by=['Broad Sector for JSI'])
            merged_df['Total'] = pd.to_numeric(merged_df['Total'])
            #     merged_df = merged_df.dropna(subset=['Broad Sector for JSI'])
            #     display(merged_df)
            grouped_df = merged_df.groupby("Broad Sector for JSI", sort=True)['Total'].sum().reset_index()\
                            .rename(columns={'Broad Sector for JSI':'date','Total': datetime.strptime(dt, '%B%d%Y').strftime("%d-%m-%Y")})
            grouped_df = grouped_df.T.reset_index()
            grouped_df.columns = [col.replace('& ','').lower().replace(' ','_') for col in grouped_df.iloc[0].values]
            grouped_df = grouped_df[1:]
            
            df_list.append(grouped_df)
        
        month_df = pd.concat(df_list).reset_index(drop=True)
        month_df['date'] = pd.to_datetime(month_df['date'], format="%d-%m-%Y")
        month_df['date'] = month_df['date'].dt.to_period('M')
        group_df =  month_df.groupby(['date'], sort=False).sum().reset_index()
        group_df['date'] = group_df['date'].dt.strftime("01-%m-%Y")

        filename = os.path.join(data_path, f"fpi_monthly_{prev_mnth_date.strftime('%m%Y')}.csv")
        group_df.to_csv(filename, index=False)

        gupload.upload(filename, f"fpi_monthly_{prev_mnth_date.strftime('%m%Y')}.csv", gdrive_fpi_monthly_folder)


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

fpi_monthly_dag = DAG("fpiMonthlyScraping", default_args=default_args, schedule_interval='0 20 3 * *')

fpi_monthly_task = PythonOperator(task_id='fpi_monthly',
                                       python_callable = fpi_monthly,
                                       dag = fpi_monthly_dag,
                                       provide_context = True,
                                    )