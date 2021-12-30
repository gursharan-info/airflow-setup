from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os
from bs4 import BeautifulSoup

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
    'fii_monthly',
    default_args=default_args,
    description='FPI/FII Monthly',
    schedule_interval = '0 20 10 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['industry_and_trade'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'fii')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    

    SECTOR_NAME = 'Industry and Trade'
    DATASET_NAME = 'fii_monthly'


    def scrape_fii_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)
        
        prev_month_mid = dict(long = curr_date.strftime('%B15%Y'), short = curr_date.strftime('%b15%Y'))
        prev_month_end = dict(long = (date(curr_date.year + int(curr_date.month/12), curr_date.month%12+1, 1)-timedelta(days=1)).strftime('%B%d%Y'),
                            short = (date(curr_date.year + int(curr_date.month/12), curr_date.month%12+1, 1)-timedelta(days=1)).strftime('%b%d%Y') )
        date_list = [prev_month_mid, prev_month_end]

        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }

        base_url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_"
        categories_mapping_url = "https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/FPI_FII_categories.csv"

        df_list = []

        try:
            for dt in date_list:
                try:
                    full_url = f"{base_url}{dt['long']}.html"
                    print(full_url)
                    req = requests.get(full_url, headers)
                    soup = BeautifulSoup(req.content, 'html.parser')
                
                    div=soup.select_one("div#dvFortnightly")
                    table_list = pd.read_html(str(div))
                except Exception as e:
                    full_url = f"{base_url}{dt['short']}.html"
                    print(e, full_url)
                    req = requests.get(full_url, headers)
                    soup = BeautifulSoup(req.content, 'html.parser')
                
                    div=soup.select_one("div#dvFortnightly")
                    table_list = pd.read_html(str(div))
                    
                # print(table_list)
                if len(table_list) > 0:
                    table = table_list[0]
                    table.to_csv(os.path.join(raw_data_path, f"{datetime.strptime(dt['long'], '%B%d%Y').strftime('%d-%m-%Y')}.csv"), index=False)
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
                FPI = pd.read_csv(categories_mapping_url)[['Sectors', 'Broad Sector for JSI']]
                # FPI
                merged_df = pd.merge(filtered, FPI, on='Sectors')[['Broad Sector for JSI','Total']]
                merged_df = merged_df.sort_values(by=['Broad Sector for JSI'])
                merged_df['Total'] = pd.to_numeric(merged_df['Total'])
                #     merged_df = merged_df.dropna(subset=['Broad Sector for JSI'])
                #     display(merged_df)
                grouped_df = merged_df.groupby("Broad Sector for JSI", sort=True)['Total'].sum().reset_index()\
                                .rename(columns={'Broad Sector for JSI':'date','Total': datetime.strptime(dt['long'], '%B%d%Y').strftime("%d-%m-%Y")})
                grouped_df = grouped_df.T.reset_index()
                grouped_df.columns = [col.replace('& ','').lower().replace(' ','_') for col in grouped_df.iloc[0].values]
                grouped_df = grouped_df[1:]
                
                df_list.append(grouped_df)

            month_df = pd.concat(df_list).reset_index(drop=True)
            month_df['date'] = pd.to_datetime(month_df['date'], format="%d-%m-%Y")
            month_df['date'] = month_df['date'].dt.to_period('M')

            group_df =  month_df.groupby(['date'], sort=False).sum().reset_index()
            group_df['date'] = group_df['date'].dt.strftime("01-%m-%Y")
            group_df.columns = ['date'] + [col+"_india" for col in group_df.columns.tolist()[1:]]

            filename = os.path.join(monthly_data_path, f"fii_fpi_{curr_date.strftime('%m-%Y')}.csv")
            group_df.to_csv(filename, index=False)

        except Exception as e:
            raise ValueError(e)


    scrape_fii_monthly_task = PythonOperator(
        task_id = 'scrape_fii_monthly',
        python_callable = scrape_fii_monthly,
        depends_on_past = True
    )

    

    # Upload data file 
    def upload_fii_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(monthly_data_path, f"fii_fpi_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"fii_fpi_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except Exception as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_fii_monthly_task = PythonOperator(
        task_id = 'upload_fii_monthly',
        python_callable = upload_fii_monthly,
        depends_on_past = True
    )

    scrape_fii_monthly_task >> upload_fii_monthly_task

