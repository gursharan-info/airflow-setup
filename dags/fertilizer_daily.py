from bs4 import BeautifulSoup
import requests, json, io, re, pendulum, urllib3, os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_v2_17Sep21_fertilizer.csv'
dir_path = '/usr/local/airflow/data/hfi/fertilizer_sales'
data_path = os.path.join(dir_path, 'daily')
raw_data_path = os.path.join(dir_path, 'raw_data')
gdrive_fertilizer_folder = '1EZeIWEq_Yshb-C-0E1HBXzBDh5luPsZf'
gdrive_fertilizer_raw_folder = '1zgBIGhN4j0TDI0HdMqlxaTebb0iIgwPo'
gdrive_fert_hist_folder = '1n0tMtKBWtT8MJYfpV2lAHf4MDk5sPO_Y'
day_lag = 2
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_fertilizer_data(**context):
    # Load the main page
    # The current date would be previous day from date of execution
    curr_date = datetime.fromtimestamp(context['execution_date'].timestamp())- timedelta(day_lag)
    print(curr_date)

    try:
        resp = requests.get('https://reports.dbtfert.nic.in/mfmsReports/getPOSReportForm', verify = False)
        main_content = resp.content
        main_soup = BeautifulSoup(main_content, 'html.parser')
        states = [option['value'] for option in main_soup.find("select",{"id":"parameterStateName"}).findAll("option")[1:] ]

        state_dist_list = []
        for state in states:
            dist_url = "https://reports.dbtfert.nic.in/mfmsReports/getDistrictList"
            state_resp = requests.post(url = dist_url, data = {'selectedStateName': state}, verify = False)
            districts = list( json.loads(state_resp.text).keys() )
            # print(state, districts)
            for dist in districts:
                state_dist_list.append(dict(state_name=state, dist_name=dist))

        # state_codes_df = pd.read_csv(os.path.join(dir_path, 'LGD_v2_17Sep21_fertilizer.csv'))
        state_codes_df = pd.read_csv(lgd_codes_file)
        state_codes_df[['district_name','state_name']] = state_codes_df[['district_name','state_name']].apply(lambda x: x.str.strip().str.upper())
        state_codes_df['merge_name'] = state_codes_df['state_fert'].str.strip().str.lower() + "_" + state_codes_df['district_fert'].str.strip().str.lower()
        
        all_data = []
        for state_dist in state_dist_list:
#             print(state_dist)
            f_url = "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action"

            form_data = { "parameterStateName": state_dist['state_name'], "parameterDistrictName": state_dist['dist_name'],
                                "parameterFromDate": curr_date.strftime("%d/%m/%Y"),
                                "parameterToDate": curr_date.strftime("%d/%m/%Y")
                                }        
            state_dist_resp = requests.post(url = f_url, data = form_data, verify = False) 

            state_dist_resp_cookies = state_dist_resp.cookies

            report_url = "https://reports.dbtfert.nic.in/mfmsReports/report.jsp"
            report_resp = requests.post(url = report_url, verify = False, cookies=state_dist_resp_cookies) 

            df = pd.read_csv(io.StringIO(report_resp.content.decode('utf-8')), header=1).reset_index(drop=True).iloc[:-1,:].iloc[:, :-1]
            df = df.drop(df.columns[0],axis=1)
            df.insert(0, 'State_Name', state_dist['state_name'])
            df.insert(0, 'Date', curr_date.strftime("%d-%m-%Y"))

            all_data.append(df)
        
        all_state_data = pd.concat(all_data).drop_duplicates()
        raw_filename = os.path.join(raw_data_path, curr_date.strftime("%Y-%m-%d")+".csv")
        all_state_data.to_csv(raw_filename, index=False)
        gupload.upload(raw_filename, curr_date.strftime("%Y-%m-%d")+".csv",gdrive_fertilizer_raw_folder)

        all_state_data.columns = [re.sub('[^A-Za-z0-9]+', ' ',col).strip().replace(" ","_") for col in all_state_data.columns]
        all_state_data['Date'] = pd.to_datetime(all_state_data['Date'], format="%d-%m-%Y")

        dist_summary = all_state_data.groupby(['State_Name','District_Name','Date'])[['Quantity_Sold_MT','No_Of_Sale_Transaction']].sum().reset_index()
        dist_summary.columns = ['state_fert','district_fert','date','quantity_sold_daily_district','number_of_sale_transactions_daily_district']
        dist_summary['merge_name'] = dist_summary['state_fert'].str.lower().str.strip() + "_" + \
                                                    dist_summary['district_fert'].str.lower().str.strip()
        dist_summary = dist_summary[~dist_summary['district_fert'].str.contains('^\d+$')].copy()
        # display(dist_summary)
        dist_merged = pd.merge(dist_summary, state_codes_df[['state_name','state_code','district_name','district_code','merge_name']], on='merge_name', how='left')
        # display(dist_merged)
    
        hist_df = pd.read_csv(os.path.join(dir_path,'data_historical.csv'))
        hist_df['merge_name'] = hist_df['state_fert'].str.lower().str.strip() + "_" + \
                                                    hist_df['district_fert'].str.lower().str.strip()
        hist_df['date'] = pd.to_datetime(hist_df['date'], format="%d-%m-%Y")
        hist_df = hist_df[(hist_df['date'] < curr_date.strftime("%Y-%m-%d"))]
        # display(hist_df)
        stacked_df = pd.concat([hist_df, dist_merged],sort=False).reset_index(drop=True).drop_duplicates()
        stacked_df = stacked_df.sort_values(by=['date','state_name','district_name'])
    
        final_df = stacked_df.copy()
        stacked_df['date'] = stacked_df['date'].dt.strftime("%d-%m-%Y")
        stacked_df.drop(columns='merge_name').to_csv(os.path.join(dir_path,'data_historical.csv'), index=False)
        gupload.upload(os.path.join(dir_path,'data_historical.csv'), "data_historical.csv",gdrive_fert_hist_folder)
        # display(stacked_df)
    
        final_df[['quantity_sold_roll_district', 
                  'number_of_sale_transactions_roll_district']] = final_df.groupby('merge_name')[['quantity_sold_daily_district', 
                                                            'number_of_sale_transactions_daily_district']].transform(lambda x: x.rolling(window=7,min_periods=1).mean())
        state_df = final_df.groupby(['state_code','date'])[['quantity_sold_daily_district', 'number_of_sale_transactions_daily_district']].sum().reset_index()
        state_df.columns = ['state_code', 'date', 'quantity_sold_daily_state', 'number_of_sale_transactions_daily_state']
        state_df[['quantity_sold_roll_state', 
                'number_of_sale_transactions_roll_state']] = state_df.groupby('state_code')[['quantity_sold_daily_state',
                                                                'number_of_sale_transactions_daily_state']].transform(lambda x: x.rolling(window=7,min_periods=1).mean())
        final_df = pd.merge(final_df, state_df, on=["state_code", "date"], how="left")
    
        india_df = final_df.groupby('date')[['quantity_sold_daily_district', 'number_of_sale_transactions_daily_district']].sum().reset_index()
        india_df.columns = ['date', 'quantity_sold_daily_india', 'number_of_sale_transactions_daily_india']
        india_df[['quantity_sold_roll_india', 'number_of_sale_transactions_roll_india']] = india_df[['quantity_sold_daily_india', 
                                                                                           'number_of_sale_transactions_daily_india']].rolling(window=7,min_periods=1).mean()
    
        final_df = pd.merge(final_df, india_df, on="date", how="left")
        daily_final_df = final_df[(final_df['date'] == curr_date.strftime("%Y-%m-%d"))].reset_index(drop=True)
        daily_final_df['date'] = daily_final_df['date'].dt.strftime("%d-%m-%Y")
        daily_final_df = daily_final_df[['date','state_name','state_code','district_name','district_code','quantity_sold_roll_district','number_of_sale_transactions_roll_district',
                                    'quantity_sold_roll_state','number_of_sale_transactions_roll_state','quantity_sold_roll_india', 'number_of_sale_transactions_roll_india']]
        filename = os.path.join(data_path, f"fertilizer_sales_daily_{curr_date.strftime('%d-%m-%Y')}.csv")
        daily_final_df.to_csv(filename,index=False)
        
        # filename = os.path.join(data_path, currentDate.strftime("%d-%m-%Y")+'.csv')
        # final_df.to_csv(filename,index=False)
        gupload.upload(filename, f"fertilizer_sales_daily_{curr_date.strftime('%d-%m-%Y')}.csv",gdrive_fertilizer_folder)
    
    except requests.exceptions.RequestException as e:
        print(e)
        pass


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=9, day=19, hour=12, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(hours=3),
}

fertilizer_dag = DAG("fertilizerDailyScraping", default_args=default_args, schedule_interval="@daily")

read_fertilizer_data_task = PythonOperator(task_id='read_fertilizer_data',
                                       python_callable=read_fertilizer_data,
                                       dag=fertilizer_dag,
                                       provide_context = True,
                                       depends_on_past=True,
                                       wait_for_downstream=True
                                    )

