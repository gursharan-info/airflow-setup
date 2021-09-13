from bs4 import BeautifulSoup
import requests, json, io, re, pendulum, urllib3, os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

# lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_v1_17Oct19.csv'
dir_path = '/usr/local/airflow/data/hfi/fertilizer_sales'
data_path = os.path.join(dir_path, 'daily')
gdrive_fertilizer_folder = '1EZeIWEq_Yshb-C-0E1HBXzBDh5luPsZf'
day_lag = 6
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_fertilizer_data(**context):
    # Load the main page
    try:
        resp = requests.get('https://reports.dbtfert.nic.in/mfmsReports/getPOSReportForm', verify = False)
        # , headers=headers)#, proxies=proxies)
        main_content = resp.content
        main_soup = BeautifulSoup(main_content, 'html.parser') 
        # print(context['execution_date'], type(context['execution_date']))
        
        # The current date would be previous day from date of execution
        currentDate = datetime.fromtimestamp(context['execution_date'].timestamp())- timedelta(day_lag)
        print(currentDate)
        # currentDate = datetime.today()
         
        # Get list of states from the response
        states = [option['value'] for option in main_soup.find("select",{"id":"parameterStateName"}).findAll("option")[1:] ]

        state_codes_df = pd.read_csv(os.path.join(dir_path, 'LGD_v1_17Oct19_fertilizer.csv'))
        state_codes_df.rename(columns={'D_NAME': 'District_lgd', 'D_CODE': 'District_code', 'S_NAME': 'State_lgd', 'S_CODE': 'State_code'}, inplace=True)
        state_codes_df[['District_lgd','State_lgd']] = state_codes_df[['District_lgd','State_lgd']].apply(lambda x: x.str.strip().str.upper())
        state_codes_df['state_district_lower'] = state_codes_df['State_lgd'].str.lower() + "_" + state_codes_df['District_lgd'].str.lower()

        all_data = []
        # Fetch discricts and relevant data for each state
        for state in states:
            dist_url = "https://reports.dbtfert.nic.in/mfmsReports/getDistrictList"
            state_resp = requests.post(url = dist_url, data = {'selectedStateName': state}, verify = False)
               
            districts = list( json.loads(state_resp.text).keys() )
            # print(state, districts)
            for dist in districts:
                f_url = "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action"
                form_data = { "parameterStateName": state, "parameterDistrictName": dist,
                            "parameterFromDate": currentDate.strftime("%d/%m/%Y"),
                            "parameterToDate": currentDate.strftime("%d/%m/%Y")
                            }
                state_dist_resp = requests.post(url = f_url, data = form_data, verify = False) 
                state_dist_resp_cookies = state_dist_resp.cookies
                
                report_url = "https://reports.dbtfert.nic.in/mfmsReports/report.jsp"
                report_resp = requests.post(url = report_url, verify = False, cookies=state_dist_resp_cookies) 

                df = pd.read_csv(io.StringIO(report_resp.content.decode('utf-8')), header=1).reset_index(drop=True).iloc[:-1,:].iloc[:, :-1]
                df = df.drop(df.columns[0],axis=1)
                df.insert(0, 'State_Name', state)
                df.insert(0, 'Date', currentDate.strftime("%d-%m-%Y"))
                all_data.append(df)

        all_state_data = pd.concat(all_data).drop_duplicates()
        all_state_data.columns = [re.sub('[^A-Za-z0-9]+', ' ',col).strip().replace(" ","_") for col in all_state_data.columns]
        all_state_data['state_district_lower'] = all_state_data['State_Name'].str.lower().str.strip() + "_" + \
                                                all_state_data['District_Name'].str.lower().str.strip()

        dist_summary = all_state_data.groupby(['State_Name','District_Name','state_district_lower','Date'],
                                        as_index=False)['Quantity_Sold_MT','No_Of_Sale_Transaction'].sum()

        dist_merged = pd.merge(dist_summary, state_codes_df, on='state_district_lower', how='left')
        dist_merged.rename(columns={'Quantity_Sold_MT': 'Quantity_Sold_daily_district', 
                                'No_Of_Sale_Transaction': 'Number_of_sale_Transactions_daily_district'},
                                inplace=True)

        historical_data = pd.read_csv(os.path.join(dir_path,'data_historical.csv'))
        historical_data.rename(columns={'merge_name': 'state_district_lower'},inplace=True)
        
        stacked_df = pd.concat([historical_data, dist_merged],sort=False).reset_index(drop=True)
        stacked_df['Date'] = pd.to_datetime(stacked_df['Date'], format="%d-%m-%Y").dt.date
        stacked_df.sort_values(by='Date', ascending=False)

        stacked_df[['Quantity_Sold_roll_district',
                'Number_of_sale_Transactions_roll_district']] = stacked_df.groupby('state_district_lower')[['Quantity_Sold_daily_district',
                                                                        'Number_of_sale_Transactions_daily_district']].transform(
                                                                        lambda x: x.rolling(7).mean()
                                                                    )
        state_df = stacked_df.groupby(['State_Name','Date'])[['Quantity_Sold_daily_district',
                                                        'Number_of_sale_Transactions_daily_district']].sum().reset_index()
        state_df.columns = ['State_Name', 'Date', 'Quantity_Sold_daily_state', 'Number_of_sale_Transactions_daily_state']
        state_df[['Quantity_Sold_roll_state', 
            'Number_of_sale_Transactions_roll_state']] = state_df.groupby('State_Name')[['Quantity_Sold_daily_state', 
                                                                                        'Number_of_sale_Transactions_daily_state']
                                                                                    ].transform(lambda x: x.rolling(7).mean())
        
        stacked_df = pd.merge(stacked_df, state_df, on=["State_Name", "Date"], how="left")
        india_df = stacked_df.groupby('Date')[['Quantity_Sold_daily_district', 'Number_of_sale_Transactions_daily_district']].sum().reset_index()
        india_df.columns = ['Date', 'Quantity_Sold_daily_india', 'Number_of_sale_Transactions_daily_india']
        india_df[['Quantity_Sold_roll_india',
            'Number_of_sale_Transactions_roll_india']] = india_df[['Quantity_Sold_daily_india', 
                                                                'Number_of_sale_Transactions_daily_india']].rolling(7).mean()
        stacked_df = pd.merge(stacked_df, india_df, on="Date", how="left")     
        date_to_be_sent = currentDate                                          

        final_df = stacked_df[stacked_df['Date']==date_to_be_sent.date()]
        final_df = final_df[['State_Name', 'District_Name', 'Date', 'District_lgd',
                'District_code', 'State_lgd', 'State_code',
                'Quantity_Sold_daily_district',
                'Number_of_sale_Transactions_daily_district',
                'Quantity_Sold_roll_district',
                'Number_of_sale_Transactions_roll_district',
                'Quantity_Sold_daily_state', 'Number_of_sale_Transactions_daily_state',
                'Quantity_Sold_roll_state', 'Number_of_sale_Transactions_roll_state',
                'Quantity_Sold_daily_india', 'Number_of_sale_Transactions_daily_india',
                'Quantity_Sold_roll_india', 'Number_of_sale_Transactions_roll_india']].reset_index(drop=True)
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        final_df['Date'] = final_df['Date'].dt.strftime("%d-%m-%Y")


        filename = os.path.join(data_path, currentDate.strftime("%d-%m-%Y")+'.csv')
        final_df.to_csv(filename,index=False)
        gupload.upload(filename, currentDate.strftime("%d-%m-%Y")+'.csv',gdrive_fertilizer_folder)
    
    except requests.exceptions.RequestException as e:
        print(e)
        pass


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=8, day=20, hour=12, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

fertilizer_dag = DAG("fertilizerDailyScraping", default_args=default_args, schedule_interval="@daily")

read_fertilizer_data_task = PythonOperator(task_id='read_fertilizer_data',
                                       python_callable=read_fertilizer_data,
                                       dag=fertilizer_dag,
                                       provide_context = True,
                                    )

