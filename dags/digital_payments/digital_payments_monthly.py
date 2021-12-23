from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os, re
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
    'digital_payments_monthly',
    default_args=default_args,
    description='Digital Payments Monthly',
    schedule_interval = '0 20 6 * *',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=12, day=8, hour=12, minute=0),
    catchup = True,
    tags=['digital_payments'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'digital_payments')
    data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    # source_file_url = 'https://rbidocs.rbi.org.in/rdocs/content/docs/PSDDP04062020.xlsx'

    SECTOR_NAME = 'Consumption'
    DATASET_NAME = 'digital_payments_monthly'
    day_lag = 2


    def process_digital_payments_monthly(ds, **context):  
        '''
        Process the scraped daily raw data. 
        '''
        curr_date = context['execution_date']
        print("Scraping for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            local_excel_path = os.path.join(raw_data_path, f"raw_digital_payments_{curr_date.add(months=1).strftime('%m-%Y')}.xlsx")

            excel_file_obj = pd.ExcelFile(local_excel_path)
            sheet_names = [name for name in excel_file_obj.sheet_names if all([curr_date.strftime('%B') in name, curr_date.strftime('%Y') in name])]

            month_df_list = []
            for sheet_name in sheet_names:
                # year_file_path = os.path.join(raw_path, f"raw_cleaned_{sheet_name.capitalize().replace(' ','')}.csv")
                sheet_date = datetime.strptime(sheet_name, "%B %Y")
                print(sheet_date)
                
                raw_df = pd.read_excel(local_excel_path, sheet_name = sheet_name, skiprows=4, header=[0,1], parse_dates=False)
                raw_df.columns = [new_col.strip() for new_col in [col[0] if 'Unnamed' in col[1] else f"{col[0]} {col[1]}" for col in raw_df.columns.tolist() ] ]

                if sheet_date.date() >= datetime(year=2021, month=10, day=1).date():
                    print('October 2021 onwards pattern')
                    raw_df.columns = raw_df.columns+' '+raw_df.iloc[0,:]
                    raw_df = raw_df.iloc[1:,].reset_index(drop=True)
                else:
                    print('Pattern before October 2021')

                column_names = [re.sub(' +',' ',re.sub("[^A-Za-z]", " ",str(a))).strip().lower().replace(' ','_') for a in raw_df.columns.to_list()]
            
                raw_df.columns = ['date'] + column_names[1:]

                split_idx = raw_df.index[raw_df[raw_df.columns[0]].str.lower().str.contains('Note:', na=False,case=False)].tolist()[0]
                total_row_idx = raw_df.index[raw_df[raw_df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()
                total_row_idx = total_row_idx[0] if len(total_row_idx) > 0 else None

                # Drop unnecessary rows
                if total_row_idx:
                    new_df = raw_df.iloc[:total_row_idx].copy().reset_index(drop=True)
                else:
                    new_df = raw_df.iloc[:split_idx-1].copy().reset_index(drop=True)
                new_df = new_df.replace('h', np.nan)
                new_df['date'] = pd.to_datetime(new_df['date']).dt.strftime("%d-%m-%Y")

                month_df_list.append(new_df)

            year_df = pd.concat(month_df_list)
            year_df['date'] = pd.to_datetime(year_df['date'], format="%d-%m-%Y")
            year_df = year_df.sort_values(by='date')
            
            vol_columns = ['rtgs_vol','neft_vol','aeps_vol','upi_vol','imps_vol','nach_credit_vol',
                        'nach_debit_vol','netc_vol','bbps_vol','cts_vol']
            val_columns = ['rtgs_val','neft_val','aeps_val','upi_val','imps_val','nach_credit_val',
                        'nach_debit_val','netc_val','bbps_val','cts_val']
            year_df['number_of_payments'] = year_df[vol_columns].sum(axis=1)
            year_df['value_of_transactions'] = year_df[val_columns].sum(axis=1)

            final_df = year_df.drop(columns=vol_columns+val_columns, axis=1)
            final_df = final_df[['date','number_of_payments','value_of_transactions','aeps_through_micro_atms_bcs_vol',
                                'aeps_through_micro_atms_bcs_val']]
            final_df.columns = ['date','number_of_payments','value_of_transactions','cash_withdrwal_microatms_vol',
                                'cash_withdrwal_microatms_val']
            final_df['date'] = final_df['date'].dt.to_period('M')

            monthly_df = final_df.groupby(['date'], sort=False)[['number_of_payments', 'value_of_transactions', 
                                'cash_withdrwal_microatms_vol','cash_withdrwal_microatms_val'
                                ]].sum().reset_index()
            monthly_df.columns = ['date']+["cumm_"+col+"_india" for col in monthly_df.columns.tolist()[1:]]

            month_df = monthly_df[monthly_df['date'] == curr_date.strftime("%Y-%m")].copy()
            month_df['date'] = month_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(data_path, f"digital_payments_{curr_date.strftime('%m-%Y')}.csv")
            month_df.to_csv(filename, index=False)

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    process_digital_payments_monthly_task = PythonOperator(
        task_id = 'process_digital_payments_monthly',
        python_callable = process_digital_payments_monthly,
        depends_on_past=True
    )

    # Upload data file 
    def upload_digital_payments_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        curr_date = context['execution_date']
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(data_path, f"digital_payments_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"digital_payments_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    upload_digital_payments_monthly_task = PythonOperator(
        task_id = 'upload_digital_payments_monthly',
        python_callable = upload_digital_payments_monthly,
        depends_on_past = True
    )
    
    process_digital_payments_monthly_task >> upload_digital_payments_monthly_task

