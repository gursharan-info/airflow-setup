import pendulum, os, re, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'hfi'), 'digital_payments')
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_digpayments_monthly_folder = '1drBcdY8T4N-CTqDzPKV4bq5tEq1-BGyD'
gdrive_digpayments_raw_folder = '1rxGVgY5FGEj9yyqvIStCaTdu_uGN5dcj'
day_lag = 1
source_file_url = 'https://rbidocs.rbi.org.in/rdocs/content/docs/PSDDP04062020.xlsx'


def digital_payments_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        prev_mnth_date = curr_date
        prev_month_str = prev_mnth_date.strftime('%Y-%m')
        print(curr_date, prev_mnth_date)
        
        excel_file_obj = pd.ExcelFile(source_file_url)
        
        resp = requests.get(source_file_url)
        local_excel_path = os.path.join(raw_path, f"raw_digital_payments_{curr_date.strftime('%m%Y')}.xlsx")
        output = open(local_excel_path, 'wb')
        output.write(resp.content)
        output.close()

        mnth_df_list = []
        for sheet_name in excel_file_obj.sheet_names:
            raw_df = pd.read_excel(source_file_url, sheet_name = sheet_name,
                            skiprows=4, header=[0,1])
            column_names = ["_".join([re.sub(' +',' ',re.sub("[^A-Za-z]", " ",x)).strip().lower().replace(' ','_') 
                                                for x in a]) for a in raw_df.columns.to_flat_index()]
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
            new_df['date'] = pd.to_datetime(new_df['date'])
            
            mnth_df_list.append(new_df)
       
        combined_df = pd.concat(mnth_df_list, sort=False).reset_index(drop=True)
        vol_columns = ['rtgs_vol','neft_vol','aeps_vol','upi_vol','imps_vol','nach_credit_vol',
                    'nach_debit_vol','netc_vol','bbps_vol','cts_vol']
        val_columns = ['rtgs_val','neft_val','aeps_val','upi_val','imps_val','nach_credit_val',
                    'nach_debit_val','netc_val','bbps_val','cts_val']
        combined_df['number_of_payments'] = combined_df[vol_columns].sum(axis=1)
        combined_df['value_of_transactions'] = combined_df[val_columns].sum(axis=1)

        final_df = combined_df.drop(columns=vol_columns+val_columns, axis=1)
        final_df = final_df[['date','number_of_payments','value_of_transactions','aeps_through_micro_atms_bcs_vol',
                            'aeps_through_micro_atms_bcs_val']]
        final_df.columns = ['date','number_of_payments','value_of_transactions','cash_withdrwal_microatms_vol',
                            'cash_withdrwal_microatms_val']
        
        final_df['date'] = final_df['date'].dt.to_period('M')
        monthly_df = final_df.groupby(['date'], sort=False)[['number_of_payments', 'value_of_transactions', 
                                   'cash_withdrwal_microatms_vol','cash_withdrwal_microatms_val'
                                  ]].sum().reset_index()
        monthly_df.columns = ['date']+["cumm_"+col+"_india" for col in monthly_df.columns.tolist()[1:]]
        month_df = monthly_df[monthly_df['date'] == prev_month_str].copy()
        month_df['date'] = month_df['date'].dt.strftime("01-%m-%Y")

        filename = os.path.join(data_path, f"rbi_digpayments_monthly_{prev_mnth_date.strftime('%m%Y')}.csv")
        month_df.to_csv(filename, index=False)

        gupload.upload(filename, f"rbi_digpayments_monthly_{prev_mnth_date.strftime('%m%Y')}.csv", gdrive_digpayments_monthly_folder)
        gupload.upload(local_excel_path, f"raw_digital_payments_{curr_date.strftime('%m%Y')}.xlsx", gdrive_digpayments_raw_folder)

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

digital_payments_monthly_dag = DAG("digitalPaymentsMonthlyScraping", default_args=default_args, schedule_interval='0 20 2 * *')

digital_payments_monthly_task = PythonOperator(task_id='digital_payments_monthly',
                                       python_callable = digital_payments_monthly,
                                       dag = digital_payments_monthly_dag,
                                       provide_context = True,
                                    )