from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import requests, os
from bipp.sharepoint.uploads import upload_file  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=3),
}
with DAG(
    'fertilizer_monthly',
    default_args=default_args,
    description='Fertilizer Sales Monthly',
    schedule_interval = "0 20 6 * *",
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=11, day=5, hour=12, minute=0),
    catchup = True,
    tags=['agriculture'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'fertilizer_sales')
    data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(data_path, exist_ok = True)

    lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_v2_17Sep21_fertilizer.csv'

    SECTOR_NAME = 'Agriculture'
    DATASET_NAME = 'fertilizer_sales_monthly'


    def process_fertilizer_monthly(ds, **context):  
        '''
        Uses the scraped historical daily raw data and convert it into monthly values
        '''
        # The current date would be derived from the execution date using the lag parameter. 
        # Lag is the delay which the source website has to upload data for that particular date
        curr_date = context['data_interval_start']
        print("Processing for: ",curr_date)

        try:
            hist_df = pd.read_csv(os.path.join(dir_path,'data_historical.csv'))
            hist_df['date'] = pd.to_datetime(hist_df['date'], format='%d-%m-%Y')
            hist_df = hist_df.sort_values(by=['date','state_name','district_name'])

            hist_df['month'] = hist_df['date'].dt.to_period('M')
            df_grouped_dist = hist_df.groupby(['month','state_name','state_code','district_name','district_code']
                                        )[['quantity_sold_daily_district',
                                    'number_of_sale_transactions_daily_district']].sum().reset_index()
            df_grouped_dist.columns = ['month','state_name','state_code','district_name','district_code','quantity_sold_roll_district',
                                'number_of_sale_transactions_roll_district']
            
            df_grouped_state = df_grouped_dist.groupby(['month','state_name','state_code'])[['quantity_sold_roll_district',
                                        'number_of_sale_transactions_roll_district']].sum().reset_index()
            df_grouped_state.columns = ['month','state_name','state_code','quantity_sold_roll_state',
                                        'number_of_sale_transactions_roll_state']

            df_grouped_india = df_grouped_state.groupby(['month'])[['quantity_sold_roll_state',
                                    'number_of_sale_transactions_roll_state']].sum().reset_index()
            df_grouped_india.columns = ['month','quantity_sold_roll_india','number_of_sale_transactions_roll_india']

            df_grouped_final = df_grouped_dist.merge(
                                    df_grouped_state.merge(df_grouped_india,
                                        on=['month'], how='left' 
                                    ), on=['month','state_name','state_code'], how='left')

            df_grouped_final = df_grouped_final[df_grouped_final['month'] == curr_date.strftime("%Y-%m")]
            df_grouped_final.insert(0, 'date', "01-"+df_grouped_final['month'].dt.strftime("%m-%Y"))
            df_grouped_final = df_grouped_final.drop(columns=['month']).reset_index(drop=True)

            filename = os.path.join(data_path, f"fertilizer_sales_{curr_date.strftime('%m-%Y')}.csv")
            df_grouped_final.to_csv(filename,index=False)

            # upload_file(filename, DATASET_NAME, f"fertilizer_sales_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    process_fertilizer_monthly_task = PythonOperator(
        task_id = 'process_fertilizer_monthly',
        python_callable = process_fertilizer_monthly,
        depends_on_past = True
    )

    # Upload data file 
    def upload_fertilizer_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        curr_date = context['data_interval_start']
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            filename = os.path.join(data_path, f"fertilizer_sales_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"fertilizer_sales_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except Exception as e:
            raise ValueError(e)


    upload_fertilizer_monthly_task = PythonOperator(
        task_id = 'upload_fertilizer_monthly',
        python_callable = upload_fertilizer_monthly,
        depends_on_past = True
    )
    
    process_fertilizer_monthly_task >> upload_fertilizer_monthly_task

