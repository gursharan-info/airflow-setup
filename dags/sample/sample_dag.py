from airflow import DAG
from airflow.operators.python import PythonOperator
import camelot.io as camelot
import os 
import pandas as pd 

from bipp.sharepoint.uploads import upload_file  



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['snehal_bhartiya@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=1),
}
with DAG(
    'sample_monthly',
    default_args=default_args,
    description='sample_monthly_monthly',
    schedule_interval = '@monthly',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=12, day=18, hour=12, minute=0),
    catchup = True,
    tags=['sample_monthly'],

) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'sample')
    data_path = os.path.join(dir_path, 'daily')
    os.makedirs(data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    # finding the page number using the the function fnPDF_FindText 
    
    def sample_processing(ds,**context):
        try:
            nme = ["aparna", "pankaj", "sudhir", "Geeku"]
            deg = ["MBA", "BCA", "M.Tech", "MBA"]
            scr = [90, 40, 80, 98]

            # dictionary of lists 
            dict = {'name': nme, 'degree': deg, 'score': scr} 

            df = pd.DataFrame(dict)

            # saving the dataframe
            return df

        except Exception as e: 
            return ValueError(e)

    sample_processing_task=PythonOperator(
        task_id='sample_processing',
        python_callable=sample_processing,
        depends_on_past=True)

    def saving_data(ds,**context):
        df.to_csv(os.path.join(dir_path,'sample_monthly.csv'),index=False)


    saving_data_task=PythonOperator(
        task_id='saving_data_task',
        python_callable=saving_data,
        depends_on_past=True
    )

#setting task dependencies

    sample_processing_task>>saving_data_task