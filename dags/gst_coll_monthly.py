import pendulum,  os, requests
from lxml import html
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

lgd_codes_file = 'https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_gst_coll_monthly.csv'
dir_path = '/usr/local/airflow/data/hfi/gst_coll_monthly'
raw_data_path = os.path.join(dir_path, 'raw_data')
gdrive_gst_coll_monthly_folder = '1J9gYPEelsNxX9AJU4xCrBojT90leaNuX'
gdrive_gst_coll_raw_folder = '1sBUFyFFNEXyOV_PwisIF0pm07eB2DtO5'


def gst_coll_monthly(**context):
    # Load the main page
    try:
        print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        # prev_mnth_date = context['execution_date'].subtract(months=1)
        prev_month = context['execution_date']
        prev_month_str = prev_month.strftime('%Y-%m')

        main_url = "https://www.gst.gov.in/download/gststatistics"

        session = requests.Session()
        response = session.get(main_url)
        tree = html.fromstring(response.content)
        # tree
        all_links = ["https:"+a for a in tree.xpath('//*[@class="hasdownload"]/a/@href')]
        xls_links = [link for link in [i for i in all_links if '.xlsx' in i] if 'Tax-Collection' in link]

        curr_link = [j for j in xls_links if prev_month.strftime('-%Y-') in j][0]

        file_loc = os.path.join(raw_data_path, curr_link.split("/")[-1])
        resp = requests.get(curr_link)
        with open(file_loc, 'wb') as output:
            output.write(resp.content)
        output.close()
        gupload.upload(file_loc, curr_link.split("/")[-1],gdrive_gst_coll_raw_folder)

        raw_df = pd.read_excel(curr_link)
        start_idx = raw_df.index[raw_df[raw_df.columns[0]].str.lower().str.contains('state', na=False,case=False)].tolist()[0]
        end_idx = raw_df.index[raw_df[raw_df.columns[0]].str.lower().str.contains('Grand Total', na=False,case=False)].tolist()[0]
        raw_df = raw_df.iloc[start_idx:end_idx-2].reset_index(drop=True)

        raw_df.loc[[0,1]] = raw_df.loc[[0,1]].ffill(axis=1)
        raw_df = raw_df.set_index('Unnamed: 1')
        raw_df.columns = pd.MultiIndex.from_arrays([raw_df.iloc[0,:],raw_df.iloc[1,:]], names = ['date', 'gst'])

        df = raw_df.drop(raw_df.index[[0,1]])
        df = df.stack('date').reset_index()
        df['date'] = df['date'].dt.to_period('M')
        df = df.reset_index(drop=True)
        df.columns = ['state','date'] + [col.lower()+"_state" for col in df.columns.tolist()[2:]]

        test_df = df[df['date'] == prev_month.strftime("%Y-%m")].copy()

        if test_df.empty: # Check that is empty, which means data for date is not available
            raise Exception(f"No data available for {prev_month} yet")
        else:
            st_codes= pd.read_csv(lgd_codes_file)
            merged_df = pd.merge(df, st_codes, on='state', how='left')
            merged_df = merged_df[['date','state_name','state_code'] + df.columns.tolist()[2:]]
            merged_df["total_revenue_state"] = merged_df["cgst_state"] + merged_df["sgst_state"] + merged_df["igst_state"] + merged_df["cess_state"]

            india_df = merged_df.groupby(['date'])[merged_df.columns.tolist()[3:]].agg('sum').reset_index()
            india_df.columns = ['date'] + [col.rsplit("_",1)[0]+"_india" for col in india_df.columns.tolist()[1:]]

            final_df = pd.merge(merged_df,india_df,on='date',how='left').sort_values(by=['date','state_name']).reset_index(drop=True)
            final_df = final_df[final_df['date'] == prev_month.strftime("%Y-%m")].reset_index(drop=True)

            final_df['date'] = final_df['date'].dt.strftime("01-%m-%Y")

            filename = os.path.join(dir_path, 'gst_collections_monthly_'+prev_month.strftime('%m%Y')+'.csv')
            final_df.to_csv(filename,index=False)
            gupload.upload(filename, 'gst_collections_monthly_'+prev_month.strftime('%m%Y')+'.csv',gdrive_gst_coll_monthly_folder)

    # except requests.exceptions.RequestException as e:
    #     print(e)
    #     pass
    except Exception as e:
        print(e)
        pass

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=3, day=6, hour=00, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

gst_coll_monthly_dag = DAG("gstCollMonthly", default_args=default_args, schedule_interval='0 20 10 * *')

gst_coll_monthly_task = PythonOperator(task_id='gst_coll_monthly',
                                       python_callable = gst_coll_monthly,
                                       dag = gst_coll_monthly_dag,
                                       provide_context = True,
                                    )