import pendulum, os, requests, json
import pandas as pd
from datetime import timedelta
from urllib.parse import urlparse, parse_qs, urlencode

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/mca_company_reg'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_mca_monthly_folder = '1srfUeLflSJ-VGhv_ZZCgt7CuNjTGeT-q'
gdrive_mca_raw_folder = '1vYO_-g4DAnvw_76kT9_Na04xCB9w-ZEa'


def mca_company_reg_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        curr_year = curr_date.strftime("%Y")
        curr_month = curr_date.strftime("%B")
        print("Scraping for: ",curr_date.strftime("%d-%m-%Y"))
        
        base_url = "https://www.mca.gov.in/bin/dms/searchDocList"
        query_dict = {'page': ['1'], 'perPage': ['20'], 'sortField': ['Date'], 'sortOrder': ['D'], 'searchField': ['Title'], 'dialog': ['{"folder":"403","language":"English","totalColumns":3,"columns":["Title","Month","Year of report"]}']}
        scraping_url = f"{base_url}?{urlencode(query_dict,doseq=True)}"
 
        session = requests.Session()
        resp = session.get(scraping_url)
        data_dict = json.loads(resp.content)
        doc_list = json.loads(data_dict['documentDetails'])

        curr_link = None
        for doc in doc_list:
            if all([curr_year in doc['column1'], curr_month in doc['column1']]):
                curr_link = dict(url=f"https://www.mca.gov.in/bin/dms/getdocument?mds={doc['docID']}&type=open", docType=doc['docType'])

        if curr_link:
            # Read source raw file
            raw_file_loc = os.path.join(raw_path, f"mca_raw_{curr_date.strftime('%m%Y')}.{curr_link['docType']}")
            # raw_file_loc
            resp_xls = requests.get(curr_link['url'])
            with open(raw_file_loc, 'wb') as output:
                output.write(resp_xls.content)
            output.close()
            gupload.upload(raw_file_loc, f"mca_raw_{curr_date.strftime('%m%Y')}.csv", gdrive_mca_raw_folder)

            #Indian companies
            ic = pd.read_excel(raw_file_loc, sheet_name="Indian Companies")
            df1 = ic.groupby(['STATE'])['COMPANY NAME'].count().reset_index()
            df1_tot=ic.groupby(['STATE'])['PAIDUP CAPITAL'].sum().reset_index()
            df1_manu=ic[ic['ACTIVITY DESCRIPTION'].str.contains("Manu",na=False)].groupby(['STATE'])['PAIDUP CAPITAL'].sum().reset_index()
            df1_cons=ic[ic['ACTIVITY DESCRIPTION'].str.contains("Cons",na=False)].groupby(['STATE'])['PAIDUP CAPITAL'].sum().reset_index()

            #LLP
            llp=pd.read_excel(raw_file_loc, sheet_name="LLP Companies")
            df2=llp.groupby(['State'])['LLP Name'].count().reset_index()

            df2_tot=llp.groupby(['State'])['Obligation of Contribution(Rs.)'].sum().reset_index()
            df2_manu=llp[llp['Description'].str.contains("Manu",na=False)].groupby(['State'])['Obligation of Contribution(Rs.)'].sum().reset_index()
            df2_cons=llp[llp['Description'].str.contains("Cons",na=False)].groupby(['State'])['Obligation of Contribution(Rs.)'].sum().reset_index()

            state_codes = pd.read_csv('https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_MCA_reg_10162021.csv')

            df = pd.merge(state_codes, df1, left_on="state_name",right_on='STATE', how="left")
            df = pd.merge(df, df1_tot, on="STATE", how="left")
            df = pd.merge(df, df1_manu, on="STATE", how="left")
            df = pd.merge(df, df1_cons, on="STATE", how="left")
            df = pd.merge(df, df2, left_on="state_name",right_on='State', how="left")
            df = pd.merge(df, df2_tot, on='State', how="left")
            df = pd.merge(df, df2_manu, on='State', how="left")
            df = pd.merge(df, df2_cons, on='State', how="left")

            df=df.fillna(0)
            df['count_state']=df['COMPANY NAME'] + df['LLP Name']
            df['value_tot_state']=df['PAIDUP CAPITAL_x'] + df['Obligation of Contribution(Rs.)_x']
            df['value_manu_state']=df['PAIDUP CAPITAL_y'] + df['Obligation of Contribution(Rs.)_y']
            df['value_constr_state']=df['PAIDUP CAPITAL'] + df['Obligation of Contribution(Rs.)']
            df['date'] = curr_date.strftime("01-%m-%Y")  

            dfi_count=df.groupby(['date'])['count_state'].sum().reset_index()
            dfi_tot=df.groupby(['date'])['value_tot_state'].sum().reset_index()
            dfi_manu=df.groupby(['date'])['value_manu_state'].sum().reset_index()
            dfi_cons=df.groupby(['date'])['value_constr_state'].sum().reset_index()

            df = pd.merge(df, dfi_count, on="date", how="left")
            df = pd.merge(df, dfi_tot, on="date", how="left")
            df = pd.merge(df, dfi_manu, on="date", how="left")
            df = pd.merge(df, dfi_cons, on="date", how="left")

            data=df[['date','state_name','state_code','count_state_x','value_tot_state_x','value_manu_state_x','value_constr_state_x',
                    'count_state_y','value_tot_state_y','value_manu_state_y','value_constr_state_y']]

            data.columns=['date','state_name','state_code','count_state','value_tot_state','value_manu_state','value_constr_state','count_india',
                        'value_tot_india','value_manu_india','value_constr_india']
                        
            filename = os.path.join(data_path, f"mca_company_reg_monthly_{curr_date.strftime('%m%Y')}.csv")
            data.to_csv(filename, index=False)
            gupload.upload(filename, f"mca_company_reg_monthly_{curr_date.strftime('%m%Y')}.csv", gdrive_mca_monthly_folder)

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

mca_company_reg_monthly_dag = DAG("mcaCompanyRegistrationsScraping", default_args=default_args, schedule_interval='0 20 8 * *')

mca_company_reg_monthly_task = PythonOperator(task_id='mca_company_reg_monthly',
                                       python_callable = mca_company_reg_monthly,
                                       dag = mca_company_reg_monthly_dag,
                                       provide_context = True,
                                    )