from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os, re, json, urllib
from urllib.parse import urlencode

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
    'company_registrations_monthly',
    default_args=default_args,
    description='Company Registrations Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['industry_and_trade'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'mca')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    base_url = "https://www.mca.gov.in/bin/dms/searchDocList"
    query_dict = {'page': ['1'], 'perPage': ['20'], 'sortField': ['Date'], 'sortOrder': ['D'], 'searchField': ['Title'], 'dialog': ['{"folder":"403","language":"English","totalColumns":3,"columns":["Title","Month","Year of report"]}']}
    scraping_url = f"{base_url}?{urlencode(query_dict,doseq=True)}"

    SECTOR_NAME = 'Industry and Trade'
    DATASET_NAME = 'company_registrations_monthly'


    def scrape_company_registrations_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        curr_year = curr_date.strftime("%Y")
        curr_month = curr_date.strftime("%B")

        try:
            session = requests.Session()
            resp = session.get(scraping_url)
            data_dict = json.loads(resp.content)
            doc_list = json.loads(data_dict['documentDetails'])
            session.close()

            curr_link = None

            for doc in doc_list:
                if all([curr_year in doc['column1'], curr_month in doc['column1']]):
                    curr_link = dict(url=f"https://www.mca.gov.in/bin/dms/getdocument?mds={doc['docID']}&type=download", docType=doc['docType'])
                    print(curr_link)
            
            if curr_link:
                raw_filename = os.path.join(raw_data_path, f"mca_raw_{curr_date.strftime('%m-%Y')}.xlsx")
                print(raw_filename)
                
                urllib.request.urlretrieve(curr_link['url'], raw_filename)
                resp_xls = requests.get(curr_link['url'],allow_redirects=True)
                with open(raw_filename, 'wb') as output:
                    output.write(resp_xls.content)
                    output.close()

                return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

            else:
                raise ValueError(f"No data available yet for: : {curr_date.strftime('%m-%Y')}")

        except requests.exceptions.RequestException as e:
            raise ValueError(e)


    scrape_company_registrations_monthly_task = PythonOperator(
        task_id = 'scrape_company_registrations_monthly',
        python_callable = scrape_company_registrations_monthly,
        depends_on_past = True
    )


    def process_company_registrations_monthly(ds, **context):  
        '''
        Process the scraped monthly raw data. 
        '''
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            raw_filename = os.path.join(raw_data_path, f"mca_raw_{curr_date.strftime('%m-%Y')}.xlsx")
            if os.path.exists(raw_filename):
                
                ic = pd.read_excel(raw_filename, sheet_name="Indian Companies")
                ic.columns = [col.lower().strip().replace(' ','_') for col in ic.columns.tolist()] 
                ic = ic[['month_name','state','paidup_capital','activity_description']].copy()
                ic['month_name'] = ic['month_name'].dt.to_period('M')
                # ic
                ic_grouped = ic.groupby(['month_name','state']).agg(value_tot_state=('paidup_capital', 'sum'), count_state=('paidup_capital', 'count')).reset_index()

                ic_grp_manu = ic[ic['activity_description'].str.contains("Manu",na=False)].groupby(['month_name','state'])['paidup_capital'].sum().reset_index(name='value_manu_state')
                ic_grp_constr = ic[ic['activity_description'].str.contains("Cons",na=False)].groupby(['month_name','state'])['paidup_capital'].sum().reset_index(name='value_constr_state')
                ic_grouped = ic_grouped.merge(ic_grp_manu, on=['month_name','state'], how='left')
                ic_grouped = ic_grouped.merge(ic_grp_constr, on=['month_name','state'], how='left')

                ##LLP companies
                llp = pd.read_excel(raw_filename, sheet_name="LLP Companies")
                llp.columns = [re.sub(' +', '_', col.lower().strip() ) for col in llp.columns.tolist()] 
                llp['month_name'] = llp['founded'].dt.to_period('M')
                llp = llp[['month_name','state','obligation_of_contribution(rs.)','description']].copy()

                llp_grouped = llp.groupby(['month_name','state']).agg(value_tot_state=('obligation_of_contribution(rs.)', 'sum'), count_state=('obligation_of_contribution(rs.)', 'count')).reset_index()
                llp_grp_manu = llp[llp['description'].str.contains("Manu",na=False)].groupby(['month_name','state'])['obligation_of_contribution(rs.)'].sum().reset_index(name='value_manu_state')
                llp_grp_constr = llp[llp['description'].str.contains("Cons",na=False)].groupby(['month_name','state'])['obligation_of_contribution(rs.)'].sum().reset_index(name='value_constr_state')
                llp_grouped = ( llp_grouped.merge(llp_grp_manu, on=['month_name','state'], how='left') ).merge(llp_grp_constr, on=['month_name','state'], how='left')
                
                #LGD Codes
                state_codes = pd.read_csv('https://raw.githubusercontent.com/gursharan-info/lgd-mappings/master/csv/LGD_MCA_reg_10162021.csv')

                total_grouped = pd.concat([ic_grouped, llp_grouped]).reset_index(drop=True)
                total_grouped = pd.merge(state_codes, total_grouped, left_on="STATE",right_on='state', how="right").drop(columns=['STATE','state'])
                total_grouped = total_grouped.groupby(['month_name','state_name','state_code'])['value_tot_state','count_state','value_manu_state','value_constr_state'].sum().reset_index()
                total_grouped['state_code'] = total_grouped['state_code'].astype('int')

                india_df = total_grouped.groupby(['month_name'])[['value_tot_state','count_state','value_manu_state','value_constr_state']].sum().reset_index()
                india_df.columns = [col.replace('_state','_india') for col in india_df.columns.tolist()]

                final_df = pd.merge(total_grouped, india_df, on='month_name', how='left')
                final_df.insert(0, 'date', final_df['month_name'].dt.strftime("01-%m-%Y"))
                final_df = final_df.drop(columns=['month_name'])

                final_df.to_csv(os.path.join(monthly_data_path, f"company_registrations_{curr_date.strftime('%m-%Y')}.csv"), index=False)

            else:
                raise ValueError(f"No raw file downloaded yet for: : {curr_date.strftime('%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            raise ValueError(e)


    process_company_registrations_monthly_task = PythonOperator(
        task_id = 'process_company_registrations_monthly',
        python_callable = process_company_registrations_monthly,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_company_registrations_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            raw_filename = os.path.join(raw_data_path, f"mca_raw_{curr_date.strftime('%m-%Y')}.xlsx")
            upload_file(raw_filename, f"{DATASET_NAME}/raw_data", f"mca_raw_{curr_date.strftime('%m-%Y')}.xlsx", SECTOR_NAME, "india_pulse")

            filename = os.path.join(monthly_data_path, f"company_registrations_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"company_registrations_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except requests.exceptions.RequestException as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_company_registrations_monthly_task = PythonOperator(
        task_id = 'upload_company_registrations_monthly',
        python_callable = upload_company_registrations_monthly,
        depends_on_past = True
    )

    scrape_company_registrations_monthly_task >> process_company_registrations_monthly_task >> upload_company_registrations_monthly_task

