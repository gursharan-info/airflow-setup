import pendulum, os, re, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, camelot


from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


dir_path = r'/usr/local/airflow/data/hfi/formal_emp'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
gdrive_formal_emp_monthly_folder = '1EOY6NdWyUMruy3KkxC8mztC0NVeGwKKA'
gdrive_formal_emp_raw_folder = '1BTRowwRXPSnWajvqI6JrE-g6wsFdRx-3'


def get_df(df_list):
    for df in df_list:
        columns = [col.lower() for col in df.columns.tolist()]
        print(columns)
        if 'amount of fdi equity inflows' in columns:
            return df.copy()    
    return None
    

def formal_emp_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        #  main_url = "https://www.mospi.gov.in/press-release"
        query_url = "https://www.mospi.gov.in/press-release?p_p_id=com_niip_mospi_pressRelease_PressReleaseModulePortlet&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=fetchViewPressRelease&p_p_cacheability=cacheLevelPage&draw=1&columns%5B0%5D%5Bdata%5D=start&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=title&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=releaseDate&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=start&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&order%5B1%5D%5Bcolumn%5D=1&order%5B1%5D%5Bdir%5D=asc&start=0&length=50&search=&division=&category=&subcategory=&archieve=&sortby=&fromDate=&toDate="

        session = requests.Session()
        resp = session.get(query_url)
        data_dict = json.loads(resp.content)
        
        payroll_links = []

        for record in data_dict['content']:
            if 'payroll' in record['title'].lower():
                print(record['title'])
        #         print(record)
                link_dict = dict( title = record['title'], url = f"https://www.mospi.gov.in{record['url']}")
                print(link_dict)
                payroll_links.append(link_dict)
        
        curr_link = [link['url'] for link in payroll_links if all([ curr_date.strftime("%b") in link['title'], curr_date.strftime("%Y") in link['title'] ])][0]
        
        if curr_link:
            pdf_file_name = os.path.join(raw_path, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%B%Y')}.pdf")
            if not os.path.exists(pdf_file_name):
                r = session.get(curr_link, stream = True)
                with open(pdf_file_name,"wb") as pdf:              
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            pdf.write(chunk)
                gupload.upload(pdf_file_name, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%B%Y')}.pdf", gdrive_formal_emp_raw_folder)

            tables = camelot.read_pdf(os.path.join(raw_path, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%B%Y')}.pdf"), pages='all', flavor='lattice')

            for table in tables:
                df = table.df.dropna(how='all', axis=1).copy()
                df = df.replace(r'\r', ' ', regex=True).replace(r'\n', ' ', regex=True).replace('-', '', regex=True).replace('\s+', ' ', regex=True).replace('', np.nan)
                date_idx = df.index[df[df.columns[0]].str.lower().str.contains(curr_date.strftime("%B"), na=False,case=False)].tolist()
                if len(date_idx) > 0:
                    df = df.iloc[date_idx[0]:].reset_index(drop=True)
                    epf_idx = df.index[df[df.columns[1]].str.lower().str.contains('new EPF subscriber', na=False,case=False)].tolist()
                    
                    if epf_idx:
                        total_idx = df.index[df[df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()
                        if total_idx:
                            epf_df = df.iloc[total_idx[0]:,1:4].copy().reset_index(drop=True).fillna(0)
                            epf_df.columns = ['epfo_male', 'epfo_female','epfo_others']
                            epf_df = epf_df.replace(',', '', regex=True)
                            epf_df[epf_df.columns.tolist()] = epf_df[epf_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                    
                    esic_idx = df.index[df[df.columns[5]].str.lower().str.contains('Number of newly registered', na=False,case=False)].tolist()
                    if esic_idx:
                        esic_tot_idx = df.index[df[df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()
                        if esic_tot_idx:
                            esic_df = df.iloc[total_idx[0]:,5:8].copy().reset_index(drop=True).fillna(0)
                            esic_df.columns = ['esic_male', 'esic_female','esic_others']
                            esic_df = esic_df.replace(',', '', regex=True)
                            esic_df[esic_df.columns.tolist()] = esic_df[esic_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                    
                    nps_idx = df.index[df[df.columns[1]].str.lower().str.contains('existing subscriber', na=False,case=False)].tolist()
                    if nps_idx:
                        nps_tot_idx = df.index[df[df.columns[0]].str.lower().str.contains('total', na=False,case=False)].tolist()
                        if nps_tot_idx:
                            nps_df = df.iloc[total_idx[0]+1:,2:-1].copy().reset_index(drop=True).fillna(0)
                            nps_df.columns = ['nps_central_male','nps_central_female','nps_central_trans','nps_central_nonira','nps_central_total',
                                            'nps_state_male','nps_state_female', 'nps_state_trans','nps_state_nonira','nps_state_total',
                                            'nps_nongov_male','nps_nongov_female','nps_nongov_trans','nps_nongov_nonira','nps_nongov_total']
                            nps_df.rename(columns={'nps_central_trans': 'nps_central_others','nps_state_trans': 'nps_state_others','nps_nongov_trans': 'nps_nongov_others'}, inplace=True)
                            nps_df = nps_df.replace(',', '', regex=True).drop(columns=['nps_central_nonira','nps_central_total','nps_state_nonira','nps_state_total','nps_nongov_nonira','nps_nongov_total'])
                            nps_df[nps_df.columns.tolist()] = nps_df[nps_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                    
                    
            merged = pd.concat([epf_df, esic_df,nps_df], axis=1)

            final_df = pd.DataFrame({'date':[curr_date.strftime('01-%m-%Y')]})
            final_df["all_male_india"] = merged["epfo_male"] + merged["esic_male"] + merged["nps_central_male"] + merged["nps_nongov_male"]
            final_df["all_female_india"] = merged["epfo_female"] + merged["esic_female"] + merged["nps_central_female"] + merged["nps_nongov_female"]
            final_df["all_others_india"] = merged["epfo_others"] + merged["esic_others"] + merged["nps_central_others"] + merged["nps_state_others"] + merged["nps_nongov_others"]
            final_df["total_india"] = final_df["all_male_india"] + final_df["all_female_india"] + final_df["all_others_india"]
            print(final_df)

            filename = os.path.join(data_path, f"formal_employment_monthly_{curr_date.strftime('%m%Y')}.csv")
            final_df.to_csv(filename, index=False)

            gupload.upload(filename, f"formal_employment_monthly_{curr_date.strftime('%m%Y')}.csv", gdrive_formal_emp_monthly_folder)

        else:
            print(f"No data available yet for {curr_date.strftime('%b%Y')}")
        
        session.close()

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

formal_emp_monthly_dag = DAG("formalEmpMonthlyScraping", default_args=default_args, schedule_interval='0 20 15 * *')

formal_emp_monthly_task = PythonOperator(task_id='formal_emp_monthly',
                                       python_callable = formal_emp_monthly,
                                       dag = formal_emp_monthly_dag,
                                       provide_context = True,
                                    )