from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os
import json, camelot
from bipp.sharepoint.uploads import upload_file



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(days=10),
}
with DAG(
    'payroll_reporting_monthly',
    default_args=default_args,
    description='Formal Employment Monthly',
    schedule_interval = '0 20 4 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['employment'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'formal_employment')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    query_url = "https://www.mospi.gov.in/press-release?p_p_id=com_niip_mospi_pressRelease_PressReleaseModulePortlet&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=fetchViewPressRelease&p_p_cacheability=cacheLevelPage&draw=1&columns%5B0%5D%5Bdata%5D=start&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=title&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=releaseDate&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=start&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&order%5B1%5D%5Bcolumn%5D=1&order%5B1%5D%5Bdir%5D=asc&start=0&length=50&search=&division=&category=&subcategory=&archieve=&sortby=&fromDate=&toDate="

    SECTOR_NAME = 'Employment'
    DATASET_NAME = 'formal_employment_monthly'


    def scrape_formal_employment_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            session = requests.Session()
            resp = session.get(query_url)
            data_dict = json.loads(resp.content)
            session.close()

            payroll_links = []
            curr_link = None

            for record in data_dict['content']:
                if 'payroll' in record['title'].lower():
                    print(record['title'])
            #         print(record)
                    link_dict = dict( title = record['title'], url = f"https://www.mospi.gov.in{record['url']}")
                    print(link_dict)
                    payroll_links.append(link_dict)
            
            curr_link = [link['url'] for link in payroll_links if all([ curr_date.strftime("%b") in link['title'], curr_date.strftime("%Y") in link['title'] ])]
            curr_link = curr_link[0] if len(curr_link) else None
            
            if curr_link:
                pdf_file_name = os.path.join(raw_data_path, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%m-%Y')}.pdf")

                r = session.get(curr_link, stream = True)
                with open(pdf_file_name,"wb") as pdf:              
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            pdf.write(chunk)
                    # gupload.upload(pdf_file_name, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%B%Y')}.pdf", gdrive_formal_emp_raw_folder)


            return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            raise ValueError(e)


    scrape_formal_employment_monthly_task = PythonOperator(
        task_id = 'scrape_formal_employment_monthly',
        python_callable = scrape_formal_employment_monthly,
        depends_on_past = True
    )


    def process_formal_employment_monthly(ds, **context):  
        '''
        Process the scraped monthly raw data. 
        '''
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Processing for: ",curr_date)

        try:
            # read the raw data file already scraped in first task
            raw_filename = os.path.join(raw_data_path, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%m-%Y')}.pdf")
            if os.path.exists(raw_filename):
                tables = camelot.read_pdf(raw_filename, pages='all', flavor='lattice')

                for table in tables:
                    df = table.df.dropna(how='all', axis=1).copy()
                    df = df.replace(r'\r', ' ', regex=True).replace(r'\n', ' ', regex=True).replace('-', '', regex=True).replace('\s+', ' ', regex=True).replace('', np.nan)
                    date_parsed = pd.to_datetime(df[df.columns[0]], format="%B %Y", errors='coerce').dropna()

                    if len(date_parsed) > 0:
        
                        table_date = date_parsed.values[0]
                        date_idx = date_parsed.index.tolist()[0]
                        df = df.iloc[date_idx:].reset_index(drop=True)
                        
                        epf_idx = df.index[df[df.columns[1]].str.lower().str.contains('new EPF subscriber', na=False,case=False)].tolist()    
                        if epf_idx:
                            epf_df = df[ ~(df[df.columns[0]].str.contains('total', na=False,case=False)) ]
                            female_idx = epf_df.index[epf_df[epf_df.columns[2]].str.lower().str.contains('female', na=False,case=False)].tolist()
                            epf_df = epf_df.iloc[female_idx[0]:,1:4].reset_index(drop=True).fillna(0)
                            epf_df.columns = ['epfo_'+col.lower() for col in epf_df.iloc[0].tolist()]
                            epf_df = epf_df.iloc[1: , :]
                            epf_df = epf_df.replace(',', '', regex=True)
                            epf_df[epf_df.columns.tolist()] = epf_df[epf_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                            
                            epf_total_df = pd.DataFrame([epf_df.sum(axis=0).values.tolist()], columns=epf_df.columns.tolist())
                            
                        esic_idx = df.index[df[df.columns[5]].str.lower().str.contains('Number of newly registered', na=False,case=False)].tolist()
                        if esic_idx:
                            esic_df = df[ ~(df[df.columns[0]].str.contains('total', na=False,case=False)) ].reset_index(drop=True)
                            female_idx = esic_df.index[esic_df[esic_df.columns[6]].str.lower().str.contains('female', na=False,case=False)].tolist()
                            esic_df = esic_df.iloc[female_idx[0]:,5:8].reset_index(drop=True).fillna(0)
                            esic_df.columns = ['esic_'+col.lower() for col in esic_df.iloc[0].tolist()]
                            esic_df = esic_df.iloc[1: , :]
                            esic_df = esic_df.replace(',', '', regex=True)
                            esic_df[esic_df.columns.tolist()] = esic_df[esic_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                            
                            esic_total_df = pd.DataFrame([esic_df.sum(axis=0).values.tolist()], columns=esic_df.columns.tolist())
                        
                        nps_idx = df.index[df[df.columns[1]].str.lower().str.contains('existing subscriber', na=False,case=False)].tolist()
                        if nps_idx:
                            nps_df = df[ ~(df[df.columns[0]].str.contains('total', na=False,case=False)) ]
                            female_idx = nps_df.index[nps_df[nps_df.columns[3]].str.lower().str.contains('female', na=False,case=False)].tolist()
                            nps_df = nps_df.iloc[female_idx[0]:,2:-1].reset_index(drop=True).fillna(0)
                            nps_df.columns = ['nps_central_male','nps_central_female','nps_central_trans','nps_central_nonira','nps_central_total',
                                                'nps_state_male','nps_state_female', 'nps_state_trans','nps_state_nonira','nps_state_total',
                                                'nps_nongov_male','nps_nongov_female','nps_nongov_trans','nps_nongov_nonira','nps_nongov_total']
                            nps_df.rename(columns={'nps_central_trans': 'nps_central_others','nps_state_trans': 'nps_state_others','nps_nongov_trans': 'nps_nongov_others'}, inplace=True)
                            nps_df = nps_df.replace(',', '', regex=True).drop(columns=['nps_central_nonira','nps_central_total','nps_state_nonira','nps_state_total','nps_nongov_nonira','nps_nongov_total'])
                            nps_df[nps_df.columns.tolist()] = nps_df[nps_df.columns.tolist()].apply(pd.to_numeric, errors='coerce')
                            
                            nps_total_df = pd.DataFrame([nps_df.sum(axis=0).values.tolist()], columns=nps_df.columns.tolist())

                merged = pd.concat([epf_total_df, esic_total_df,nps_total_df], axis=1).reset_index(drop=True)

                final_df = pd.DataFrame({'date':[curr_date.strftime('01-%m-%Y')]})
                final_df["all_male_india"] = merged["epfo_male"] + merged["esic_male"] + merged["nps_central_male"] + merged["nps_nongov_male"]
                final_df["all_female_india"] = merged["epfo_female"] + merged["esic_female"] + merged["nps_central_female"] + merged["nps_nongov_female"]
                final_df["all_others_india"] = merged["epfo_others"] + merged["esic_others"] + merged["nps_central_others"] + merged["nps_state_others"] + merged["nps_nongov_others"]
                final_df["total_india"] = final_df["all_male_india"] + final_df["all_female_india"] + final_df["all_others_india"]
                
                filename = os.path.join(monthly_data_path, f"formal_employment_{curr_date.strftime('%m-%Y')}.csv")
                final_df.to_csv(filename, index=False)
                
            else:
                raise ValueError(f"No raw file downloaded yet for: : {curr_date.strftime('%m-%Y')}")

            return f"Processed final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            raise ValueError(e)


    process_formal_employment_monthly_task = PythonOperator(
        task_id = 'process_formal_employment_monthly',
        python_callable = process_formal_employment_monthly,
        depends_on_past=True
    )
    

    # Upload data file 
    def upload_formal_employment_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            pdf_file_name = os.path.join(raw_data_path, f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%m-%Y')}.pdf")
            upload_file(pdf_file_name, f"{DATASET_NAME}/raw_data", f"Payroll_reporting_Employment_Perspective_{curr_date.strftime('%m-%Y')}.pdf", SECTOR_NAME, "india_pulse")

            filename = os.path.join(monthly_data_path, f"formal_employment_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"formal_employment_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")

            return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"

        except requests.exceptions.RequestException as e:
            raise ValueError(e)


    upload_formal_employment_monthly_task = PythonOperator(
        task_id = 'upload_formal_employment_monthly',
        python_callable = upload_formal_employment_monthly,
        depends_on_past = True
    )

    scrape_formal_employment_monthly_task >> process_formal_employment_monthly_task >> upload_formal_employment_monthly_task

