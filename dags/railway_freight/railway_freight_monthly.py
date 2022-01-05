from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import requests, os, re
from lxml import html
import tabula

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
    'railway_freight_monthly',
    default_args=default_args,
    description='Petroleum Consumption Monthly',
    schedule_interval = '0 20 10 * *',
    start_date = datetime(year=2021, month=10, day=2, hour=12, minute=0),
    catchup = True,
    tags=['logistics'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'railway_freight')
    monthly_data_path = os.path.join(dir_path, 'monthly')
    os.makedirs(monthly_data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)
    

    SECTOR_NAME = 'Logistics'
    DATASET_NAME = 'railway_freight_monthly'


    def scrape_railway_freight_monthly(ds, **context):  
        '''
        Scrape the monthly data file
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)
        
        main_url = "https://indianrailways.gov.in/railwayboard/view_section.jsp?lang=0&id=0,1,304,366,554,818,821"

        try:
            session = requests.Session()
            response = session.get(main_url)
            tree = html.fromstring(response.content)
            session.close()

            # tree
            all_links = [] 
            for a in tree.xpath('//*[@id="table3"]/tbody/tr/td//a'):
                mnth_text = ''.join(a.itertext())
                a_href = a.xpath('@href')[0]
            #     print(a_href)
                if a_href.startswith("/"):
                    link = "https://indianrailways.gov.in"+a_href
                elif a_href.startswith("./"):
                    link = "https://indianrailways.gov.in"+a_href[1:]
                elif a_href.startswith("http"):
                    link = a_href
                all_links.append(dict(month=mnth_text, link=link))
            
            curr_link = [link['link'] for link in all_links if all([curr_date.strftime('%b') in link['month'], curr_date.strftime('%Y') in link['link'] ])]

            if curr_link:
                r = requests.get(curr_link[0], stream = True)
    
                local_raw_path = os.path.join(raw_data_path,  f"raw_{curr_date.strftime('%m-%Y')}.pdf")
                with open(local_raw_path,"wb") as pdf:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            pdf.write(chunk)
                
                # Data Processing
                df = tabula.read_pdf(local_raw_path, pages=2, lattice=True)[0]     
                processed_df = df.replace(r'\r', ' ', regex=True)
                processed_df.columns = [re.sub('\s+',' ', re.sub(r'[^a-zA-Z% ]', '', str(col).lower())).replace(' ','_') for col in processed_df.iloc[0].tolist()]

                start_idx = processed_df.index[processed_df[processed_df.columns[1]].str.lower().str.contains('commodity', na=False,case=False)].tolist()[0]
                last_idx = processed_df.index[processed_df[processed_df.columns[1]].str.lower().str.replace(r' ','').str.contains('%var', na=False,case=False)].tolist()[0]
                processed_df = processed_df.iloc[start_idx+1:last_idx-1].reset_index(drop=True)
                processed_df = processed_df[['commodity','%_age_to_total']].dropna().replace('-', np.nan)
                processed_df['commodity'] = processed_df['commodity'].str.replace(r".?\)","").str.strip()
                processed_df['date'] = curr_date.strftime("01-%m-%Y")
                processed_df['%_age_to_total'] = pd.to_numeric(processed_df['%_age_to_total'])

                reshaped_df = processed_df.pivot(index='date', columns='commodity', values='%_age_to_total').reset_index()
                reshaped_df.columns = [re.sub('\s+',' ', re.sub(r'[^a-zA-Z% ]', '', str(col).lower())).replace(' ','_') for col in reshaped_df.columns.tolist()]
                reshaped_df = reshaped_df[['date','total_coal','rmsp','steel','iron_ore', 'total_cement','foodgrains','fertilizers', 'container_service']].copy()
                reshaped_df['steel_india'] = reshaped_df[['rmsp','steel','iron_ore']].sum(axis=1)
                reshaped_df = reshaped_df.drop(['rmsp','steel','iron_ore'], axis=1)
                reshaped_df = reshaped_df.rename(columns={'total_coal':'coal_india', 'total_cement':'cement_india', 'foodgrains':'foodgrain_india', 
                                                        'fertilizers':'fertilizer_india', 'container_service':'container_india',
                                                        'steel_india':'iron_steel_india'})

                if not reshaped_df.empty:
                    filename = os.path.join(monthly_data_path, f"railway_freight_{curr_date.strftime('%m-%Y')}.csv")
                    reshaped_df.to_csv(filename, index=False)
                else:
                    raise ValueError("No link:  No Data available for this month yet")
            else:
                raise ValueError("No link:  No Data available for this month yet")
            

            return f"Scraped raw data for: {curr_date.strftime('%m-%Y')}"

            # else:
                # raise ValueError(f"No data available yet for: : {curr_date.strftime('%m-%Y')}")

        except Exception as e:
            raise ValueError(e)


    scrape_railway_freight_monthly_task = PythonOperator(
        task_id = 'scrape_railway_freight_monthly',
        python_callable = scrape_railway_freight_monthly,
        depends_on_past = True
    )

    

    # Upload data file 
    def upload_railway_freight_monthly(ds, **context):  
        '''
        Upload the process monthly data file on sharepoint
        '''
        # print(context)
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())
        print("Uploading data file for: ",curr_date.strftime('%m-%Y'))

        try:
            local_raw_path = os.path.join(raw_data_path,  f"raw_{curr_date.strftime('%m-%Y')}.pdf")
            upload_file(local_raw_path, f"{DATASET_NAME}/raw_data", f"raw_{curr_date.strftime('%m-%Y')}.pdf", SECTOR_NAME, "india_pulse")

            filename = os.path.join(monthly_data_path, f"railway_freight_{curr_date.strftime('%m-%Y')}.csv")
            upload_file(filename, DATASET_NAME, f"railway_freight_{curr_date.strftime('%m-%Y')}.csv", SECTOR_NAME, "india_pulse")
    
        except Exception as e:
            raise ValueError(e)
        
        return f"Uploaded final data for: {curr_date.strftime('%m-%Y')}"


    upload_railway_freight_monthly_task = PythonOperator(
        task_id = 'upload_railway_freight_monthly',
        python_callable = upload_railway_freight_monthly,
        depends_on_past = True
    )

    scrape_railway_freight_monthly_task >> upload_railway_freight_monthly_task

