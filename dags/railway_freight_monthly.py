import pendulum, os, requests, re
import pandas as pd
import numpy as np
from datetime import timedelta
from urllib.parse import urlparse, parse_qs, urlencode
from lxml import html
import tabula

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
# from helpers import google_upload as gupload
from helpers import sharepoint_o365 as sharepoint


dir_path = r'/usr/local/airflow/data/hfi/railway_freight'
data_path = os.path.join(dir_path, 'monthly')
raw_path = os.path.join(dir_path, 'raw_data')
# gdrive_rail_freight_monthly_folder = '1U8HcixB11fKhUKCeDmxBXj6qLMe8nRFQ'
# gdrive_rail_freight_raw_folder = '1AOJls5Oysm0rrXq2E_APxhI_Uf7a4ZCp'
SECTOR_NAME = 'Logistics'
DATASET_NAME = 'railway_freight'

def railway_freight_monthly(**context):
    # Load the main page
    try:
        # print(context['execution_date'], type(context['execution_date']))
        # The current date would be previous day from date of execution
        curr_date =  context['execution_date']
        print("Scraping for: ",curr_date.strftime("%d-%m-%Y"))
        
        session = requests.Session()
        response = session.get("https://indianrailways.gov.in/railwayboard/view_section.jsp?lang=0&id=0,1,304,366,554,818,821")
        tree = html.fromstring(response.content)
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
                
        session.close()

        curr_link = [link['link'] for link in all_links if all([curr_date.strftime('%b') in link['month'], curr_date.strftime('%Y') in link['link'] ])]
        print(curr_date)
        if curr_link:
            r = requests.get(curr_link[0], stream = True)
            with open(os.path.join(raw_path, curr_link[0].split('/')[-1]),"wb") as pdf:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        pdf.write(chunk)
                        
            # gupload.upload(os.path.join(raw_path, curr_link[0].split('/')[-1]), curr_link[0].split('/')[-1], gdrive_rail_freight_raw_folder)
            # sharepoint.upload_file(raw_path, '7A_'+curr_date.strftime("&b_%y")+'.pdf', SECTOR_NAME, f"{DATASET_NAME}/raw_data")
            
            df = tabula.read_pdf(os.path.join(raw_path, curr_link[0].split('/')[-1]),pages=2, lattice=True)[0]
            processed_df = df.replace(r'\r', ' ', regex=True)
            processed_df.columns = [re.sub('\s+',' ', re.sub(r'[^a-zA-Z% ]', '', str(col).lower())).replace(' ','_') for col in processed_df.iloc[0].tolist()]

            start_idx = processed_df.index[processed_df[processed_df.columns[1]].str.lower().str.contains('commodity', na=False,case=False)].tolist()[0]
            last_idx = processed_df.index[processed_df[processed_df.columns[1]].str.lower().str.contains('MT@', na=False,case=False)].tolist()[0]
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
                    
            filename = os.path.join(data_path, f"railway_freight_{curr_date.strftime('%m%Y')}.csv")
            reshaped_df.to_csv(filename, index=False)
            # gupload.upload(filename, f"railway_freight_{curr_date.strftime('%m%Y')}.csv", gdrive_rail_freight_monthly_folder)
            sharepoint.upload_file(filename, f"railway_freight_{curr_date.strftime('%m%Y')}.csv", SECTOR_NAME, DATASET_NAME)

        else:
            # print('No Data available for this month yet')
            raise ValueError('No Data available for this month yet')
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
    "retries": 3,
    "retry_delay": timedelta(days=1),
}

railway_freight_monthly_dag = DAG("railwayFreightScraping", default_args=default_args, schedule_interval='0 20 8 * *')

railway_freight_monthly_task = PythonOperator(task_id='railway_freight_monthly',
                                       python_callable = railway_freight_monthly,
                                       dag = railway_freight_monthly_dag,
                                       provide_context = True,
                                    )