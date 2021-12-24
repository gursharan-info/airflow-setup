from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests, os, re, urllib3
from lxml import html
import PyPDF2 as pypdf
from tabula import read_pdf
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
    'electricity_daily',
    default_args=default_args,
    description='Electricity Supply Daily',
    schedule_interval = '@daily',
    # start_date = days_ago(6),
    start_date = datetime(year=2021, month=11, day=11, hour=12, minute=0),
    catchup = True,
    tags=['electricity'],
) as dag:

    dir_path = os.path.join(os.path.join(os.path.join(os.getcwd(), 'data'), 'IndiaPulse'), 'electricity')
    data_path = os.path.join(dir_path, 'daily')
    os.makedirs(data_path, exist_ok = True)
    raw_data_path = os.path.join(dir_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok = True)

    main_url = "https://posoco.in/reports/daily-reports/daily-reports-2021-22/"
    states = ['Punjab', 'Haryana', 'Rajasthan', 'Delhi', 'UP', 'Uttarakhand', 'HP', 'J&K(UT) & Ladakh(UT)','J&K(UT)','Ladakh(UT)','J&K','Ladakh', 'Chandigarh', 'Chhattisgarh', 'Gujarat', 'MP', 'Maharashtra', 'Goa', 'DD', 'DNH',"Essar steel", 'AMNSIL', 'Andhra Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Puducherry','Pondy','Bihar','DVC', 'Jharkhand', 'Odisha', 'West Bengal', 'Sikkim', 'Arunachal Pradesh', 'Assam', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Tripura']
    elec_columns = ['state','max_demand_met_state','shortage','energy_met_state','drawal_schedule_mu','od_ud_mu','max_od_mw','energy_storage']

    SECTOR_NAME = 'Consumption'
    DATASET_NAME = 'electricity_daily'
    day_lag = 1
    

    def scrape_electricity_daily(ds, **context):  
        '''
        Scrapes the daily raw data of electricity supply
        '''
        # print(context['execution_date'])
        # The current date would be derived from the execution date using the lag parameter. 
        # Lag is the delay which the source website has to upload data for that particular date
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Scraping on: ",context['data_interval_end'])   
        print("Scraping for: ",curr_date)

        try:
            curr_date = datetime.fromtimestamp(context['execution_date'].timestamp())- timedelta(day_lag)
            scrape_dateString = curr_date.strftime("%d-%m-%y")

            session = requests.Session()
            response = session.get(main_url)
            tree = html.fromstring(response.content)
            # For 2020-21
            # anchor_tags = [a.values()[0] for a in tree.xpath('//*[@id="wpdmmydls-cce6da02590d197e2b9dc1635b59849e"]/tbody/tr/td/a')]
            # For 2021-22. For later year you may need to find and change the xpath
            anchor_tags = [a.values()[0] for a in tree.xpath('//*[@id="wpdmmydls-462b5773dd1f1c85ab4365e6a09bde68"]/tbody/tr/td/a')]
            update_list = [url for url in anchor_tags if 'nldc_psp' in url]
            latest_url = [a for a in update_list if scrape_dateString in a]
            print('list', latest_url)
            # for url in update_list:
            if len(latest_url) > 0:
                url = latest_url[0]
                print(url)
                # while True:

                # date = (datetime.datetime.strptime(date,"%d.%m.%y") - datetime.timedelta(days=1)).strftime("%d/%m/%Y")
                # file_date = scrape_dateString
                # print(file_date)
                table_not_found = 0
                response = requests.get(url)
                d = response.headers['content-disposition'] 
                fname = re.findall("filename=(.+)", d)[0]
                date_obj = datetime.strptime(url.split("/")[-2].split("_")[0], "%d-%m-%y")
                file_date = date_obj.strftime("%d-%m-%Y")
                
                file_loc = os.path.join(raw_data_path, 'file'+file_date+'.pdf')
                with open (file_loc,'wb') as f:
                    f.write(response.content)
                f.close()

                upload_file(file_loc, f"{DATASET_NAME}/raw_data", 'file'+file_date+'.pdf', SECTOR_NAME, "india_pulse")
            else:
                print('No data available')

            return f"Scraped data for: {curr_date.strftime('%d-%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)

    scrape_electricity_daily_task = PythonOperator(
        task_id='scrape_electricity_daily',
        python_callable=scrape_electricity_daily,
        depends_on_past=True
    )



    def process_electricity_daily(ds, **context):  
        '''
        Process the scraped daily raw data. Uses the processed historical data file to derive 7 day rolling mean of the values
        '''
        # The current date would be derived from the execution date using the lag parameter. 
        # Lag is the delay which the source website has to upload data for that particular date
        curr_date = datetime.fromtimestamp(context['data_interval_start'].timestamp())- timedelta(day_lag)
        print("Processing for: ",curr_date)

        try:
            file_date = curr_date.strftime("%d-%m-%Y")
            file_loc = os.path.join(raw_data_path, 'file'+file_date+'.pdf')
            #FIND_TABLE
            pdf = open(file_loc,'rb')
            pdfReader = pypdf.PdfFileReader(pdf)
            page_num = -1
            table_num = -1
            table_1 = 'Power Supply Position at All India and Regional level'
            table_2 = 'Frequency Profile'
            keyword = 'Power Supply Position in States'
            for x in range(pdfReader.numPages):
                page = pdfReader.getPage(x)
                page_text = page.extractText()
                if keyword in page_text:
                    page_num = x+1
                    if table_1 in page_text:
                        table_num = 2
                    elif table_2 in page_text:
                        table_num = 1
                    else:
                        table_num = 0
            pdf.close()
            if page_num == -1 :
                table_not_found = 1
            else:
                df = read_pdf(file_loc,pages=page_num)[table_num]
            #FORMATTING
                values = []
                for x in range(len(df)):
                    temp = df[x:x+1].dropna(1)
                    value = list(temp.values[0])
                    for y in range(len(value)):
                        if value[y] in states:
                            values.append(value[y:])
                            break
                elec_data = pd.DataFrame(values,columns=elec_columns)
                elec_data['region'] = ''
                for x in elec_data.index:
                    if elec_data['state'][x] in states[:13]:
                        elec_data['region'][x] = 'NR'
                    elif elec_data['state'][x] in states[13:22]:
                        elec_data['region'][x] = 'WR'
                    elif elec_data['state'][x] in states[22:29]:
                        elec_data['region'][x] = 'SR'
                    elif elec_data['state'][x] in states[29:35]:
                        elec_data['region'][x] = 'ER'
                    elif elec_data['state'][x] in states[35:]:
                        elec_data['region'][x] = 'NER'
                elec_data['date'] = file_date
                for x in elec_data.columns[1:8]:
                    try:
                        elec_data[x] = elec_data[x].astype(int)
                    except:
                        elec_data[x] = elec_data[x].astype(float)

            posoco = elec_data.iloc[:,[0,1,3,9]]
            remove_list = ['AMNSIL','DVC']
            posoco =posoco[~posoco['state'].isin(remove_list)]

            posoco['max_demand_met_india'] = posoco['max_demand_met_state'].groupby(posoco['date']).transform('sum')
            posoco['energy_met_india'] = posoco['energy_met_state'].groupby(posoco['date']).transform('sum')

            stlgd = pd.DataFrame({
                'state':['J&K(UT) & Ladakh(UT)','Bihar','Sikkim','Arunachal Pradesh',
                        'Nagaland','Manipur','Mizoram',  'Tripura', 'Meghalaya',
                        'Assam', 'West Bengal','HP', 'Jharkhand', 'Odisha',
                        'Chhattisgarh','MP','Gujarat', 'Maharashtra','Andhra Pradesh',
                        'Karnataka','Punjab','Goa', 'Kerala', 'Tamil Nadu','Puducherry',
                        'Telangana', 'DD', 'DNH', 'Chandigarh','Uttarakhand',
                        'Haryana','Delhi','Rajasthan', 'UP'],

                'state_code':[1,10,11,12,13,14,15,16,17,18,19,2,20,21,22,23,24,27,
                            28,29,3,30,32,33,34,36,38,38,4,5,6,7,8,9]
            })
            #stlgd

            posoco = posoco.merge(stlgd, on='state', how='left')
            posoco.rename(columns={'state':'state_name'}, inplace=True)
            posoco = posoco[['date','state_name', 'state_code','max_demand_met_state', 'energy_met_state',  
                                    'max_demand_met_india', 'energy_met_india']]
            posoco_file_loc = os.path.join(data_path, 'electricity_'+file_date+'.csv')
            posoco.to_csv(posoco_file_loc,index=False)
            # gupload.upload(posoco_file_loc, file_date+'.csv',gdrive_electricity_folder)
            # sharepoint.upload(posoco_file_loc, 'electricity_daily_'+file_date+'.csv', SECTOR_NAME, DATASET_NAME)
            upload_file(posoco_file_loc, DATASET_NAME, 'electricity_'+file_date+'.csv', SECTOR_NAME, "india_pulse")

            return f"Processed final data for: {curr_date.strftime('%d-%m-%Y')}"

        except requests.exceptions.RequestException as e:
            print(e)


    process_electricity_daily_task = PythonOperator(
        task_id='process_electricity_daily',
        python_callable=process_electricity_daily,
        depends_on_past=True
    )
    
    scrape_electricity_daily_task >> process_electricity_daily_task

