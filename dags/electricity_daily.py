from bs4 import BeautifulSoup
import requests, pendulum, os
import PyPDF2 as pypdf
from tabula import read_pdf
import pandas as pd
from datetime import datetime, timedelta
from urllib.request import urlopen
from lxml import html

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload


data_folder = os.path.join(os.path.join(os.getcwd(), 'data'), 'hfi')
data_path = os.path.join(data_folder, 'electricity_daily')
pdf_path = os.path.join(data_path, 'pdf')
gdrive_electricity_folder = '139M_aquK9oXptaDTjvHlxe0NTzMdu8m7'
gdrive_electricity_pdf_folder = '1n-619wmzIh6b2fnWdyeCzT1SmP5xO8ud'
day_lag = 3

main_url = "https://posoco.in/reports/daily-reports/daily-reports-2020-21/"
states = ['Punjab', 'Haryana', 'Rajasthan', 'Delhi', 'UP', 'Uttarakhand', 'HP', 'J&K(UT) & Ladakh(UT)','J&K(UT)','Ladakh(UT)','J&K','Ladakh', 'Chandigarh', 'Chhattisgarh', 'Gujarat', 'MP', 'Maharashtra', 'Goa', 'DD', 'DNH',"Essar steel", 'AMNSIL', 'Andhra Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Puducherry','Pondy','Bihar','DVC', 'Jharkhand', 'Odisha', 'West Bengal', 'Sikkim', 'Arunachal Pradesh', 'Assam', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Tripura']
elec_columns = ['state','max_demand_met','shortage','energy_met_mu','drawal_schedule_mu','od_ud_mu','max_od_mw','energy_storage']

def read_electricity_data(**context):
    today = datetime.fromtimestamp(context['execution_date'].timestamp())
    scrape_dateString = datetime.strftime(today - timedelta(day_lag), "%d-%m-%y")

    session = requests.Session()
    response = session.get(main_url)
    tree = html.fromstring(response.content)
    anchor_tags = [a.values()[0] for a in tree.xpath('//*[@id="wpdmmydls-cce6da02590d197e2b9dc1635b59849e"]/tbody/tr/td/a')]

    latest_url = [a for a in anchor_tags if scrape_dateString in a]

    if len(latest_url) > 0:
        url = latest_url[0]
        while True:
            try:
        #         date = (datetime.datetime.strptime(date,"%d.%m.%y") - datetime.timedelta(days=1)).strftime("%d/%m/%Y")
                file_date = scrape_dateString
            #     print(file_date)
                table_not_found = 0
                response = requests.get(url)
                file_loc = os.path.join(pdf_path, 'file'+file_date+'.pdf')
                with open (file_loc,'wb') as f:
                    f.write(response.content)
                f.close()
                gupload.upload(file_loc, 'file'+file_date+'.pdf',gdrive_electricity_pdf_folder)

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
                    elec_data['date'] = scrape_dateString
                    for x in elec_data.columns[1:8]:
                        try:
                            elec_data[x] = elec_data[x].astype(int)
                        except:
                            elec_data[x] = elec_data[x].astype(float)

                posoco = elec_data.iloc[:,[0,1,3,9]]
                remove_list = ['AMNSIL','DVC']
                posoco =posoco[~posoco['state'].isin(remove_list)]

                posoco['max_demand_met_india'] = posoco['max_demand_met'].groupby(posoco['date']).transform('sum')
                posoco['energy_met_india'] = posoco['energy_met_mu'].groupby(posoco['date']).transform('sum')

                stlgd = pd.DataFrame({
                    'state':['J&K(UT) & Ladakh(UT)','Bihar','Sikkim','Arunachal Pradesh',
                            'Nagaland','Manipur','Mizoram',  'Tripura', 'Meghalaya',
                            'Assam', 'West Bengal','HP', 'Jharkhand', 'Odisha',
                            'Chhattisgarh','MP','Gujarat', 'Maharashtra','Andhra Pradesh',
                            'Karnataka','Punjab','Goa', 'Kerala', 'Tamil Nadu','Puducherry',
                            'Telangana', 'DD', 'DNH', 'Chandigarh','Uttarakhand',
                            'Haryana','Delhi','Rajasthan', 'UP'],

                    'State_code':[1,10,11,12,13,14,15,16,17,18,19,2,20,21,22,23,24,27,
                                28,29,3,30,32,33,34,36,38,38,4,5,6,7,8,9]
                })
                #stlgd

                posoco = posoco.merge(stlgd, on='state', how='left')

                posoco_file_loc = os.path.join(folder, file_date+'.csv')
                posoco.to_csv(posoco_file_loc,index=False)
                gupload.upload(posoco_file_loc, file_date+'.csv',gdrive_electricity_folder)
                gupload.upload(file_loc, 'file'+file_date+'.pdf',gdrive_electricity_pdf_folder)

                break
            except AssertionError as error:
                # Output expected AssertionErrors.
                Logging.log_exception(error)
                print("Error has been thrown. " + str(error))
                print("cannot process: "+url+" for date: "+scrape_dateString+".  Table format is different.")
                break
            except (ValueError, TypeError):
                print("cannot process: "+url+" for date: "+scrape_dateString+".  Date Values inconsistent.")
                break
            except Exception as exception:
                print("Exception has been thrown. " + str(exception))
                print("cannot process: "+url+" for date: "+scrape_dateString+".  Table format is different.")
                break
    else:
        print('No Data available for the date yet. Try Again tommorow.')

    session.close()


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=8, day=1, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

electricityDaily_dag = DAG("electricityDaily", default_args=default_args, schedule_interval="@daily")

read_electricity_data_task = PythonOperator(task_id='read_electricity_data',
                                       python_callable=read_electricity_data,
                                       dag = electricityDaily_dag,
                                       provide_context = True,
                                    )