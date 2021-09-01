import os, requests, pendulum, re
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators import PythonOperator
from airflow.operators.python_operator import PythonOperator
from helpers import google_upload as gupload

data_folder = os.path.join(os.getcwd(), 'data')
data_path = os.path.join(os.path.join(data_folder, 'hfi'), 'agmarknet_daily')
html_path = os.path.join(data_path, 'html')

agmarknet_url = "https://agmarknet.gov.in/PriceAndArrivals/CommodityWiseDailyReport.aspx"
server_error='server error please try again later'
gdrive_agmarknet_folder = '1pJbhs8QiC2E0MGP0AgHBe2SpYTAcQydy'
gdrive_agmarknet_raw_folder = '1X9824GO5xCMZWbAjQXWm0g47mMY9bs_e'
day_lag = 2

def select_date(response, selected_date):
    selected_day = '{dt.day}'.format(dt = selected_date)
    soup=BeautifulSoup(response.text,'html.parser')
    tags = soup.findAll(name='a', attrs={'style': 'color:#266606'})
    encoded_date = [re.findall('\d{4}', x.attrs['href'])[0] for x in tags if x.text==selected_day][0]
    return encoded_date


def select_month(response,view_state_text,event_validation_text,viewstate_generator_text, selected_date, session):
    payload = {'ctl00$ScriptManager1': 'ctl00$cphBody$up1|ctl00$cphBody$drpDwnMonth',
               'ctl00$cphBody$drpDwnYear': selected_date.strftime("%Y"),
               'ctl00$cphBody$drpDwnMonth': selected_date.strftime("%B"),
               '__EVENTTARGET': 'ctl00$cphBody$drpDwnMonth',
               #'__EVENTARGUMENT': str(int(initial_date) + date_count),
               '__VIEWSTATE': view_state_text,
               '__VIEWSTATEGENERATOR': viewstate_generator_text,
               '__EVENTVALIDATION': event_validation_text,
               '__ASYNCPOST': 'true'
               }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }
    response = session.post(response.url, data=payload, headers=headers)
    return response


def capture_UI_state(response):
    soup = BeautifulSoup(response.text, 'html.parser')
    view_state_element = soup.find(name='input', attrs={'type': 'hidden', 'id': '__VIEWSTATE'})
    view_state_text = view_state_element['value']
    event_validation_element = soup.find(name='input', attrs={'type': 'hidden', 'id': '__EVENTVALIDATION'})
    event_validation_text = event_validation_element['value']
    viewstate_generator = soup.find(name='input', attrs={'type': 'hidden', 'id': '__VIEWSTATEGENERATOR'})
    viewstate_generator_text = viewstate_generator['value']
    return view_state_text,event_validation_text,viewstate_generator_text 


def capture_UI_state_regex(response):
    view_state_text=re.findall('__VIEWSTATE\|(.*?)\|',response.text)[0]
    event_validation_text=re.findall('__EVENTVALIDATION\|(.*?)\|',response.text)[0]
    viewstate_generator_text=re.findall('__VIEWSTATEGENERATOR\|(.*?)\|',response.text)[0]
    return view_state_text,event_validation_text,viewstate_generator_text


def send_date(response,current_date,view_state_text,viewstate_generator_text,event_validation_text, selected_date, session):
    payload = {'ctl00$ScriptManager1': 'ctl00$cphBody$up1|ctl00$cphBody$Calendar1',
               'ctl00$cphBody$drpDwnYear': selected_date.strftime("%Y"),
               'ctl00$cphBody$drpDwnMonth': selected_date.strftime("%B"),
               '__EVENTTARGET': 'ctl00$cphBody$Calendar1',
               '__EVENTARGUMENT': current_date,
               '__VIEWSTATE':view_state_text,
               '__VIEWSTATEGENERATOR':viewstate_generator_text,
               '__EVENTVALIDATION':event_validation_text,
               '__ASYNCPOST': 'true'
               }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }
    response = session.post(response.url,data=payload,headers=headers)
    return response



def submit_first_page(view_state_text, event_validation_text, viewstate_generator_text, selected_date, 
                          session, first_url):
    payload = {'ctl00$ddlLanguages': 'en',
               'ctl00$ddlArrivalPrice': '0',
               'ctl00$ddlCommodity': '0',
               'ctl00$ddlState': '0',
               'ctl00$ddlDistrict': '--Select--',
               'ctl00$ddlMarket': '0',
               'ctl00$txtDate': selected_date.strftime("%d-%b-%Y"),
               'ctl00$ValidatorExtender1_ClientState': '',
               'ctl00$txtDateTo': selected_date.strftime("%d-%b-%Y"),
               'ctl00$ValidatorCalloutExtender2_ClientState': '',
               'ctl00$cphBody$drpDwnYear': selected_date.strftime("%Y"),
               'ctl00$cphBody$drpDwnMonth': selected_date.strftime("%B"),
               'ctl00$cphBody$Submit_list': 'Submit',
               '__EVENTTARGET': '',
               '__EVENTARGUMENT': '',
               '__LASTFOCUS': '',
               '__VIEWSTATE':view_state_text,
               '__VIEWSTATEGENERATOR':viewstate_generator_text,
               '__EVENTVALIDATION':event_validation_text
               }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        #'Content-Type': 'application/x-www-form-urlencoded',
        'DNT': '1',
        'Host': 'agmarknet.gov.in',
        'Origin': 'https://agmarknet.gov.in',
        'Referer': 'https://agmarknet.gov.in/PriceAndArrivals/CommodityWiseDailyReport.aspx',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }
    response= session.post(first_url,headers=headers,data=payload)
    return response


def submit_second_page(response,view_state_text, event_validation_text, viewstate_generator_text, selected_date, session):
    payload={
        'ctl00$ddlLanguages': 'en',
        'ctl00$ddlArrivalPrice': '0',
        'ctl00$ddlCommodity': '0',
        'ctl00$ddlState': '0',
        'ctl00$ddlDistrict': '--Select--',
        'ctl00$ddlMarket': '0',
        'ctl00$txtDate': selected_date.strftime("%d-%b-%Y"),
        'ctl00$ValidatorExtender1_ClientState': '',
        'ctl00$txtDateTo': selected_date.strftime("%d-%b-%Y"),
        'ctl00$ValidatorCalloutExtender2_ClientState': '',
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTTARGET':'',
        '__EVENTARGUMENT':'',
        '__LASTFOCUS':'',
        '__VIEWSTATE':view_state_text,
        '__EVENTVALIDATION':event_validation_text,
        '__VIEWSTATEGENERATOR':viewstate_generator_text,
        'ctl00$cphBody$btnSubmit': 'Submit'}
    soup = BeautifulSoup(response.text, 'html.parser')
    check_box_tags = soup.findAll(name='input', attrs={'type': 'checkbox'})
    check_box_dict = {}
    for check_box_tag in check_box_tags:
        check_box_name = check_box_tag.attrs['name']
        check_box_dict[check_box_name] = 'on'
    payload.update(check_box_dict)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '236008',
        #'Content-Type': 'application/x-www-form-urlencoded',
        'DNT': '1',
        'Host': 'agmarknet.gov.in',
        'Origin': 'https://agmarknet.gov.in',
        'Referer': 'https://agmarknet.gov.in/PriceAndArrivals/CommodityWiseDailyReport2.aspx',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }
    response= session.post(response.url,headers=headers,data=payload,allow_redirects=True)
    return response


def scrape_agmarknet_daily(**context):
    # The current date would be previous day from date of execution
    # mydate = datetime.fromtimestamp(context['execution_date'].timestamp())
    selected_date = datetime.fromtimestamp(context['execution_date'].timestamp()) - timedelta(day_lag)
    print('Running for: ', selected_date)

    # mydate = datetime.strptime("31-Jan-2021","%d-%b-%Y")
    # print(mydate)
    date_str = selected_date.strftime("%d-%b-%Y") 
    session = requests.Session()
    first_response = session.get(agmarknet_url)

    view_state_text, event_validation_text, viewstate_generator_text = capture_UI_state(first_response)
    second_response = select_month( first_response, view_state_text, event_validation_text, 
                                   viewstate_generator_text, selected_date, session)
    view_state_text, event_validation_text, viewstate_generator_text = capture_UI_state_regex(second_response)
    encoded_date = select_date(second_response, selected_date)
    current_date = str(int(encoded_date))
    print(current_date)

    third_response = send_date(second_response,current_date, view_state_text, viewstate_generator_text, 
                               event_validation_text, selected_date, session)
    
    view_state_text, event_validation_text, viewstate_generator_text = capture_UI_state_regex(third_response)
    fourth_response = submit_first_page( view_state_text, event_validation_text, viewstate_generator_text, 
                                        selected_date, session, agmarknet_url)
    
    view_state_text, event_validation_text, viewstate_generator_text = capture_UI_state(fourth_response)
    fifth_response = submit_second_page( fourth_response, view_state_text, event_validation_text, 
                                        viewstate_generator_text, selected_date, session)
    soup_fifth_response = BeautifulSoup( fifth_response.text,'html.parser')
    excel_table = soup_fifth_response.find(name='div',attrs={'id':'cphBody_DivExport'})
#     print(excel_table)
    f = os.path.join(html_path, date_str+'.html')
    file = open(f, "w")
    file.write(str(excel_table))
    file.close()
    print('Output generated for ' + date_str)
    session.close()

    #Writing to CSV
    data = []
    list_header = []
    soup = BeautifulSoup(open(f),'html.parser')
    # for header
    header = soup.find_all("table")[0].find("tr")
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue

    # for getting the data
    HTML_data = soup.find_all("table")[0].find_all("tr")[1:]

    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)
    
    # Storing the data into Pandas
    df = pd.DataFrame(data = data, columns = list_header)
    filename = os.path.join(data_path, date_str+'.csv')
    df.to_csv(filename, index=False)
    gupload.upload(filename, date_str+'.csv',gdrive_agmarknet_raw_folder)


default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2021, month=8, day=25, hour=20, minute=00 ).astimezone('Asia/Kolkata'),
    'provide_context': True,
    'email': ['gursharan_singh@isb.edu'],
    'email_on_failure': True,
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

scrape_agmarknet_daily_dag = DAG("agmarkNetDailyScraping", default_args=default_args, schedule_interval="@daily")

scrape_agmarknet_daily_task = PythonOperator(task_id='scrape_agmarknet_daily',
                                       python_callable=scrape_agmarknet_daily,
                                       dag = scrape_agmarknet_daily_dag,
                                       provide_context = True,
                                    )
