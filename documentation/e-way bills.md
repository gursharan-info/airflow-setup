# Eway bills
E-Way bill system is for GST registered person / enrolled transporter for generating the way bill (a document to be carried by the person in charge of conveyance) electronically on commencement of movement of goods exceeding the value of Rs. 50,000 in relation to supply or for reasons other than supply or due to inward supply from an unregistered person.
The data is available on: https://www.gst.gov.in/download/gststatistics
The processed data and script is available on [India Pulse(HFI) sharepoint](https://isbhydmoh-my.sharepoint.com/:f:/g/personal/idp_isb_edu/EmU4G7koE4xKm7EXhD6Kg_EB8MCcrZnRu7zS7lsRDfu8Zg?e=tGWVUR).

# Scraping and Processing 
- The gst data can be accesed directly using the API. 
api_url = "https://www.gstn.org.in/gstnadmin/hr/ewayBillApi.json"
- Request library is then used to retrieve the data, and return a real object that represents the data.
```
session = requests.Session()
resp = session.get(api_url)
session.close()
resp.content
``` 
- The loads method helps to parase the JSON string and convert into a python dictionary. 
```
json_response = json.loads(resp.content.decode())['query']
```
- We then iterate over the whole dict and create a dictionary for the data for each month with variables date,intrastate_eway_bills_india, interstate_eway_bills_india, total_eway_bills_india. The data for each month(mnth_dict) is then appended to a list. 
```
data_list = []
for idx, month in enumerate(json_response['labels']):
mnth_dict = dict(
date = month,intrastate_eway_bills_india = json_response['datasets'][0]['data'][idx],interstate_eway_bills_india = json_response['datasets'][1]['data'][idx],total_eway_bills_india = json_response['datasets'][2]['data'][idx])
data_list.append(mnth_dict)
```
- The data for the previous month and retrieved and saved in a csv file. 
```
data_df = pd.DataFrame.from_dict(data_list, orient='columns')
data_df['date'] = pd.to_datetime(data_df['date'], format="%b %Y")
data_df = data_df[data_df['date'] == curr_date.strftime("%Y-%m-01")].copy()
data_df.to_csv(os.path.join(data_path, f"eway_bills_{curr_date.strftime('%m%Y')}.csv"), index=False)
```

