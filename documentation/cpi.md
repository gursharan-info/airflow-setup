# CPI 
Consumer Price Indices (CPI) measures change over time in general level of prices of goods and services that households acquire for the purpose of consumption.The Price Statistics Division (PSD) of the National Statistical Office (NSO), Ministry of Statistics and Programme Implementation (MoSPI) started compiling Consumer Price Index (CPI) separately for rural, urban, and combined sectors on monthly basis with Base Year (2010=100) for all India and States/UTs with effect from January 2011.


The data is saved at [consumer_price_index](https://isbhydmoh-my.sharepoint.com/:f:/g/personal/idp_isb_edu/EudS-9Q6gEhEj0TPc_t6J0gBFzXZrrk4d1KyRrva_iII9g?e=OTA6Q0)
The scripts are saved in [scripts folder](https://isbhydmoh-my.sharepoint.com/:f:/g/personal/idp_isb_edu/EudS-9Q6gEhEj0TPc_t6J0gBFzXZrrk4d1KyRrva_iII9g?e=OTA6Q0).
# Scraping 
Steps for data downloading the dataset:

- http://www.mospi.nic.in/download-tables-data
- Click on "Consumer Price Index Archives"
- Click Yes.
-  Click on "Time Series" -> Base {2012} -> Current series
- Select the required time periods
- Search index
- Download the file
>Please note that the CPI and inflation here are measured with base year 2012(CPI=100).

# Processing 
- The processing script can be found on [sharepoint](https://isbhydmoh-my.sharepoint.com/:f:/g/personal/idp_isb_edu/EgP7YFur0ZlBsvSNy5oO4B0BSgNIEtF4jJAVmkGmhKS7GQ?e=439MGr). 
- The raw data is read into a dataframe. The first row is skipped due to undesirable strings. 
  
```
df=pd.read_csv(raw, skiprows=[0])
```

- The columns not required are dropped subsequently: 

```
df=df.drop(['Group','Sub Group','Status','Rural','Urban','Status','Unnamed: 10'],axis=1)  
```

  

> Data not available for March-July 2020 due to lockdown-I
- The table is then pivoted to get the categories as columns
```
    df.pivot(index=['Year','Month','State'],columns='Description',values='Combined').reset_index()
```

- For India Pulse, the categories decided were Food and beverages, Household goods and services, Health, Transport and communication, Recreation and amusement, Education. 
```
df3=df2[['Year', 'Month', 'State','Food and beverages','Household goods and services','Health', 'Transport and communication','Recreation and amusement','Education']]

```
- The data columns only have month and year, however, India Pulse is designed to have date formats dd-mm-YY, therefore we assume that the data was uploaded on first of every month 
``` 
df3['date']='01+'-'+df3['Month'].astype(str)+'-'+df3['Year'].astype(str)
```
- The date column is then finally converted into dd-mm-YY format.
```
df3['date']=pd.to_datetime(df3['date']).dt.strftime('%d-%m-%Y')
```
- The columns names are converted into lowers case, and spaces are replaced by '_'
```
df4=df3.rename({'Food and beverages':'food_and_beverages','Household goods and services':'household_services','Transport and communication':'transport_and_communication','Recreation and amusement':'recreation','Health':'health','Education':'education'},axis=1)
```
- The country level data is then sliced out of the existing dataframe 
```
india_df=df[df['State']=='ALL India']
``` 
- Country level data is then merged with the state level data
```
df5=pd.merge(df4,india_df,on=['date'])
```
- The dataframe is then merged with the lgd_codes files to assign each state a LGD code. 
- The final dataframe is saved as csv, and the file name is named as f"cpi_{latest_date}".csv. Here the latest date is the latest date for which data is available. 

