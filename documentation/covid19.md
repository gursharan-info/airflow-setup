# COVID-19
This is the documentation for [covid19_daily.py](https://github.com/bippisb/idp_hfi_automation/blob/develop/dags/covid19_daily.py "covid19_daily.py"), [covid19_monthly.py](https://github.com/bippisb/idp_hfi_automation/blob/develop/dags/covid19_monthly.py "covid19_monthly.py")

## Scraping 
- The data is retrieved from by covid19india.org. 
Link: https://api.covid19india.org/csv/latest/districts.csv
- The daily data has a lag of one day, hence the values are calculated for the previous day. 

## Processing 
- The data is cumulative, hence, the daily values are calculated by subtracting the values for (n-1)th day from the values for nth day. 
- The cumulative values for nth day are saved in a dataframe: `yday_filtered=district_wise_daily[district_wise_daily['Date'].isin([yday])].iloc[:,0:6].reset_index(drop=True)`
- The cumulative  values for (n-1)th day are saved in another dataframe: `db_yday_filtered=district_wise_daily[district_wise_daily['Date'].isin([db_yday])].iloc[:,1:6].reset_index(drop=True)` 
- The final data for each day is calculated using: `delta_df[['Confirmed','Recovered','Deceased']] = ( df1  -  df2).astype(int)`
- The delta_df is then mapped with the LGD codes available at [idp-repository](https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_covid_20Oct19.csv).
- The daily values for state are calculated by aggregating(sum) the delta_df at ['Date','State'].
- The country level data is retrieved from https://api.covid19india.org/csv/latest/case_time_series.csv.
- The district level data(delta_df), state level data(state_total_df) and india level data(india_total_df) is then merged into a single dataframe(final_merged_data). 
- The null values and unknown districts are kept empty. `
final_merged_data = final_merged_data.fillna("")`
- The dataframe is then saved as f{'covid_'+yday+'.csv'}, where yday is the date for nth day. 
