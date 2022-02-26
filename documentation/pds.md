
# PDS 
This is the official documentation for Public Distribution System. 

## Introduction
The Public Distribution System (PDS) evolved as a system of management of scarcity through distribution of foodgrains at affordable prices. 

PDS is operated under the joint responsibility of the Central and the State/UT Governments. The Central Government, through Food Corporation of India (FCI), has assumed the responsibility for procurement, storage, transportation and bulk allocation of food grains to the State Governments. The operational responsibility including allocation within State, identification of eligible families, issue of Ration Cards and supervision of the functioning of Fair Price Shops (FPSs) etc., rest with the State Governments. Under the PDS, presently the commodities namely wheat, rice, sugar and kerosene are being allocated to the States/UTs for distribution. Some States/UTs also distribute additional items of mass consumption through the PDS outlets such as pulses, edible oils, iodized salt, spices, etc.

## Scraping 

 - The data was scraped from [annavitran.nic.in](https://annavitran.nic.in/stateUnautmated#).
 - Click on 'STATES' and chose the state from which the data is to be found. 
 - The script generalizes the link as a function of state name, state id, month number, and year number in the following manner: "https://annavitran.nic.in/unautomatedStDetailMonthDFSOWiseRpt?m={month_num}&y={year_num}&s={str(state['id']).zfill(2)}&sn={state['name']}"
 - The state names and state codes are retrieved from json file at [idp-hfi repository](https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/pds_state_name_codes.json). The month and dates are automatically picked up for the preivious month. 


## Processing 
- The data is processed iteratively for each state each month. 
- The data is in the table which is retrieved and converted into a beautifulsoup object by:
		  `str(soup.select('table[id="example"]')[0])`
- This object is then converted into a dataframe by 
`df = pd.read_html(table_str)[0]`
- All the dataframes are concatenated in a master dataframe
 `master_df  =  pd.concat(total_data).reset_index(drop=True)`
 - The data is then mapped with the respective lgd codes available [here](https://raw.githubusercontent.com/gursharan-info/idp-scripts/master/sources/LGD_pds_monthly_05102021.csv).
 - A column 'month_year' is created and then then the data is aggregated into a dataframe  named 'grouped_df' at monthly level for each district by:
 `grouped_df=merged_df.groupby(['month_year','state_name','state_code','district_name','district_code'])[['total_rice_allocated','total_wheat_allocated',total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()`
 - Similarly, this data is aggregated at state level for each month and saved in a dataframe named 'state_df'. 
 `merged_df.groupby(['month_year','state_name','state_code'])[['total_rice_allocated','total_wheat_allocated','total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()`
 - Then, all the values are aggregated per month to get an estimate for the whole country , and saved as 'india_df'. `merged_df.groupby(['month_year'[['total_rice_allocated','total_wheat_allocated','total_rice_distributed','total_wheat_distributed']].agg('sum').reset_index()`
 - The columns names have to be given a suffix according to the granularity. For example- Total rice allocation for 

	- total_rice_allocated_district at district level data
	- total_rice_allocated_state for state level data
	- total_rice_allocated_india for country level data. 
	  
 - All the data is then merged together into a 'final_df' and saved as a csv file. 
