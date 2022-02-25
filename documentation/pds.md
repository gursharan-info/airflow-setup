
# PDS 
This is the official documentation for Public Distribution System. 

## Introduction
The Public Distribution System (PDS) evolved as a system of management of scarcity through distribution of foodgrains at affordable prices. 

PDS is operated under the joint responsibility of the Central and the State/UT Governments. The Central Government, through Food Corporation of India (FCI), has assumed the responsibility for procurement, storage, transportation and bulk allocation of food grains to the State Governments. The operational responsibility including allocation within State, identification of eligible families, issue of Ration Cards and supervision of the functioning of Fair Price Shops (FPSs) etc., rest with the State Governments. Under the PDS, presently the commodities namely wheat, rice, sugar and kerosene are being allocated to the States/UTs for distribution. Some States/UTs also distribute additional items of mass consumption through the PDS outlets such as pulses, edible oils, iodized salt, spices, etc.

## Scraping 

 - The data was scraped from [annavitran.nic.in](https://annavitran.nic.in/stateUnautmated#).
 - Click on 'STATES' and chose the state from which the data is to be found. 
 - The script generalizes the link as a function of state name, state id, month number, and year number in the following manner: "https://annavitran.nic.in/unautomatedStDetailMonthDFSOWiseRpt?m={month_num}&y={year_num}&s={str(state['id']).zfill(2)}&sn={state['name']}"
 - 
 - 

