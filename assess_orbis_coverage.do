/*******************************************************************************
Compares Orbis coverage with industry-country aggregate data for years 2013, 2019

GJ
*******************************************************************************/

*------------------------------
* Set-up folders
*------------------------------

if "`c(username)'"=="chasewilliamson"{
	global dropbox "/Users/chasewilliamson/Library/CloudStorage/Dropbox/" 
	
}

if "`c(username)'"=="jota_"{
	global dropbox "/Users/jota_/Dropbox/Pesquisa/" 
	
}

global raw_data "${dropbox}/carbon_policy_reallocation/data/raw"
global int_data "${dropbox}/carbon_policy_reallocation/data/intermediate"
global proc_data "${dropbox}/carbon_policy_reallocation/data/processed"

*------------------------------
* Read in industry-country data from Eurostat
*------------------------------

	import delimited "${raw_data}/Eurostat/industry_output_current_prices.csv", clear
	
	keep nace geo time obs_value
	
	rename nace_r2 nace_r2
	rename time_period time
	rename obs_value output