/*******************************************************************************
Some numbers for the electricity sector in Spain and Portugal

This is something Mar requested in May 28, 2024

TO-DO/Obs:
1. 

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
global output "${dropbox}/carbon_policy_reallocation/output"

*------------------------------
* Number of firms in electricity sector in Portugual and Spain
*------------------------------

	use "${int_data}/firm_year.dta", clear
	
	replace nace = nace_orbis/100 if missing(nace)
	
	g country = substr(bvdid, 1, 2)
	
	count if country == "ES" &  abs(nace - 35.11) < 1e-6 & year == 2013
	count if country == "PT" &  abs(nace - 35.11) < 1e-6 & year == 2013
	
*------------------------------
* Number of installations in electricity sector in Portugual and Spain
*------------------------------

	use "${int_data}/installation_year_emissions.dta", clear
	
	g country = substr(bvdid, 1, 2)
	
	count if country == "ES" &  abs(nace - 35.11) < 1e-6 & year == 2013
	count if country == "PT" &  abs(nace - 35.11) < 1e-6 & year == 2013
	
