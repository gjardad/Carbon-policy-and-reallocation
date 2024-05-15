/*******************************************************************************
Creates data set at the installation-year level with information on
1. emissions
2. BvD id

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
* Read in account info
*------------------------------

	tempfile inst
	
	import delimited "${raw_data}/EUTL/account.csv", clear

	g mi_instid = missing(installation_id)
	sum mi_instid // we dont know installation for about 30% of accounts

	keep if !missing(installation_id)
	
	keep installation_id bvdid
	
	save "`inst'"

*------------------------------
* Read in compliance info
*------------------------------

	import delimited "${raw_data}/EUTL/compliance.csv", clear
	
	keep installation_id year verified
	
	g mi_em = missing(verified)
	sum mi_em // info on emissions is missing for ~60% of installation-year obs
	
	keep if !missing(verified)
		
	duplicates tag installation_id year, generate(duplicate)
	
	preserve
		
		keep if duplicate == 1
		
	restore
	
	drop mi_em
	
*------------------------------
* Merge account and compliance info
*------------------------------

merge m:1 installation_id using "`inst'"

	


	
	