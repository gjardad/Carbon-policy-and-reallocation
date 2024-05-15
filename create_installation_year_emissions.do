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
* Read in installation info
*------------------------------

	import delimited "${raw_data}/EUTL/installation.csv", clear
	
	keep if activity_id != 10
	
	rename id installation_id
	
	keep installation_id nace_id activity_id


*------------------------------
* Read in account (and bvdid) info
*------------------------------

preserve
	tempfile account
	
	import delimited "${raw_data}/EUTL/account.csv", clear

	keep if accounttype_id == "100-7" | accounttype_id == "120-0"
	// only OHA, aircraft accounts, or former HA have an installation_id associated with it
	// and we dont care about aircraft accounts (type 100-9)
	
	g mi_instid = missing(installation_id)
	sum mi_instid // we dont know installation for about 6% of those accounts
	tab accounttype_id if mi_instid == 1 // and they are all "120-0" accounts

	keep if !missing(installation_id)
	
	keep installation_id bvdid accounttype_id // obs uniquely identified by installation_id and accounttype_id
	
	bysort installation_id (bvdid): gen bvdid_consistent = bvdid[1] == bvdid[_N]
	tab bvdid_consistent // most of obs who share installation_id also share bvdid, but not all
	
	drop if bvdid_consistent == 1 & accounttype_id == "120-0" // for those accounts, bvdid is unambiguous
	
	bysort installation_id (bvdid): gen bvdid_120 = bvdid[1] == "120-0"
	// there are no installation_id for which the only account type is a former account (this is reassuring!)
	
	drop if accounttype_id == "120-0" // for now, let's focus only on the 100-7.
	// then, installation_id uniquely identifies accounts
	
	drop bvdid_120 bvdid_consistent accounttype_id
	
	save "`account'"
restore

*------------------------------
* Merge account info to obtain bvdid
*------------------------------

	merge 1:1 installation_id using "`account'"

	drop if _merge == 1 // this seems to be irrelavant installations

	drop _merge


*------------------------------
* Read in compliance info
*------------------------------

	import delimited "${raw_data}/EUTL/compliance.csv", clear
	
	keep if year <= 2022
	
	g mi_em = missing(verified)
	sum mi_em // info on emissions is missing for ~60% of installation-year obs
	
	keep if !missing(verified)
		
	duplicates tag installation_id year, generate(duplicate) // around 1% of installation-year observations 
	
	preserve
		
		keep if duplicate == 1
		
	restore
	
	drop mi_em
	
*------------------------------
* Merge account and compliance info
*------------------------------

merge m:1 installation_id using "`inst'"

	


	
	