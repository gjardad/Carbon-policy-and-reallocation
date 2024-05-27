/*******************************************************************************
Creates data set at the installation-year level with information on
1. emissions
2. BvD id
3. acitivity and nace ids

TO-DOs/Observations:

1. there are some installations for which bvdid in OHA is not the same as bvdid in FOHA.
   for all such installations, we are currently using the bvdid as in their OHA
   
2. there are some duplicates (in installation_id and year) in compliance data set.
   for now we are dropping the obs within a duplicate with the LOWEST level of emissions
   this is arbitrary

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
	
	rename id installation_id
	
	keep installation_id nace_id activity_id

*------------------------------
* Read in account (and bvdid) info
*------------------------------

preserve
	tempfile account
	
	import delimited "${raw_data}/EUTL/account.csv", clear

	keep if accounttype_id == "100-7" | accounttype_id == "100-9" | accounttype_id == "120-0"
	// only OHA, aircraft accounts, or former HA have an installation_id associated with it
	
	g mi_instid = missing(installation_id)
	*sum mi_instid // we dont know installation for about 6% of those accounts
	*tab accounttype_id if mi_instid == 1 // and they are all "120-0" accounts

	keep if !missing(installation_id)
	
	keep installation_id bvdid accounttype_id // obs uniquely identified by installation_id and accounttype_id
	
	bysort installation_id (bvdid): gen bvdid_consistent = bvdid[1] == bvdid[_N]
	*tab bvdid_consistent // most of obs who share installation_id also share bvdid, but not all
	
	drop if bvdid_consistent == 1 & accounttype_id == "120-0" // for those accounts, bvdid is unambiguous
	
	bysort installation_id (bvdid): gen bvdid_120 = bvdid[1] == "120-0"
	// there are no installation_id for which the only account type is a former account (this is reassuring!)
	
	drop if accounttype_id == "120-0" // FOR NOW, let's focus only on the 100-7.
	// then, installation_id uniquely identifies accounts
	
	drop bvdid_120 bvdid_consistent accounttype_id
	
	save "`account'"
restore

*------------------------------
* Merge account info with installation info
*------------------------------

	merge 1:1 installation_id using "`account'"

	drop if _merge == 1 // this seems to be irrelavant installations

	drop _merge

*------------------------------
* Read in compliance info
*------------------------------

	preserve

		tempfile emissions

		import delimited "${raw_data}/EUTL/compliance.csv", clear
		
		keep if year <= 2022 // verified emissions go up to 2022
		
		keep installation_id year verified
		
		bysort installation_id (year): gen starts_2005 = year[1] == 2005
		drop if starts_2005 == 0 // this is a different type of account that we are not interested on (my guess is that this is country level info)
		
		bysort installation_id (year): gen mi_2005 = missing(verified[1])
		// a lot of those for which emissions are missing in 2005 are installations for which info starts in 2013
		// an year with big changes in EUETS' regulation
		
		duplicates tag installation_id year verified, generate(dup1)
		*tab dup1 // around 1.9% of installation-year observations;
		// mostly obs for which verified emissions are missing
		
		bysort installation_id year (dup1): drop if dup1 == 1 & _n == 2
		// drop one obs per duplicate if dup2==1
		
		duplicates tag installation_id year, generate(dup2)
		*tab dup2 // around 0.6% of installation-year observations; they all start 2020 onwards
		
		bysort installation_id year (verified): drop if dup2 == 1 & _n == 1
		// arbitrarily drop the obs within installation and year which has the lowest emissions
		
		keep installation_id year verified
		
		save "`emissions'"
		
	restore
	
*------------------------------
* Merge in emissions info
*------------------------------
	
	merge 1:m installation_id using "`emissions'"
	// perfect match
	
	drop _merge
	
	drop if activity_id == 10 // we don't care about aircrafts
	
	save "${int_data}/installation_year_emissions.dta", replace

	


	
	