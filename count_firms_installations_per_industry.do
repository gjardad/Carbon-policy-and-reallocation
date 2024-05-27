/*******************************************************************************
Creates data set data set at industry-year level with measures of
within-industry dispersion in productivity and number of firms for
which each of these productivity measures are available

TO-DO/Obs:
1. figure out why _merge == 1,2 > 0 when creating nace-year data set
and why _merge==2 > 0 when creating activity-year data set

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
* Create data set at nace-year level with number of firms and installations
*------------------------------

	use "${int_data}/firm_year.dta", clear
	
	rename nace nace_4digit
	gen str nace_str = string(nace_4digit, "%9.2f")
	gen dot_pos = strpos(nace_str, ".")
	gen str nace = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	replace nace = substr(string(nace_orbis), 1, 2) if missing(nace)
	replace nace = "" if nace == "."	
	
	bysort year nace: egen number_firms = count(bvdid)
	bysort year nace: egen number_firms_positive_emissions = count(bvdid) if co2 > 0
	bysort year nace (number_firms_positive_emissions): replace number_firms_positive_emissions = number_firms_positive_emissions[1] if missing(number_firms_positive_emissions)
	
	collapse (first) number_firms*, by(nace year)
	
	drop if missing(nace)
	
	preserve
	
		tempfile installations
		
		use "${int_data}/installation_year_emissions.dta", clear
		
		rename nace nace_4digit
		gen str nace_str = string(nace_4digit, "%9.2f")
		gen dot_pos = strpos(nace_str, ".")
		gen str nace = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
		
		rename activity_id activity
		
		bysort year nace: egen number_installations = count(installation_id)
		bysort year nace: egen number_inst_positive_emissions = count(installation_id) if verified > 0
		bysort year nace (number_inst_positive_emissions): replace number_inst_positive_emissions =number_inst_positive_emissions[1] if missing(number_inst_positive_emissions)
		
		collapse (first) number_inst*, by(nace year)
		
		keep if !missing(nace)
		
		save "`installations'"
		
	restore
	
	merge 1:1 nace year using "`installations'"
	
	save "${int_data}/nace_year_number_units.dta", replace
	
*------------------------------
* Create data set at activity-year level with number of firms and installations
*------------------------------
	
	use "${int_data}/firm_year.dta", clear
	
	// update activity categories according to
	// EEA (2014) EU ETS data view user manual
	// (${dropbox}/carbon_policy_reallocation/manuals)
	replace activity = 20 if activity == 1
	replace activity = 21 if activity == 2
	replace activity = 22 if activity == 3
	replace activity = 23 if activity == 4
	replace activity = 24 if activity == 5
	replace activity = 29 if activity == 6
	replace activity = 31 if activity == 7
	replace activity = 32 if activity == 8
	replace activity = 36 if activity == 9
	
	bysort year activity: egen number_firms = count(bvdid)
	bysort year activity: egen number_firms_positive_emissions = count(bvdid) if co2 > 0
	bysort year activity (number_firms_positive_emissions): replace number_firms_positive_emissions = number_firms_positive_emissions[1] if missing(number_firms_positive_emissions)
	
	collapse (first) number_firms*, by(activity year)
	
	preserve
	
		tempfile installations
		
		use "${int_data}/installation_year_emissions.dta", clear
		
		rename activity_id activity
		
		// update activity categories according to
		// EEA (2014) EU ETS data view user manual
		// (${dropbox}/carbon_policy_reallocation/manuals)
		replace activity = 20 if activity == 1
		replace activity = 21 if activity == 2
		replace activity = 22 if activity == 3
		replace activity = 23 if activity == 4
		replace activity = 24 if activity == 5
		replace activity = 29 if activity == 6
		replace activity = 31 if activity == 7
		replace activity = 32 if activity == 8
		replace activity = 36 if activity == 9
		
		bysort year activity: egen number_installations = count(installation_id)
		bysort year activity: egen number_inst_positive_emissions = count(installation_id) if verified > 0
		bysort year activity (number_inst_positive_emissions): replace number_inst_positive_emissions =number_inst_positive_emissions[1] if missing(number_inst_positive_emissions)
		
		collapse (first) number_inst*, by(activity year)
		
		save "`installations'"
		
	restore
	
	merge 1:1 activity year using "`installations'"
	
*------------------------------
* Read in installation info
*------------------------------

	import delimited "${raw_data}/EUTL/installation.csv", clear
	
	rename id installation_id
	
	// as a sanity check, I want to compare our data with info on tables from
	// Verde et al (2019) "Installation entries and exits in the EU ETS"
	// (${dropbox}/carbon_policy_reallocation/literature)
	// in particular, their table A2
	
	// update activity categories according to
	// EEA (2014) EU ETS data view user manual
	// (${dropbox}/carbon_policy_reallocation/manuals)
	replace activity_id = 20 if activity_id == 1
	replace activity_id = 21 if activity_id == 2
	replace activity_id = 22 if activity_id == 3
	replace activity_id = 23 if activity_id == 4
	replace activity_id = 24 if activity_id == 5
	replace activity_id = 29 if activity_id == 6
	replace activity_id = 31 if activity_id == 7
	replace activity_id = 32 if activity_id == 8
	replace activity_id = 36 if activity_id == 9
	
	drop if activity_id == 1000