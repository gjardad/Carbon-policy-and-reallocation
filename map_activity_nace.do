/*******************************************************************************
Creates activity-level data set with corresponding NACE code from EUETS.INFO

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

*------------------------------
* Get the 2-digit NACE codes of all firms associated with each activity
*------------------------------

	use "${int_data}/firm_year.dta", clear

	gen str nace_str = string(nace, "%9.2f")
	gen dot_pos = strpos(nace_str, ".")
	rename nace nace_num
	gen str nace = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	keep activity nace
	
	duplicates drop
	drop if missing(nace)
	
	* Sort the data by activity and nace
	sort activity nace

	* Create a unique identifier for each nace within each activity
	by activity: gen id = _n

	reshape wide nace, i(activity) j(id)

	
	