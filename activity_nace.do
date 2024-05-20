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
* Read in firm-level 4-digit NACE codes from ORBIS
*------------------------------

	tempfile nace_orbis
	
	import delimited "${raw_data}/ORBIS/orbis_nace", clear
	
	rename nacepcod2 nace
	
	keep if !missing(nace)
	
	keep bvdid nace
	
	* Create a unique identifier for each nace value within each bvdid
	bysort bvdid (nace): gen nace_id = _n

	* Reshape the data from long to wide format
	reshape wide nace, i(bvdid) j(nace_id)
	
	save "`nace_orbis'"
	
*------------------------------
* Read in firm-year-level data
*------------------------------

	use "${int_data}/firm_year.dta", clear
	
	drop nace_orbis
	keep if year == 2005
	keep bvdid nace
	
	merge 1:1 bvdid using "`nace_orbis'"
	
	drop if _merge == 2
	
	drop _merge
	
	*make nace codes comparable
	gen long nace_eutl = round(nace * 100)

	forvalues v = 1/25{
		g match_`v' = (nace_eutl == nace`v')
	}
	
	egen any_match = rowmax(match_1 match_2 match_3 match_4 match_5 match_6 match_7 match_8 match_9 match_10 match_11 match_12 match_13 match_14 match_15 match_16 match_17 match_18 match_19 match_20 match_21 match_22 match_23 match_24 match_25)
	
		
		