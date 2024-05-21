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
	
	*check matching in terms of first two NACE digits	
	g nace_2digit = substr(string(nace), 1, 2)
	forvalues v = 1/25{
		g nace`v'_orbis_2digit = substr(string(nace`v'), 1, 2)
		g match_2digit_`v' = (nace_2digit == nace`v'_orbis_2digit)
	}
	
	egen any_match_2digit = rowmax(match_2digit_1 match_2digit_2 match_2digit_3 match_2digit_4 match_2digit_5 match_2digit_6 match_2digit_7 match_2digit_8 match_2digit_9 match_2digit_10 match_2digit_11 match_2digit_12 match_2digit_13 match_2digit_14 match_2digit_15 match_2digit_16 match_2digit_17 match_2digit_18 match_2digit_19 match_2digit_20 match_2digit_21 match_2digit_22 match_2digit_23 match_2digit_24 match_2digit_25)
	
	*check matching in terms of first NACE digit
	g nace_1digit = substr(string(nace), 1, 1)
	forvalues v = 1/25{
		g nace`v'_orbis_1digit = substr(string(nace`v'), 1, 1)
		g match_1digit_`v' = (nace_1digit == nace`v'_orbis_1digit)
	}
	
	egen any_match_1digit = rowmax(match_1digit_1 match_1digit_2 match_1digit_3 match_1digit_4 match_1digit_5 match_1digit_6 match_1digit_7 match_1digit_8 match_1digit_9 match_1digit_10 match_1digit_11 match_1digit_12 match_1digit_13 match_1digit_14 match_1digit_15 match_1digit_16 match_1digit_17 match_1digit_18 match_1digit_19 match_1digit_20 match_1digit_21 match_1digit_22 match_1digit_23 match_1digit_24 match_1digit_25)
	
	
		
	
		
		