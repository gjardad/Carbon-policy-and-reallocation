/*******************************************************************************
Creates data set data set at industry-year level with measures of
within-industry dispersion in productivity and number of firms for
which each of these productivity measures are available

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
* Read in firm-year-level data
*------------------------------

	use "${int_data}/firm_year.dta", clear

*------------------------------
* Create data set at industry-year level with number of firms and installations
*------------------------------
	
	rename nace nace_4digit
	gen str nace_str = string(nace_4digit, "%9.2f")
	gen dot_pos = strpos(nace_str, ".")
	gen str nace = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	replace nace = substr(string(nace_orbis), 1, 2) if missing(nace)
	replace nace = "" if nace == "."	
	
	foreach ind in activity nace{
		bysort year `ind': egen number_firms_in_`ind' = count(bvdid)
	}
	
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
		
		foreach ind in activity nace{
			bysort year `ind': egen number_installations_in_`ind' = count(installation_id)
		}
		
		collapse (first) number_installations*, by(nace year)
		
		keep if !missing(nace)
		
		save "`installations'"
		
	restore
	
	merge 1:1 nace year using "`installations'"
	
	drop *activity*