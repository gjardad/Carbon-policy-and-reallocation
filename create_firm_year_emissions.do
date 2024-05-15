/*******************************************************************************
Creates data set at the firm-year level with information on
1. emissions
2. BvD id
3. acitivity and nace ids

TO-DO/Obs:
1. some firms have 0 emissions for some years. This is probably because in those
years they were not part of the EUETS, in which case their emissions should be missing,
and not zero. Need to fix this.

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
* Read in installation-year info and calculate sum of emissions at the firm level
*------------------------------

	use "${int_data}/installation_year_emissions.dta", clear
	
	g mi_bvdid = missing(bvdid)
	*tab mi_bvdid // 10% of observations
	// which is also 10% of installations because it's a balanced panel
	
	bysort bvdid year: egen firm_emissions = total(verified) if mi_bvdid == 0
	
	*bysort bvdid (activity_id): g act_consistent = activity_id[1] == activity_id[_N] if !missing(bvdid)
	// not consistent across installations within bvdid
	
	*bysort bvdid (nace_id): g nace_consistent = nace_id[1] == nace_id[_N] if !missing(bvdid)
	// not consistent across installations within bvdid
	
	// identify for each bvdid the most polluting installation across all years
	bysort installation_id: egen installation_emissions = total(verified)
	bysort bvdid (installation_emissions): egen max_emissions = max(installation_emissions)
	gen max_installation_id = installation_id if installation_emissions == max_emissions
	bysort bvdid (max_installation_id): replace max_installation_id = max_installation_id[_N]
	
	// create firm-level activity and nace ids
	bysort bvdid: g bvd_activity = activity_id if installation_id == max_installation_id
	bysort bvdid (bvd_activity): replace bvd_activity = bvd_activity[1]
	
	bysort bvdid: g bvd_nace = nace_id if installation_id == max_installation_id
	bysort bvdid (bvd_nace): replace bvd_nace = bvd_nace[1]
	
	*bysort bvdid (bvd_activity): g act_consistent = bvd_activity[1] == bvd_activity[_N] if !missing(bvdid)
	// consistent!
	
	*bysort bvdid (bvd_nace): g nace_consistent = bvd_nace[1] == bvd_nace[_N] if !missing(bvdid)
	// consistent!
	
	collapse (first) firm_emissions bvd_nace bvd_activity, by(bvdid year)
	
	rename bvd_nace nace_id
	rename bvd_activity activity_id
	
	drop if missing(bvdid)
	
	save "${int_data}/firm_year_emissions.dta", replace
	
