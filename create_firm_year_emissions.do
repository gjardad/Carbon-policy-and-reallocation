/*******************************************************************************
Creates data set at the firm-year level with information on
1. emissions
2. BvD id
3. acitivity ids
4. nace ids
5. added value
6. sales
7. emissions over added value
8. emissions over sales

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
	
*------------------------------
* Read in firm-year-level data from ORBIS
*------------------------------

	tempfile orbis
	
	use "${raw_data}/ORBIS/orbis_eutl_firms.dta", clear
	
	rename CLOSDATE_year year
	rename AV value_added
	rename TURN sales
	rename STAF labor
	rename TFAS capital
	rename CONSCODE cons
	
	// deal with duplicates across bvdid year:
		// 1. if there are consolidated and unconsolidated obs keep the unconsolidated one
		// this already eliminates XX% of the duplicates
		
		// 2. if there's no distinction in consolidated code, check if there's at leat one obs w/ non-missing 
		// valuea_dded. if yes, keep the obs with non-missing valueadded
		
		// 3. if there's no distinction in consolidated code and in missingness of VA, check missingness of sales
		
		// 4. do the same for labor
		
		// 5. do the same for capital
		
		// 6. 
		
	duplicates tag bvdid year, gen(dup)
	
	* check consolidation codes
	bysort bvdid year (cons): gen last_code = cons[_N] if dup > 0
	bysort bvdid year (cons): gen first_code = cons[1] if dup > 0
	bysort bvdid year (cons): gen code_consistent = last_code == first_code if dup > 0
	
	* check missigness of VA
	bysort bvdid year (value_added): gen mi_last_va = missing(value_added[_N]) if dup > 0 & code_consistent == 1
	bysort bvdid year (value_added): gen mi_first_va = missing(value_added[1]) if dup > 0 & code_consistent == 1
	bysort bvdid year (value_added): gen mi_va_consistent = mi_last_va == mi_first_va if dup > 0 & code_consistent == 1
	
	* check missigness of sales
	bysort bvdid year (sales): gen mi_last_sales = missing(sales[_N]) if dup > 0 & code_consistent == 1
	bysort bvdid year (sales): gen mi_first_sales = missing(sales[1]) if dup > 0 & code_consistent == 1
	bysort bvdid year (sales): gen mi_sales_consistent = mi_last_sales == mi_first_sales if dup > 0 & code_consistent == 1
	
	// 1. if there are both, keep the unconsolidated
	drop if dup > 0 & last_code == "U1" & code != "U1" | dup > 0 & last_code == "U2" & code != "U2" 
	
	// 2. if there's no distinction in consolidated code, keep the obs with non-missing value_added
	drop if dup > 0 & code_consistent == 1 & mi_va_consistent == 0 & missing(value_added)
	
	// 3. if there's no distinction in consolidated code or VA missigness, keep the obs with non-missing sales
	drop if dup > 0 & code_consistent == 1 & mi_sales_consistent == 0 & missing(sales) 
	
	
	
	g mi_va = (dup > 0 & missing(valueadded))
	
	save "`orbis'"
	
*------------------------------
* Merge firm-level emissions with firm-level ORBIS
*------------------------------

	use "${int_data}/firm_year_emissions.dta", clear
	
	merge 1:1 bvdid year using "`orbis'"

	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	
