/*******************************************************************************
Creates data set at the firm-year level with information on
1. emissions
2. BvD id
3. acitivity ids (from EUTL)
4. nace ids (from EUTL)
5. added value
6. sales

TO-DO/Obs:
1. some firms have 0 emissions for some years. This is probably because in those
years they were not part of the EUETS, in which case their emissions should be missing,
and not zero. Need to fix this.

2. ORBIS also contains NACE codes but we dont have them in current download.
get ORBIS' NACE codes for each firm and compare with our NACE codes.

3. (THIS IS IMPORTANT) check whether obs for which _merge == 2 in final merge
are ORBIS firms for which corresponding account in account.csv are not OHA
(and therefore are not account that belong to firms actually treated by carbon policy)

4. (THIS IS IMPORTANT) check why _merge == 1

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
	keep if year >= 2005
	rename AV value_added
	rename TURN sales
	rename STAF labor
	rename TFAS capital
	rename CONSCODE code
	
	// deal with duplicates across bvdid year:
		// 1. if there are consolidated and unconsolidated obs keep the unconsolidated one
		// this already eliminates almost all of the duplicates
		
		// 2. if can't choose based on consolidated code, check if there's at leat one obs w/ non-missing 
		// value_added. if yes, keep the obs with non-missing value_added
		
		// 3. if can't choose based on missingness of VA, check missingness of sales
		
		// 4. do the same for labor
		
		// 5. do the same for capital
		
		// 6. for the remaining duplicates, choose the obs with highest sales
		
		// 7. if sales same for all obs within firm-year, choose obs with max value_added
		
		// 8. then obs with max labor
		
		// 9. then obs with max capital
		
		// 10. by construction, remaining duplicates are dup in all relevant variables so choose one at random
		
	duplicates tag bvdid year, gen(dup)
	
	* check consolidation codes
	bysort bvdid year (code): gen last_code = code[_N] if dup > 0
	bysort bvdid year (code): gen first_code = code[1] if dup > 0
	bysort bvdid year (code): gen code_consistent = last_code == first_code if dup > 0
	
	// 1. if there are both, keep the unconsolidated
	drop if (dup > 0 & last_code == "U1" & code != "U1") | (dup > 0 & last_code == "U2" & code != "U2")
	
	drop last_code first_code code_consistent dup
	
	duplicates tag bvdid year, gen(dup)

	* check missigness of VA
	bysort bvdid year (value_added): gen mi_last_va = missing(value_added[_N]) if dup > 0
	bysort bvdid year (value_added): gen mi_first_va = missing(value_added[1]) if dup > 0
	bysort bvdid year (value_added): gen mi_va_consistent = mi_last_va == mi_first_va if dup > 0
	
	// 2. if there's no distinction in consolidated code, keep the obs with non-missing value_added
	drop if dup > 0 & mi_va_consistent == 0 & missing(value_added)
	
	drop mi_last_va mi_first_va mi_va_consistent dup
	duplicates tag bvdid year, gen(dup)
	
	* check missigness of sales
	bysort bvdid year (sales): gen mi_last_sales = missing(sales[_N]) if dup > 0
	bysort bvdid year (sales): gen mi_first_sales = missing(sales[1]) if dup > 0
	bysort bvdid year (sales): gen mi_sales_consistent = mi_last_sales == mi_first_sales if dup > 0
	
	// 3. if there's no distinction in consolidated code or VA missigness, keep the obs with non-missing sales
	drop if dup > 0 & mi_sales_consistent == 0 & missing(sales) 
	
	drop mi_last_sales mi_first_sales mi_sales_consistent dup
	duplicates tag bvdid year, gen(dup)
	
	* check missigness of labor
	bysort bvdid year (labor): gen mi_last_labor = missing(labor[_N]) if dup > 0
	bysort bvdid year (labor): gen mi_first_labor = missing(labor[1]) if dup > 0
	bysort bvdid year (labor): gen mi_labor_consistent = mi_last_labor == mi_first_labor if dup > 0
	
	// 4. move on to labor
	drop if dup > 0 & mi_labor_consistent == 0 & missing(labor) 
	
	drop mi_last_labor mi_first_labor mi_labor_consistent dup
	duplicates tag bvdid year, gen(dup)
	
	* check missigness of capital
	bysort bvdid year (capital): gen mi_last_capital = missing(capital[_N]) if dup > 0
	bysort bvdid year (capital): gen mi_first_capital = missing(capital[1]) if dup > 0
	bysort bvdid year (capital): gen mi_capital_consistent = mi_last_capital == mi_first_capital if dup > 0
	
	// 5. move on to capital
	drop if dup > 0 & mi_capital_consistent == 0 & missing(capital) 
	
	drop mi_last_capital mi_first_capital mi_capital_consistent dup
	duplicates tag bvdid year, gen(dup)
	
	// 6. select based on max sales
	bysort bvdid year (sales): gen max_sales = sales[_N]
	drop if dup > 0 & sales != max_sales
	
	drop max_sales dup
	duplicates tag bvdid year, gen(dup)
	
	// 7. select based on max value added
	bysort bvdid year (value_added): gen max_va = value_added[_N]
	drop if dup > 0 & value_added != max_va
	
	drop max_va dup
	duplicates tag bvdid year, gen(dup)
	
	// 8. select based on max labor
	bysort bvdid year (labor): gen max_labor = labor[_N]
	drop if dup > 0 & labor != max_labor
	
	drop max_labor dup
	duplicates tag bvdid year, gen(dup)
	
	// 9. select based on max capital
	bysort bvdid year (capital): gen max_k = capital[_N]
	drop if dup > 0 & capital != max_k
	
	drop max_k dup
	duplicates tag bvdid year, gen(dup)
	
	// 10. choose a random one to drop
	bysort bvdid year: gen number = _n
	drop if dup > 0 & number != 1
	
	drop number dup
	duplicates tag bvdid year, gen(dup)
	
	keep bvdid year value_added sales labor capital
	
	save "`orbis'"
	
*------------------------------
* Merge firm-level emissions with firm-level ORBIS
*------------------------------

	use "${int_data}/firm_year_emissions.dta", clear
	
	merge 1:1 bvdid year using "`orbis'"
	
	// why _merge == 2?
	// ORBIS data is subset of ORBIS firms that are present in EUTL account.csv
	// per code "pull_bvd_id_numbers.do"
	// there are bvdid in account.csv in EUTL that are NOT part of the EUETS
	// e.g. trading accounts
	// if there are trading accounts that are owned by ORBIS firms, then
	// we will select them but they do not actually correspond to EUETS firms
	
	drop if _merge == 2
	
	// why _merge == 1? 
	// firm-year in EUETS that are missing in ORBIS
	// the firm is present in ORBIS for some year, otherwise it wouldnt have BvD id
	// but for some particular year, the info is missing
	
	rename firm_emissions co2
	rename value_added va
	rename activity_id activity
	rename nace_id nace
	
	save "${proc_data}/firm_year.dta", replace
	

	





	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	
