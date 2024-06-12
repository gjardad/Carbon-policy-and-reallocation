/*******************************************************************************
Evaluate orbis coverage using a subset of industries in france

**************************************************************************/

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
global output "${dropbox}/carbon_policy_reallocation/output/orbis_data_quality"

set seed 1640 

*------------------------------
* Process ORBIS data for a subset of industries in France 
* This chunk of the code does the basic processing required to deduplicate the raw ORBIS data
*------------------------------
{
	use CLOSDATE_year AV TURN STAF TFAS CONSCODE EXCHRATE bvdid using "${raw_data}/ORBIS/orbis_FR_subset.dta", clear
	rename CLOSDATE_year year
	keep if year >= 2005
	rename AV value_added
	rename TURN sales
	rename STAF labor
	rename TFAS capital
	rename CONSCODE code
	replace sales = sales/EXCHRATE
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
	
	tempfile orbis 
	save `orbis'
	
}
*------------------------------
* Read in firm-level 4-digit NACE codes from ORBIS
*------------------------------
{
	tempfile nace_orbis
	
	import delimited "${raw_data}/ORBIS/FR_subset_nace_codes", clear
	
	rename nacepcod2 nace
		
	* Step 1: Sort the data by bvdid
	bysort bvdid: gen random_order = runiform()

	* Step 2: Sort within each bvdid group by the random order
	bysort bvdid (random_order): gen random_nace = nace if _n == 1

	* Step 3: Create the final variable with the random nace value for each bvdid group
	bysort bvdid: egen random_nace_final = min(random_nace)
	
	*bysort bvdid: g stay = (nace == random_nace_final)

	rename random_nace_final nace_orbis
	
	keep bvdid nace_orbis
	duplicates drop
	
	rename nace nace4

	gen str nace_str = string(nace4, "%9.2f")


	gen str nace2 = substr(nace_str, 1, 2) // extract digits before dot
	
	//replace nace2 = substr(string(nace_orbis), 1, 2) if missing(nace2)
	replace nace2 = "" if nace2 == "."
	replace nace2 = "0" + nace2 if strlen(nace2) == 1
	
	save "`nace_orbis'"

}
*------------------------------
* Merge with NACE codes from ORBIS
*------------------------------
{
	use `orbis', clear
	keep if inrange(year, 2013,2019)
	merge m:1 bvdid using "`nace_orbis'", assert(2 3) keep(3) nogen
	
	merge 1:1 bvdid  year using "${int_data}/firm_year", keep(1 3) keepusing(bvdid year)
	g ets= _merge==3
	

	
	g country = substr(bvdid, 1, 2)
	keep if country=="FR"
	gcollapse (sum) sales, by(country year nace2 ets)
	reshape wide sales, i(country year nace2) j(ets)
	replace sales1 = 0 if mi(sales1)
	g output_orbis = sales0 + sales1 
	g output_orbis_ets = sales1

	ren nace2 nace

	tempfile ind_country_year
	save `ind_country_year'
}

*------------------------------
* Read in industry-country data from Eurostat and merge to ORBIS
*------------------------------
{
	import delimited "${raw_data}/Eurostat/industry_output_current_prices_v2.csv", clear
	
	keep nace geo time obs_value
	
	rename time_period year
	rename obs_value output_eurostat
	rename geo country
	keep if country=="FR"
	keep if inrange(year, 2012, 2019)
	
	* Drop NACE codes where we have data for each sub-industry
	drop if nace_r2 == "C16-C18"
	drop if nace_r2 =="C22_C23"
	drop if nace_r2 =="C24_C25"
	drop if nace_r2 =="C29_C30"
	drop if nace_r2 =="C31-C33"
	drop if nace_r2 == "J58-J60"  

	g nace = substr(nace_r2, 2, 3)
	replace nace = "1012" if nace_r2 == "C10-C12"
	replace nace = "1315" if nace_r2 == "C13-C15"
	replace nace = "3132" if nace_r2 == "C31_C32"
	replace nace = "3739" if nace_r2 == "E37-E39"
	replace nace = "5960" if nace_r2 == "J59_J60"
	replace nace = "6263" if nace_r2 == "J62_J63"
	replace nace = "6970" if nace_r2 == "M69_M70"
	replace nace = "7475" if nace_r2 == "M74_M75"
	replace nace = "8082" if nace_r2 == "N80-N82"
	replace nace = "8788" if nace_r2 == "Q87_Q88"
	replace nace = "9092" if nace_r2 == "R90-R92"
	
	drop nace_r2
	
	drop if strlen(country) > 2
	
	* fix some country codes
	replace country = "GR" if country == "EL"
	replace country = "GB" if country == "UK"
		
	merge 1:1 country year nace using `ind_country_year', ///
		keep(3) /// 
		assert(1 3)

	
	replace output_orbis = output_orbis*10^(-6)
	replace output_orbis_ets = output_orbis_ets*10^(-6)

	g orbis_pct = output_orbis/output_eurostat
	
}
	
	
*------------------------------
* Read in industry-country data from Eurostat and merge to ORBIS
*------------------------------
	levelsof nace,local(industries)
	foreach industry in  `industries'{
		tw ///
			(connect output_orbis year if nace=="`industry'") ///
			(connect output_orbis_ets year if nace=="`industry'") ///
			(connect output_eurostat year if nace=="`industry'"),  ///
			legend(order(1 "ORBIS" 2 "ORBIS firms in ETS" 3 "Eurostat") col(3)) ///
			title("NACE 2: `industry'") name("Ind`industry'",replace)
		graph export "${output}/connect orbis_eurostat - FR - ind`industry'.png", replace
		
			
	}
