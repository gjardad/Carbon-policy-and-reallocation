/*******************************************************************************
Creates measures of within-industry dispersion in productivity

Measures of productivity are:
1. CO2 productivity:
	1a. sales/emissions
	1b. value added/emissions
	
2. Labor productivity:
	2a. sales/labor
	2b. value added/labor
	
3. Capital productivity:
	3a. sales/capital
	3b. value added/capital
	
Measures of dispersion are:
	
1. within activity std
2. within activity p90-p10 ratio
3. within activity p80-p20 ratio

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
* Generate productivity and dispersion measures
*------------------------------

	// generate productivity measures
	gen sales_co2 = log(sales/co2)
	gen va_co2 = log(va/co2)
	gen sales_labor = log(sales/labor)
	gen va_labor = log(va/labor)
	gen sales_capital = log(sales/capital)
	gen va_capital = log(va/capital)
	
	rename nace nace_4digit
	gen str nace_str = string(nace_4digit, "%9.2f")
	gen dot_pos = strpos(nace_str, ".")
	gen str nace = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	replace nace = substr(string(nace_orbis), 1, 2) if missing(nace)
	replace nace = "" if nace == "."
	
	// within-industry heterogeneity in productivity measures
	foreach ind in activity nace{
		
		foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital {
			
			bysort year `ind': egen mean_`ind'_`var' = mean(`var')
			bysort year `ind': egen median_`ind'_`var' = median(`var')
			bysort year `ind': egen p10_`ind'_`var' = pctile(`var'), p(10)
			bysort year `ind': egen p20_`ind'_`var' = pctile(`var'), p(20)
			bysort year `ind': egen p80_`ind'_`var' = pctile(`var'), p(80)
			bysort year `ind': egen p90_`ind'_`var' = pctile(`var'), p(90)
			
			* some industries have only firm-year for which prod measure is non-missing,
			* in which case dispersion will be 0 by construction
			bysort year `ind': egen valid_`ind'_`var' = count(`var')
		
			// within-activity dispersion
			bysort year `ind': egen std_`ind'_`var' = sd(`var')
			bysort year `ind': gen p9010_`ind'_`var' = p90_`ind'_`var' - p10_`ind'_`var' 
			bysort year `ind': gen p8020_`ind'_`var' = p80_`ind'_`var' - p20_`ind'_`var'

		}
		
	}

*------------------------------
* Create data sets at the activity and NACE code levels
*------------------------------
	preserve
	
		collapse (first) p9010_activity_sales_co2 p9010_activity_sales_labor p9010_activity_sales_capital, by(activity year) 
		
		save "${proc_data}/prod_dispersion_activity.dta", replace
		
	restore
	
	preserve
		
		collapse (first) p9010_nace_sales_co2 p9010_nace_sales_labor p9010_nace_sales_capital ///
						 valid_nace_sales_co2 valid_nace_sales_labor valid_nace_sales_capital ///
						 p9010_nace_va_co2 p9010_nace_va_labor p9010_nace_va_capital ///
						 valid_nace_va_co2 valid_nace_va_labor valid_nace_va_capital, by(nace year)
						 
		drop if missing(nace)

		save "${proc_data}/prod_dispersion_nace.dta", replace
		
	restore
	
	

	

	

	

	
	
	