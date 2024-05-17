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

	use "${proc_data}/firm_year.dta", clear

*------------------------------
* Generate productivity and dispersion measures
*------------------------------

	// generate productivity measures
	gen sales_co2 = sales/co2
	gen va_co2 = va/co2
	gen sales_labor = sales/labor
	gen va_labor = va/labor
	gen sales_capital = sales/capital
	gen va_capital = va/capital
	
	// within-acitivity heterogeneity in productivity measures
	foreach ind in activity nace{
		
		foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital {
			bysort year `ind': egen mean_`ind'_`var' = mean(`var')
			bysort year `ind': egen median_`ind'_`var' = median(`var')
			bysort year `ind': egen p10_`ind'_`var' = pctile(`var'), p(10)
			bysort year `ind': egen p20_`ind'_`var' = pctile(`var'), p(20)
			bysort year `ind': egen p80_`ind'_`var' = pctile(`var'), p(80)
			bysort year `ind': egen p90_`ind'_`var' = pctile(`var'), p(90)
		}
		
		* identifiers for firm-year obs which are the only obs in a given industry-year
		bysort year `ind': egen count_obs = count(`ind')
		gen solo_`ind' = count_obs == 1
		drop count_obs
		
		// within-activity dispersion
		foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital {
			bysort year `ind': egen std_`ind'_`var' = sd(`var') if solo == 0
			bysort year `ind': egen p9010_ratio_`ind'_`var' = p90_`ind'_`var'/p10_`ind'_`var' if solo == 0
			bysort year `ind': egen p8020_ratio_`ind'_`var' = p80_`ind'_`var'/p20_`ind'_`var' if solo == 0
	}

	

	

	

	
	
	