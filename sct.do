/*******************************************************************************
Scale, Composition, Technology (SCT) decomposition

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
	
	drop capital labor
	
	rename nace nace4
	gen str nace_str = string(nace4, "%9.2f")
	gen dot_pos = strpos(nace_str, ".")
	gen str nace2 = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	replace nace2 = substr(string(nace_orbis), 1, 2) if missing(nace2)
	replace nace2 = "" if nace2 == "."
	
	replace nace4 = nace_orbis/100 if missing(nace4)
	
*------------------------------
* Build balanced panel wout imputing 
*------------------------------
	
	preserve
	
		keep if year == 2019 | year == 2013
		
		bysort bvdid (sales): g last_sales_missing = (sales[_N] == .)
		
		drop if last_sales_missing == 1
		
		save "${int_data}/balanced_panel_wout_imputing_sales.dta", replace
	restore
	
*------------------------------
* Build balanced panel with imputing
*------------------------------
	
	preserve
		
		sort bvdid year
		
		// impute using mean of lead and lags
		gen lag_sales = sales[_n-1]
		gen lead_sales = sales[_n+1]
		gen mean_sales = (lag_sales + lead_sales) / 2
		replace sales = mean_sales if missing(sales) & year != 2005 & year != 2022
		replace sales = mean_sales if sales == 0 & year != 2005 & year != 2022
		
		keep if year == 2019 | year == 2013
		
		bysort bvdid (sales): g last_sales_missing = (sales[_N] == .)
		
		drop if last_sales_missing == 1		
		
		save "${int_data}/balanced_panel_with_imputing_sales.dta", replace
	restore
	
*------------------------------
* Calculate aggregates, shares of aggregates, and EI
*------------------------------

	// using data wout imputation
	use "${int_data}/balanced_panel_wout_imputing_sales.dta", clear
	
	*keep bvdid year co2 sales nace4 nace2 activity

	bysort year: egen agg_output = total(sales)
	bysort year: egen agg_va = total(va)
	bysort year: egen agg_emissions = total(co2)
	bysort year: g agg_ei = agg_emissions/agg_output

	foreach ind in activity nace2 nace4{
		
		bysort `ind' year: egen `ind'_total_output = total(sales)
		bysort `ind' year: gen `ind'_theta = `ind'_total_output/agg_output
		
		g `ind'_alpha = sales/`ind'_total_output
		
		// some industries disappear because only one firm in the industry
		* and this firm has 0 sales
		replace `ind'_alpha = 0 if missing(`ind'_alpha)
		
		bysort `ind' year: egen `ind'_co2 = total(co2)
		bysort `ind' year: g `ind'_ei = `ind'_co2/`ind'_total_output
	}
	
	g ei = co2/sales
	replace ei = 0 if missing(ei)
	
*------------------------------
* SCT
*------------------------------
	
	sort year
	g agg_ei_2013 = agg_ei[1]
	
	* scale
	bysort year: g scale = agg_output*agg_ei_2013
	
	* scale + ind composition
	foreach ind in activity nace2 nace4{
		bysort `ind' year: g `ind'_ei_2013 = `ind'_ei[1]
	
		bysort `ind' year: g `ind'_2019theta_2013ei = `ind'_theta*`ind'_ei_2013 
		bysort year: egen `ind'_aggei_2019theta_2013ei = total(`ind'_2019theta_2013ei)
		bysort year: g scale_`ind'_composition = agg_output*`ind'_aggei_2019theta_2013ei
	}
	
	* scale + ind + firm composition
	bysort bvdid year: g ei_2013 = ei[1]

	foreach ind in activity nace2 nace4{
		
		g `ind'_2019alpha_2013ei = `ind'_alpha*ei_2013
		
		bysort `ind' year: egen `ind'_indei_2019alpha_2013ei = total(`ind'_2019theta_2013ei)
		bysort year: egen `ind'_aggei_19thetaalpha_13ei = total(`ind'_2019theta_2013ei)
		bysort year: g scale_`ind'_firm_composition = agg_output*`ind'_aggei_19thetaalpha_13ei 
		
	}
	
	collapse (first) scale*, by(year)
	
	

	
	