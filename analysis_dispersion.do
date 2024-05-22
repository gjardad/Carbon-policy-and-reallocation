/*******************************************************************************
Graphs and summary stats on within-industry dispersion

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
* Read in nace-year-level data
*------------------------------

	use "${proc_data}/prod_dispersion_nace.dta", clear
	
*------------------------------
* Create mean and std across sectors by year
*------------------------------
		
	foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital {
		
		bysort year: egen avg_`var' = mean(p9010_nace_`var') if valid_nace_`var' >= 2
		bysort year: egen std_`var' = sd(p9010_nace_`var') if valid_nace_`var' >= 2
		bysort year: egen n_industries_`var' = count(avg_`var')
		
		bysort year (avg_`var'): replace avg_`var' = avg_`var'[1]
		bysort year (std_`var'): replace std_`var' = std_`var'[1]
	}
	
*------------------------------
* Create graph for average dispersion
*------------------------------

	// Graph for average dispersion across all industries 
	preserve
	
		collapse (first) avg_* n_industries*, by(year)
		
		keep if year <= 2020
		
	twoway (line avg_sales_co2 year, lcolor(black) lpattern(dash_dot)) ///
		   (line avg_sales_labor year, lcolor(black) lpattern(solid)) ///
		   (line avg_sales_capital year, lcolor(gs6) lpattern(solid)), ///
		   title("Average dispersion over time") ///
		   xlabel(2005(5)2020) ///
		   ylabel(0(1)5) ///
		   legend(label(1 "CO2") label(2 "Labor") label(3 "Capital"))

	restore
	
*------------------------------
* Create graphs for 5 most polluting sectors
*------------------------------

	preserve
		// identify sectors within "combustion fuels" which are heavily pollutants
		use "${int_data}/firm_year.dta", clear
		
		gen str nace_str = string(nace, "%9.2f")
		gen dot_pos = strpos(nace_str, ".")
		gen str nace_2digit = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
		
		drop if missing(nace)
		
		keep nace_2digit year co2
		
		bysort nace_2digit year: egen nace_co2 = total(co2)
		
		drop co2
		
		duplicates drop
		
		// the NACE sectors that represent the largest share of total emissions
		// within activity 1 are 35, 24, and 20
		
		// the NACE sectors that represent the largest share of total emissions
		// unconditionally are 35, 24, 23, 20, 19
	restore

	// Separate graphs for the 5-most polluting sectors
	preserve
		
		keep if year <= 2020 & inlist(nace, "35", "20", "24", "23", "19")
		
		local nacelist "35 20 24 23 19"
		foreach n of local nacelist {
			twoway (line avg_sales_co2 year if nace == "`n'", lcolor(black) lpattern(dash_dot)) ///
				   (line avg_sales_labor year if nace == "`n'", lcolor(black) lpattern(solid)) ///
		           (line avg_sales_capital year if nace == "`n'", lcolor(gs6) lpattern(solid)), ///
				   title("Dispersion over time for NACE = "`n'"") ///
				   xlabel(2005(5)2020) ///
				   ylabel(0(1)5) ///
				   legend(label(1 "CO2") label(2 "Labor") label(3 "Capital"))
		}

	restore
	
	
	
	

	