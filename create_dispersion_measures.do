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

	* Drop observations for which we don't have sales
	drop if mi(sales)
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
	
	gen str nace_str = string(nace, "%9.2f")
	replace nace_str = "0" + nace_str if strlen(nace_str)==3
	
	gen dot_pos = strpos(nace_str, ".")
	gen str nace2 = substr(nace_str, 1, dot_pos - 1) // extract digits before dot
	
	replace nace2 = substr(nace_orbis, 1, 2) if missing(nace2)
	replace nace2 = "" if nace2 == "."
	
	replace nace_str = nace_orbis if missing(nace_str)
	ren nace_str nace4
	drop if nace4=="."
	* Loop through each industry definition 
	foreach ind in activity nace2 nace4{
	
		preserve 
		
		* Create copies of variables for the collapse 
		* I think this is the most straightforward way to get the naming of the variables
		foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital{ 
			g mean_`ind'_`var' = `var'
			g median_`ind'_`var' = `var'
			g p10_`ind'_`var' = `var'
			g p20_`ind'_`var' = `var'
			g p80_`ind'_`var' = `var'
			g p90_`ind'_`var' = `var'
			g sd_`ind'_`var' = `var'
		}

			collapse (mean) mean_* ///
					 (median) median* ///
					 (p10) p10_* ///
					 (p20) p20_* ///
					 (p80) p80_* ///
					 (p90) p90_*  ///
					 (sd) sd_* ///
					 (count) ///
						valid_`ind'_sales_co2 = sales_co2 ///
						valid_`ind'_sales_capital = sales_capital ///
						valid_`ind'_sales_labor = sales_labor ///
						valid_`ind'_va_co2 = va_co2 ///
					 , ///
					 by(year `ind') ///
					 cw // ignore missings, casewise deletion
			
		* Create the 90-10 difference variables
		foreach var in sales_co2 va_co2 sales_labor va_labor sales_capital va_capital{ 

			g p9010_`ind'_`var'= p90_`ind'_`var'-p10_`ind'_`var'
		}
	
		save "${proc_data}/prod_dispersion_`ind'.dta", replace

		restore 
		
	}

	
		
		
	
	
	
	


	
	

	

	

	

	
	
	