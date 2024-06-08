/*******************************************************************************
Compares Orbis coverage with industry-country aggregate data for years 2013, 2019

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
* Which firms in Orbis have zero sales in 2019?
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
	
	g positive_sales_13 = (sales > 0 & year == 2013 & !missing(sales))
	bysort bvdid (positive_sales_13): replace positive_sales_13 = positive_sales_13[_N]
	
	g positive_sales_19 = (sales > 0 & year == 2019 & !missing(sales))
	bysort bvdid (positive_sales_19): replace positive_sales_19 = positive_sales_19[_N]
	

*------------------------------
* Generate country-year-sector data on output from Orbis
* using data without imputation
*------------------------------

	* important: using sample of firms which are included both in 2013 and 2019
	
	use "${int_data}/balanced_panel_wout_imputing_sales.dta", clear
	
	replace nace2 = "0" + nace2 if strlen(nace2) == 1
	
	g country = substr(bvdid, 1, 2)
	
	* create sales for combined NACE codes
	gen sales_nace0509 = sales if inlist(nace2, "05", "06", "07", "08", "09")
	replace sales_nace0509 = 0 if !inlist(nace2, "05", "06", "07", "08", "09")

	gen sales_nace1012 = sales if inlist(nace2, "10", "11", "12")
	replace sales_nace1012 = 0 if !inlist(nace2, "10", "11", "12")
	
	gen sales_nace1315 = sales if inlist(nace2, "13", "14", "15")
	replace sales_nace1315 = 0 if !inlist(nace2, "13", "14", "15")
	
	gen sales_nace3132 = sales if inlist(nace2, "31", "32")
	replace sales_nace3132 = 0 if !inlist(nace2, "31", "32")
	
	gen sales_nace3739 = sales if inlist(nace2, "37", "38", "39")
	replace sales_nace3739 = 0 if !inlist(nace2, "37", "38", "39")
	
	gen sales_nace4143 = sales if inlist(nace2, "41", "42", "43")
	replace sales_nace4143 = 0 if !inlist(nace2, "41", "42", "43")
	
	gen sales_nace5556 = sales if inlist(nace2, "55", "56")
	replace sales_nace5556 = 0 if !inlist(nace2, "55", "56")
	
	gen sales_nace5960 = sales if inlist(nace2, "59", "60")
	replace sales_nace5960 = 0 if !inlist(nace2, "59", "60")
	
	gen sales_nace6263 = sales if inlist(nace2, "62", "63")
	replace sales_nace6263 = 0 if !inlist(nace2, "62", "63")
	
	gen sales_nace6970 = sales if inlist(nace2, "69", "70")
	replace sales_nace6970 = 0 if !inlist(nace2, "69", "70")
	
	gen sales_nace7475 = sales if inlist(nace2, "74", "75")
	replace sales_nace7475 = 0 if !inlist(nace2, "74", "75")
	
	gen sales_nace8082 = sales if inlist(nace2, "80", "81", "82")
	replace sales_nace8082 = 0 if !inlist(nace2, "80", "81", "82")
	
	gen sales_nace8788 = sales if inlist(nace2, "87", "88", "89")
	replace sales_nace8788 = 0 if !inlist(nace2, "87", "88", "89")
	
	gen sales_nace9092 = sales if inlist(nace2, "90", "91", "92")
	replace sales_nace9092 = 0 if !inlist(nace2, "90", "91", "92")
	
	gen sales_nace9798= sales if inlist(nace2, "97", "98")
	replace sales_nace9798 = 0 if !inlist(nace2, "97", "98")
	
	* generate the sum of sales for each nace-country-year 
	foreach v in "0509" "1012" "1315" "3132" "3739" "4143" "5556" "5960" "6263" "6970" "7475" "8082" "8788" "9092" "9798"{
		
		bysort country year: egen sum_sales_nace`v' = total(sales_nace`v')
	}
	
	foreach v in "01" "02" "03" "04" "16" "17" "18" "19" "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "30" "33" "34" "35" "36" "40" "44" "45" "46" "47" "48" "49" "50" "51" "52" "53" "54" "57" "58" "61" "64" "65" "66" "67" "68" "71" "72" "73" "76" "77" "78" "79" "83" "84" "85" "86" "89" "93" "94" "95" "96" "99"{
		
		bysort country year: egen sum_sales_nace`v' = total(sales) if nace2 == "`v'"
		
		replace sum_sales_nace`v' = 0 if missing(sum_sales_nace`v')
	}
	
	collapse (first) sum_sales*, by(country year)
	
	rename sum_sales_nace01 sum_sales_nace1
	rename sum_sales_nace02 sum_sales_nace2
	rename sum_sales_nace03 sum_sales_nace3
	rename sum_sales_nace04 sum_sales_nace4
	rename sum_sales_nace0509 sum_sales_nace509
	
	reshape long sum_sales_nace, i(country year) j(nace)
	
	tostring nace, replace
	replace nace = "01" if nace == "1"
	replace nace = "02" if nace == "2"
	replace nace = "03" if nace == "3"
	replace nace = "04" if nace == "4"
	replace nace = "0509" if nace == "509"
	
	rename sum_sales output_orbis
	
	save "${int_data}/country_sector_year_sales_orbis_wout_imputing.dta", replace
	
*------------------------------
* Generate country-year-sector data on output from Orbis
* using data withimputation
*------------------------------

	* important: using sample of firms which are included both in 2013 and 2019
	
	use "${int_data}/balanced_panel_with_imputing_sales.dta", clear
	
	replace nace2 = "0" + nace2 if strlen(nace2) == 1
	
	g country = substr(bvdid, 1, 2)
	
	* create sales for combined NACE codes
	gen sales_nace0509 = sales if inlist(nace2, "05", "06", "07", "08", "09")
	replace sales_nace0509 = 0 if !inlist(nace2, "05", "06", "07", "08", "09")

	gen sales_nace1012 = sales if inlist(nace2, "10", "11", "12")
	replace sales_nace1012 = 0 if !inlist(nace2, "10", "11", "12")
	
	gen sales_nace1315 = sales if inlist(nace2, "13", "14", "15")
	replace sales_nace1315 = 0 if !inlist(nace2, "13", "14", "15")
	
	gen sales_nace3132 = sales if inlist(nace2, "31", "32")
	replace sales_nace3132 = 0 if !inlist(nace2, "31", "32")
	
	gen sales_nace3739 = sales if inlist(nace2, "37", "38", "39")
	replace sales_nace3739 = 0 if !inlist(nace2, "37", "38", "39")
	
	gen sales_nace4143 = sales if inlist(nace2, "41", "42", "43")
	replace sales_nace4143 = 0 if !inlist(nace2, "41", "42", "43")
	
	gen sales_nace5556 = sales if inlist(nace2, "55", "56")
	replace sales_nace5556 = 0 if !inlist(nace2, "55", "56")
	
	gen sales_nace5960 = sales if inlist(nace2, "59", "60")
	replace sales_nace5960 = 0 if !inlist(nace2, "59", "60")
	
	gen sales_nace6263 = sales if inlist(nace2, "62", "63")
	replace sales_nace6263 = 0 if !inlist(nace2, "62", "63")
	
	gen sales_nace6970 = sales if inlist(nace2, "69", "70")
	replace sales_nace6970 = 0 if !inlist(nace2, "69", "70")
	
	gen sales_nace7475 = sales if inlist(nace2, "74", "75")
	replace sales_nace7475 = 0 if !inlist(nace2, "74", "75")
	
	gen sales_nace8082 = sales if inlist(nace2, "80", "81", "82")
	replace sales_nace8082 = 0 if !inlist(nace2, "80", "81", "82")
	
	gen sales_nace8788 = sales if inlist(nace2, "87", "88", "89")
	replace sales_nace8788 = 0 if !inlist(nace2, "87", "88", "89")
	
	gen sales_nace9092 = sales if inlist(nace2, "90", "91", "92")
	replace sales_nace9092 = 0 if !inlist(nace2, "90", "91", "92")
	
	gen sales_nace9798= sales if inlist(nace2, "97", "98")
	replace sales_nace9798 = 0 if !inlist(nace2, "97", "98")
	
	* generate the sum of sales for each nace-country-year 
	foreach v in "0509" "1012" "1315" "3132" "3739" "4143" "5556" "5960" "6263" "6970" "7475" "8082" "8788" "9092" "9798"{
		
		bysort country year: egen sum_sales_nace`v' = total(sales_nace`v')
	}
	
	foreach v in "01" "02" "03" "04" "16" "17" "18" "19" "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "30" "33" "34" "35" "36" "40" "44" "45" "46" "47" "48" "49" "50" "51" "52" "53" "54" "57" "58" "61" "64" "65" "66" "67" "68" "71" "72" "73" "76" "77" "78" "79" "83" "84" "85" "86" "89" "93" "94" "95" "96" "99"{
		
		bysort country year: egen sum_sales_nace`v' = total(sales) if nace2 == "`v'"
		
		replace sum_sales_nace`v' = 0 if missing(sum_sales_nace`v')
	}
	
	collapse (first) sum_sales*, by(country year)
	
	rename sum_sales_nace01 sum_sales_nace1
	rename sum_sales_nace02 sum_sales_nace2
	rename sum_sales_nace03 sum_sales_nace3
	rename sum_sales_nace04 sum_sales_nace4
	rename sum_sales_nace0509 sum_sales_nace509
	
	reshape long sum_sales_nace, i(country year) j(nace)
	
	tostring nace, replace
	replace nace = "01" if nace == "1"
	replace nace = "02" if nace == "2"
	replace nace = "03" if nace == "3"
	replace nace = "04" if nace == "4"
	replace nace = "0509" if nace == "509"
	
	rename sum_sales output_orbis
	
	save "${int_data}/country_sector_year_sales_orbis_with_imputing.dta", replace

*------------------------------
* Read in industry-country data from Eurostat
*------------------------------

	import delimited "${raw_data}/Eurostat/industry_output_current_prices.csv", clear
	
	keep nace geo time obs_value
	
	rename time_period year
	rename obs_value output_eurostat
	rename geo country
	
	g nace = substr(nace_r2, 2, 3)
	replace nace = "0509" if nace_r2 == "B"
	replace nace = "1012" if nace_r2 == "C10-C12"
	replace nace = "1315" if nace_r2 == "C13-C15"
	replace nace = "3132" if nace_r2 == "C31_C32"
	replace nace = "35" if nace_r2 == "D"
	replace nace = "3739" if nace_r2 == "E37-E39"
	replace nace = "4143" if nace_r2 == "F"
	replace nace = "5556" if nace_r2 == "I"
	drop if nace_r2 == "J58-J60"
	replace nace = "5960" if nace_r2 == "J59_J60"
	replace nace = "6263" if nace_r2 == "J62_J63"
	replace nace = "68" if nace_r2 == "L"
	replace nace = "6970" if nace_r2 == "M69_M70"
	replace nace = "7475" if nace_r2 == "M74_M75"
	replace nace = "8082" if nace_r2 == "N80-N82"
	replace nace = "84" if nace_r2 == "O"
	replace nace = "85" if nace_r2 == "P"
	replace nace = "8788" if nace_r2 == "Q87_Q88"
	replace nace = "9092" if nace_r2 == "R90-R92"
	replace nace = "9798" if nace_r2 == "T"
	replace nace = "99" if nace_r2 == "U"
	
	drop nace_r2
	
	drop if strlen(country) > 2
	
	* fix some country codes
	replace country = "GR" if country == "EL"
	replace country = "GB" if country == "UK"
		
	merge 1:1 country year nace using "${int_data}/country_sector_year_sales_orbis_with_imputing.dta"
	
	* some NACE dont really exist
	drop if nace == "04"
	drop if nace == "34"
	drop if nace == "40"
	drop if nace == "44"
	drop if nace == "48"
	drop if nace == "54"
	drop if nace == "57"
	drop if nace == "67"
	drop if nace == "76"
	drop if nace == "83"
	drop if nace == "89"
	
	* why _merge == 2? eurostat data doesn't have obs for GB in 2019
	* also no data on nace == 99 (which really doesnt matter)
	
	replace output_orbis = output_orbis*10^(-6)
	
	g orbis_pct = output_orbis/output_eurostat
	
	g representative_in_2013 = (orbis_pct < 1 & orbis_pct > 0.5 & year == 2013)
	bysort country nace (representative_in_2013): replace representative_in_2013 = representative_in_2013[_N]
	
	keep if representative == 1
	egen group = group(country nace)
	twoway (line orbis_pct year if !missing(orbis_pct), connect(l) lcolor(%20)) ///
       (scatter orbis_pct year if !missing(orbis_pct), msymbol(circle) mcolor(%20)), ///
       by(country nace) 
	
	