/*******************************************************************************
Graphs and summary stats on number of firms/installations per sector

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
global output "${dropbox}/carbon_policy_reallocation/output"

*------------------------------
* Analysis at the nace_2digit-year level
*------------------------------

	use "${int_data}/nace2_year_number_units.dta", clear
	
	* set style of graphs
	set scheme modern, perm
	
	local nacelist "35 23 24"
	foreach n of local nacelist {				   
		twoway (line number_firms_positive_emissions year if nace == `"`n'"', ///
					lcolor(black) lpattern(solid) yaxis(1)) ///
				   (line number_inst_positive_emissions year if nace == `"`n'"', ///
					lcolor(red) lpattern(solid) yaxis(2)), ///
				   title("") ///
				   xtitle("") ///
				   ytitle("Number of firms", axis(1)) ///
				   ytitle("Number of installations", axis(2) angle(180)) ///
				   xlabel(2005(5)2020) ///
				   legend(label(1 "Active firms") label(2 "Active installations"))
			   
		graph export "${output}/number_units_per_nace_`n'.png", as(png) replace
	}
	
*------------------------------
* Analysis at the nace_4digit-year level
*------------------------------

	use "${int_data}/nace4_year_number_units.dta", clear
	
	* set style of graphs
	set scheme modern, perm
	
	local nacelist 35.11
	local tolerance = 1e-6 // need to add this because nace4digit is float
	foreach n of local nacelist {				   
		twoway (line number_firms_positive_emissions year if abs(nace - `n') < `tolerance', ///
					lcolor(black) lpattern(solid) yaxis(1)) ///
				   (line number_inst_positive_emissions year if abs(nace - `n') < `tolerance', ///
					lcolor(red) lpattern(solid) yaxis(2)), ///
				   title("") ///
				   xtitle("") ///
				   ytitle("Number of firms", axis(1)) ///
				   ytitle("Number of installations", axis(2) angle(180)) ///
				   xlabel(2005(5)2020) ///
				   legend(label(1 "Active firms") label(2 "Active installations"))
			   
		graph export "${output}/number_units_per_nace_`n'.png", as(png) replace
	}
	
*------------------------------
* Analysis at the activity-year level
*------------------------------

	use "${int_data}/activity_year_number_units.dta", clear
	
	* set style of graphs
	set scheme modern, perm
	
	local activitylist "24 29"
	foreach n of local activitylist {				   
		twoway (line number_firms_positive_emissions year if activity == `n', ///
					lcolor(black) lpattern(solid) yaxis(1)) ///
				   (line number_inst_positive_emissions year if activity == `n', ///
					lcolor(red) lpattern(solid) yaxis(2)), ///
				   title("") ///
				   xtitle("") ///
				   ytitle("Number of firms", axis(1)) ///
				   ytitle("Number of installations", axis(2) angle(180)) ///
				   xlabel(2005(5)2020) ///
				   legend(label(1 "Active firms") label(2 "Active installations"))
			   
		graph export "${output}/number_units_per_activity_`n'.png", as(png) replace
	}
	
	* table comparable to table A2 in Verde et al (2019)
	// "Installation entries and exits in the EU ETS"
	// (${dropbox}/carbon_policy_reallocation/literature)
	
	keep if year == 2005
	keep activity number_inst_positive_co2_2005_12
	
	
*------------------------------
* Analysis at the aggregate level
*------------------------------

	use "${int_data}/nace2_year_number_units.dta", clear
	
	collapse (sum) number_firms_positive_emissions number_inst_positive_emissions, by(year)
	
	* set style of graphs
	set scheme modern, perm
				   
	twoway (line number_firms_positive_emissions year, ///
				lcolor(black) lpattern(solid) yaxis(1)) ///
			   (line number_inst_positive_emissions year, ///
				lcolor(red) lpattern(solid) yaxis(2)), ///
			   title("") ///
			   xtitle("") ///
			   ytitle("Number of firms", axis(1)) ///
			   ytitle("Number of installations", axis(2) angle(180)) ///
			   xlabel(2005(5)2020) ///
			   legend(label(1 "Active firms") label(2 "Active installations"))
			   
	graph export "${output}/number_units_aggregate.png", as(png) replace
	
*------------------------------
* Comparison with Verde et al (2019)
*------------------------------

	import delimited "${raw_data}/EUTL/installation.csv", clear
	
	rename id installation_id
	
	// as a sanity check, I want to compare our data with info on tables from
	// Verde et al (2019) "Installation entries and exits in the EU ETS"
	// (${dropbox}/carbon_policy_reallocation/literature)
	// in particular, their table A2
	
	// update activity categories according to
	// EEA (2014) EU ETS data view user manual
	// (${dropbox}/carbon_policy_reallocation/manuals)
	replace activity_id = 20 if activity_id == 1
	replace activity_id = 21 if activity_id == 2
	replace activity_id = 22 if activity_id == 3
	replace activity_id = 23 if activity_id == 4
	replace activity_id = 24 if activity_id == 5
	replace activity_id = 29 if activity_id == 6
	replace activity_id = 31 if activity_id == 7
	replace activity_id = 32 if activity_id == 8
	replace activity_id = 36 if activity_id == 9
	
	drop if activity_id == 1000