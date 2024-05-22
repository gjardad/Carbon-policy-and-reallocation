/*******************************************************************************
Scale, Composition, Technology (SCT) decomposition at the industry and firm level

Notation:
	
	Z_t       := aggregate emissions in year t
	z_it      := industry-level emissions in year t
	z_ft      := firm-level emissions in year t
	e_ft      := firm-level emission intesity in year t
	Y_t       := aggregate output in year t
	y_it      := industry-level output in year t
	y_ft      := firm-level output in year t
	theta_it  := industry share of agg. output in year t
	alpha_fit := firm share of industry output in year t
	
	sum_{j}   := sum over elements indexed by j (as in latex)
	
By definition, Z_t = sum_{i} z_it = sum_{i} sum_{f} z_ft

Re-write this as Z_t = sum_{i} sum_{f} e_ft y_ft = sum_{i} y_it sum_{f} e_ft alpha_fit
					 = Y_t sum_{i} theta_it sum_{f} e_ft alpha_fit
					 
	Scale                                           = Y_t sum_{i} theta_i,2008 sum_{f} e_f,2008 alpha_fi,2008
	Scale + Industry composition                    = Y_t sum_{i} theta_it sum_{f} e_f,2008 alpha_fi,2008
	Scale + Industry composition + Firm composition = Y_t sum_{i} theta_it sum_{f} e_f,2008 alpha_fit
	SCT                                             = Y_t sum_{i} theta_it sum_{f} e_ft alpha_fit = Z_t

TO-DO/Obs:
1. how to deal with missing information?

	there are two types of missing information here.
	missing information from ORBIS (output)
	mising information from EUETS (emissions)

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
	
	drop activity nace_orbis capital labor
	
	// there are missing information in emissions and missing info in sales
	
	// 5 possible patters for emissions data:
		// 1. zeo throughout
		// 2. > 0 throughout
		// 3. starts with 0 and then becomes > 0
		// 4. starts with > 0 and then becomes 0
		// 5. zero randomly spread out
		
	// identify each case
	bysort bvdid (year): g zero_first = (co2[1] == 0)
	bysort bvdid (co2): g anyzero = (co2[1] == 0)
	bysort bvdid (year): g zero_last = (co2[_N] == 0) 
*------------------------------
* Create time t objects defined in preamble
*------------------------------

*------------------------------
* Create time 2008 objects defined in preamble
*------------------------------

	* note: not using 2007-2007 because a lot of missing data (this is due to some
	* institutional features of the market during phase I)
	
	

*------------------------------
* SCT at the firm level
*------------------------------

*------------------------------
* SCT at the industry level
*------------------------------


	drop if year < 2008