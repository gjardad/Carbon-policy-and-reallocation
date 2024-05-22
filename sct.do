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
					 
	Scale                                           = Y_t sum_{i} theta_i,2007 sum_{f} e_f,2007 alpha_fi,2007
	Scale + Industry composition                    = Y_t sum_{i} theta_it sum_{f} e_f,2007 alpha_fi,2007
	Scale + Industry composition + Firm composition = Y_t sum_{i} theta_it sum_{f} e_f,2007 alpha_fit
	SCT                                             = Y_t sum_{i} theta_it sum_{f} e_ft alpha_fit = Z_t

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
* SCT at the firm level
*------------------------------

*------------------------------
* SCT at the industry level
*------------------------------