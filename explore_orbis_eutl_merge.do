/*******************************************************************************
Read-in the EUTL accounts data, save the BvD id number and export it as a csv 
to be used in conjunction with orbis data pull 

CW
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
* Check merge rates with ORBIS
*------------------------------
import delimited "${int_data}/EUTL_bvdids.txt", clear varnames(1)

merge 1:m bvdid using "${raw_data}/ORBIS/orbis_eutl_firms.dta", ///
	assert(1 3)

* Merge rate is about 90% so we're left with ~10,000 firms
preserve 
	bys bvdid: keep if _n==1 
	g merged = _merge==3 
	sum merged 
restore

