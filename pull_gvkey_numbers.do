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
* Read in account numbers and export as csv
*------------------------------

use gvkey_fill AccountHolderName using "${raw_data}/Syndicated Loans Replication Data/Syndicated loans_Chris_Feb2020_reduc_Permits.dta", clear
drop if mi(gvkey_fill)
drop if mi(AccountHolderName)
duplicates drop

export delimited "${int_data}/Syndicated Loans - gvkey_list crosswalk.csv", replace

drop AccountHolderName
export delimited "${int_data}/Syndicated Loans - gvkey_list.csv", replace
