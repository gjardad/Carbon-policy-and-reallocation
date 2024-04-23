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

global raw_data "${dropbox}/carbon_policy_reallocation/data/raw"
global int_data "${dropbox}/carbon_policy_reallocation/data/intermediate"
global proc_data "${dropbox}/carbon_policy_reallocation/data/processed"

*------------------------------
* Read in account numbers and export as csv
*------------------------------

import delimited "${raw_data}/EUTL/account.csv", clear

g mi_bvdid = missing(bvdid)
sum mi_bvdid // About ~25% of accounts couldn't me merged to ORBIS

keep if !missing(bvdid)

keep bvdid 
duplicates drop

// Left with about ~11,000 firms which lines up with what I've seen around the size of the EUTS 
// Should read documentation to figure out why there are more obs in this data, maybe because it's 
// at the account and not firm level?
export delimited "${int_data}/EUTL_bvdids.txt", replace
