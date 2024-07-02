/*---------------------------------------------------------------------------------------------------
Explore Syndicated loan data shared by
---------------------------------------------------------------------------------------------------*/

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
* Understand data
*------------------------------
use "${raw_data}/Syndicated Loans Replication Data/Syndicated loans_Chris_Feb2020_reduc_Permits.dta", clear

* GVKEY is a subset of GVKEY_Filled
assert !mi(gvkey_filled) if !mi(gvkey)
assert gvkey == gvkey_filled if !mi(gvkey)
drop gvkey 

* We've got ~15,000 firms total, but this is both control and ETS
gunique gvkey_filled

gunique gvkey_filled if !mi(AccountHolderName)


* These are the firms I think ever appear in ETS
g in_ETS = !mi(AccountHolderName)
assert in_ETS==1 if !mi(ETSPhase)
assert !mi(ETSPhase) if in_ETS==1

assert in_ETS==1 if !mi(dum_treated)
assert !mi(dum_treated) if in_ETS==1

* Only about 500 firms in ETS
unique gvkey_filled if in_ETS==1


* To get the sample they use in the paper I think you make the restriction 
* _merge==3, not sure what merge this is from though. 
