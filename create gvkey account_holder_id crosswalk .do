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


	
import delimited "${raw_data}/EUTL/account_holder.csv", clear

*------------------------------
* Get the list of GV
*------------------------------

	
	use  "${raw_data}/Compustat - Global/from isin/compustat_global_ets_firms.dta", clear
	
	isid datadate gvkey
	keep gvkey isin
	duplicates drop 

	ren isin web_isin 
	
	merge 1:1 web_isin  using "${int_data}/bvdid_isin_crosswalk.dta", ///
		assert(2 3) /// we have some 2 because not all ISINs are in compustat-global 
		keep(3) ///
		nogen
	
	tempfile from_isin 
	save `from_isin'

* Read in the gvkeys from the syndicated loan data
import delimited "${int_data}/Syndicated Loans - gvkey_list crosswalk.csv", clear
ren accountholdername accountholder_name
compress
tempfile gvkey_list
save `gvkey_list'

* Ultimately, we only care about accounts/account holders that we can link to installations 
import delimited "${raw_data}/EUTL/account_holder.csv", clear
ren (name id) (accountholder_name accountholder_id)
keep accountholder*
tempfile acc_hold
save `acc_hold'

import delimited "${raw_data}/EUTL/account.csv", clear

drop if mi(installation_id)

assert !mi(accountholder_id)
g miss_bvdid = mi(bvdid)
g rand = runiform(0,1)
keep accountholder_id miss_bvdid bvdid rand

* Only keep 1 observation per account holder 
* Keep the one tied to a non-missing bvdid. 
* If there are multiple pick one randomly, this is very rare
bys accountholder_id (miss_bvdid rand): keep if _n==1
drop rand 
* Merge on the accountholder information
merge 1:1 accountholder_id using `acc_hold', /// 
	keep(3) /// 
	assert(2 3) ///
	nogen
	
* Change type of account holder name so we can merge on it
recast str190 accountholder_name


merge m:1 accountholder_name using `gvkey_list', ///
	keep(1 3) /// There are some _merge==2 (~81 this is something to look into)
	nogen 
	

merge m:1 bvdid using `from_isin', ///
	keep(1 3) /// There are some _merge=2 because of missing installations 
	nogen
	
destring gvkey, replace
	

g gvkeys_agree = gvkey_filled==gvkey if !mi(gvkey) & !mi(gvkey_filled)
sum gvkeys_agree 
drop gvkeys_agree
g gvkey_new = gvkey 
replace gvkey_new = gvkey_filled if mi(gvkey_new)

ren gvkey gvkey_through_isin 
ren gvkey_filled gvkey_through_syndloan
ren gvkey_new gvkey
count if !mi(gvkey) // We have gvkeys for about 1000 firms now. 

keep gvkey accountholder_id bvdid
save "${int_data}/accountholder_id_gvkey_crosswalk.dta", replace
