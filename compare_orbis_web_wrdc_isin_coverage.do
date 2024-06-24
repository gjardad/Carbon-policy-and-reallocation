/*---------------------------------------------------------------------------------------------------
Assess the coverage of ORBIS  as compared to ORBIS WRDS in terms of giving us ISINs
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
* Prep ORBIS web data
*------------------------------

use "${raw_data}/ORBIS/orbis_web_identifying_info_eutl_firms.dta", clear 

keep bvdid SD_ISIN SD_TICKER 
ren (SD_ISIN SD_TICKER) (web_isin web_ticker)

duplicates drop 
isid bvdid 

tempfile web_orbis 
save `web_orbis'

*------------------------------
* Prep ORBIS WRDC data
*------------------------------

use "${raw_data}/ORBIS/orbis_eutl_firms.dta", clear 
g wrdc_listed = MAINEXCH!="Unlisted"


keep bvdid SD_ISIN SD_TICKER wrdc_listed MAINEXCH SLEGALF

ren (SD_ISIN SD_TICKER MAINEXCH SLEGALF) (wrdc_isin wrdc_ticker wrdc_mainexch wrdc_slegalf)

duplicates drop 
isid bvdid 

bys wrdc_slegalf: sum wrdc_listed 

tempfile wrdc_orbis 
save `wrdc_orbis'

*------------------------------
* Compare coverage of the two datasets
*------------------------------
use `wrdc_orbis', clear 
merge 1:m bvdid using `web_orbis', /// 
	assert(2 3)
	
g web_has_isin = !mi(web_isin)
g wrdc_has_isin = !mi(wrdc_isin)

tab web_has_isin wrdc_has_isin

tab wrdc_listed wrdc_has_isin

* We have ISINs for all firms listed on an exchange, but that is far from all public companies
assert wrdc_has_isin==0 if wrdc_listed==0
assert wrdc_isin==web_isin if !mi(wrdc_isin)
assert wrdc_mainexch=="Delisted" if wrdc_has_isin==0 & wrdc_listed==1

keep web_isin bvdid 
keep if !mi(web_isin)

save "${int_data}/bvdid_isin_crosswalk.dta", replace 

keep web_isin
export delimited "${int_data}/ets_listed_firms_isin.csv", replace
