/*---------------------------------------------------------------------------------------------------
Get lisdt
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
* Get 
*------------------------------
loc by_vars year 
use "${int_data}/installation_year_emissions.dta", clear
drop if mi(bvdid)
keep if year<2020 

ren verified emissions 
drop if emissions ==0

gcollapse (sum) emissions, by(`by_vars')

tempfile emissions 
save `emissions'

use "${int_data}/installation_year_emissions.dta", clear
drop if mi(bvdid)
keep if year<2020 

ren verified emissions 
drop if emissions ==0
bys installation_id `by_vars': keep if _n==1 
g N_installation = 1
gcollapse (sum) N_installation, by(`by_vars')

tempfile instals 
save `instals'

use "${int_data}/installation_year_emissions.dta", clear
drop if mi(bvdid)
keep if year<2020 

ren verified emissions 
drop if emissions ==0
bys bvdid `by_vars': keep if _n==1 
g N_firms = 1
gcollapse (sum) N_firms, by(`by_vars')

tempfile firms 
save `firms'

clear 

use `emissions', clear 
merge 1:1 `by_vars' using `instals', assert(3) nogen
merge 1:1 `by_vars' using `firms', assert(3) nogen

tw /// 
	(line emissions year, sort)
