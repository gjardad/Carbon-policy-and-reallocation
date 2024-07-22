/*---------------------------------------------------------------------------------------------------
Create a compustat-based firm x year dataset. 
I think this code could be improved to handle accounts/installations/account holders more carefully

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
* Read in compustat exchange rate data
*------------------------------
use "${raw_data}/Compustat - Global/compustat_exchange_rates.dta", clear 
 
* Create a variable that converts from GBP to Euros
g exratd_GBP_to_EUR_temp = -999999
replace exratd_GBP_to_EUR_temp = exratd_fromGBP if curd=="EUR" 
gegen exratd_GBP_to_EUR = max(exratd_GBP_to_EUR_temp), by(datadate)


keep datadate curd exratd_GBP_to_EUR exratd_toGBP 

* For some reason we dont have the exchange rate for Romania or Turkey on Dec 31, 2000?? 
* We'll just use the last day we have for now
preserve 

	keep if month(datadate)==12 & year(datadate)==2000 & (curd=="ROL" | curd=="TRL")
	sort datadate 
	bys curd (datadate): keep if _n==_N
	
	replace datadate = mdy(12, 31, 2000)
	
	tempfile rol_trl_312000
	save `rol_trl_312000'
restore




append using `rol_trl_312000'

ren curd curcd
tempfile exch_rate 
save `exch_rate'

*------------------------------
* Read in firm-year-level data from compustat
*------------------------------

	tempfile compustat 
	
	use  "${raw_data}/Compustat - Global/from isin/compustat_global_ets_firms.dta", clear
	
	
	preserve 
		keep gvkey isin 
		duplicates drop 
		assert !mi(isin) & !mi(gvkey)
	
		tempfile gvkey_isin_xwalk 
		save `gvkey_isin_xwalk'
	restore
	isid datadate gvkey 
	
	tempfile from_isin 
	save `from_isin'
	
	use  "${raw_data}/Compustat - Global/from gvkey/compustat_global_ets_firms.dta", clear
	
	isid datadate gvkey 
	append using `from_isin'

	* Drop the duplicates
	bys datadate gvkey: keep if _n==1
	destring gvkey, replace 
	
	g year = year(datadate)

	keep if inrange(year, 2000, 2019)

	* Not sure what's going on with this one obs 
	count if mi(fyear)
	assert `r(N)'<=1 
	drop if mi(fyear)
	
	assert revt==sale if !mi(revt) & !mi(sale)
	g sales = sale 
	replace sales = revt if mi(sales)
	drop sale
	
	* There are a few firms for whom we're always missing sales 
	assert mi(sales) if inlist(gvkey, 282340, 360973, 361326) 
	drop if inlist(gvkey, 282340, 360973, 361326) 
	
	* Not 100% sure this is the right labor compensation variable. It's "Staff Expense - Total"
	ren xlr labor
	
	merge m:1 datadate curcd using `exch_rate', ///
		assert(2 3) ///
		keep(3) /// 
		nogen
		
		
		
	* Replace variables of interest to be in euros 
	g sales_eur = sales * exratd_toGBP * exratd_GBP_to_EUR 
	assert (sales - sales_eur) < 0.1 if curcd=="EUR"
	
	* For a handful of firms we have duplicates, not sure why XX
	bys gvkey year: keep if _n==1
	
	tempfile compustat 
	save `compustat'
*------------------------------
* Read in account (and bvdid) info
*------------------------------

	import delimited "${raw_data}/EUTL/account.csv", clear

	unique installation_id 
	keep if accounttype_id == "100-7" | accounttype_id == "100-9" | accounttype_id == "120-0"
	// only OHA, aircraft accounts, or former HA have an installation_id associated with it
	
	g mi_instid = missing(installation_id)
	*sum mi_instid // we dont know installation for about 6% of those accounts
	*tab accounttype_id if mi_instid == 1 // and they are all "120-0" accounts

	keep if !missing(installation_id)
	
	keep installation_id bvdid accounttype_id accountholder_id // obs uniquely identified by installation_id and accounttype_id
	
	bysort installation_id (bvdid): gen bvdid_consistent = bvdid[1] == bvdid[_N]
	*tab bvdid_consistent // most of obs who share installation_id also share bvdid, but not all
	
	drop if bvdid_consistent == 1 & accounttype_id == "120-0" // for those accounts, bvdid is unambiguous
	
	bysort installation_id (bvdid): gen bvdid_120 = bvdid[1] == "120-0"
	// there are no installation_id for which the only account type is a former account (this is reassuring!)
	
	drop if accounttype_id == "120-0" // FOR NOW, let's focus only on the 100-7.
	// then, installation_id uniquely identifies accounts
	
	drop bvdid_120 bvdid_consistent accounttype_id	
	
	
	merge m:1 accountholder_id using  "${int_data}/accountholder_id_gvkey_crosswalk.dta", ///
		assert(2 3) ///
		keep(3) ///
		nogen
		
	ren gvkey gvkey_accountholder_id
	
	merge m:1 bvdid using  "${int_data}/bvdid_isin_crosswalk.dta", ///
		assert(1 2 3) ///
		keep(1 3) ///
		nogen
	ren web_isin isin 
	
	merge m:1 isin using `gvkey_isin_xwalk',  ///
		keep(1 3) ///
		nogen 
		
	ren gvkey gvkey_isin 
	destring gvkey_isin, replace 
	
	* Use gvkey from the isin if possible, else use the one from the syndicated loan data
	g gvkey = gvkey_isin 
	replace gvkey = gvkey_accountholder_id if mi(gvkey)
	
	keep if !mi(gvkey)
	
	tempfile account 
	save `account'
	
	
*------------------------------
* Read in installation data and merge with account data
*------------------------------


	import delimited "${raw_data}/EUTL/installation.csv", clear
	

	rename id installation_id
	
	keep installation_id nace_id activity_id
	
	merge 1:1 installation_id using "`account'", ///
		assert(1 3) ///
		keep(3) ///
		nogen 
	drop if activity_id == 10 // we don't care about aircrafts

	isid installation_id 
	
tempfile installation_xwalk 
save `installation_xwalk'

keep gvkey 
duplicates drop 
tempfile gvkey_list 
save `gvkey_list'

		
*------------------------------
* Read in compliance info
*------------------------------

	preserve

		tempfile emissions

		import delimited "${raw_data}/EUTL/compliance.csv", clear
		
		keep if year <= 2019 // verified emissions go up to 2022
		
		keep installation_id year verified
		
		bysort installation_id (year): gen starts_2005 = year[1] == 2005
		drop if starts_2005 == 0 // this is a different type of account that we are not interested on (my guess is that this is country level info)
		
		bysort installation_id (year): gen mi_2005 = missing(verified[1])
		// a lot of those for which emissions are missing in 2005 are installations for which info starts in 2013
		// an year with big changes in EUETS' regulation
		
		duplicates tag installation_id year verified, generate(dup1)
		*tab dup1 // around 1.9% of installation-year observations;
		// mostly obs for which verified emissions are missing
		
		bysort installation_id year (dup1): drop if dup1 == 1 & _n == 2
		// drop one obs per duplicate if dup2==1
		
		duplicates tag installation_id year, generate(dup2)
		*tab dup2 // around 0.6% of installation-year observations; they all start 2020 onwards
		
		bysort installation_id year (verified): drop if dup2 == 1 & _n == 1
		// arbitrarily drop the obs within installation and year which has the lowest emissions
		
		keep installation_id year verified
		
		merge m:1 installation_id using `installation_xwalk', ///
			assert(1 3) ///
			keep(3) ///
			nogen
		
		gcollapse (sum) emissions= verified, ///
			by(year gvkey)
			
		set obs `=_N +1' 
		replace gvkey = 99999 if _N == _n & mi(gvkey)
		replace year = 2000 if _N == _n & mi(year)
		tsset gvkey year 
		tsfill, full
		drop if gvkey==99999
		save "`emissions'"
		
	restore
	

*------------------------------
* Merge in emissions info and collapse to the gvkey x year level
*------------------------------
	
use `compustat', clear 
tsset gvkey year 
tsfill, full

merge m:1 gvkey using `gvkey_list', /// 
	assert(1 2 3) /// some _merge =2 because of firms not in compustat, merge=1 because of sample restrictions
	keep(3) ///
	nogen 
	
merge 1:1 gvkey year using `emissions', /// 
	assert(2 3) /// 
	keep(3) ///
	nogen 

e
	
save "${int_data}/compustat firm_year.dta", replace
		
		
		
		