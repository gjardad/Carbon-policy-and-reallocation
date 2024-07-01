/*---------------------------------------------------------------------------------------------------
Explore Compustat firms
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

* For some reason we dont have the exchange rate for Romania on Dec 31, 2000?? 
* We'll just use the last day we have for now
preserve 


	keep if month(datadate)==12 & year(datadate)==2000 & curd=="ROL"
	sort datadate 
	keep if _n==_N
	
	replace datadate = mdy(12, 31, 2000)
	
	tempfile rol312000
	save `rol312000'
restore


append using `rol312000'

ren curd curcd
tempfile exch_rate 
save `exch_rate'

*------------------------------
* Read in firm-year-level data from compustat
*------------------------------

	tempfile compustat 
	
	use  "${raw_data}/Compustat - Global/compustat_global_ets_listed_firms.dta", clear

	g year = year(datadate)

	keep if inrange(year, 2000, 2019)

	
	assert revt==sale if !mi(revt) & !mi(sale)
	g sales = sale 
	replace sales = revt if mi(sales)
	drop sale
	
	* There are a few firms for whom we're always missing sales 
	assert mi(sales) if inlist(isin, "RORMARACNOR1", "ROMOBGACNOR9", "ROHEALACNOR2")
	drop if inlist(isin, "RORMARACNOR1", "ROMOBGACNOR9", "ROHEALACNOR2") 
	
	* Not 100% sure this is the right labor compensation variable. It's "Staff Expense - Total"
	ren xlr labor
	
	merge m:1 datadate curcd using `exch_rate', ///
		assert(2 3) ///
		keep(3) /// 
		nogen
		
	* Replace variables of interest to be in euros 
	g sales_eur = sales * exratd_toGBP * exratd_GBP_to_EUR 
	assert (sales - sales_eur) < 0.01 if curcd=="EUR"
e
g has_output = !mi(sale)
g has_profit = !mi(dbtb)

gcollapse (sum) has_output has_dbtb, by(data_yr)

tw ///
	(line has_output data_yr)
	
	
tw ///
	(line has_dbtb data_yr)
