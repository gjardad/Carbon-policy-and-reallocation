/*******************************************************************************
Investigate where BvD id and installation_id missings come from

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
* Read in account and keep only installation_id and bvdid
*------------------------------

import delimited "${raw_data}/EUTL/account.csv", clear

g mi_bvdid = missing(bvdid)
sum mi_bvdid // About ~25% of accounts couldn't me merged to ORBIS

	// Let's investigate this further: how much of those missing ORBIS ids are from aircrafts?
	
	tempfile account
		
		g mi_id = missing(installation_id)
		sum mi_id // About ~30% of accounts do not have installation_id
		keep if !missing(installation_id)
		keep installation_id bvdid
		
	save "`account'"
	
*------------------------------
* Read in installation and merge
*------------------------------

	import delimited "${raw_data}/EUTL/installation.csv", clear
	
	rename id installation_id
	keep installation_id isaircraftoperator
	
	merge 1:m installation_id using "`account'"
	
	g mi_bvdid = missing(bvdid)
	
	tab mi_bvdid isaircraft, row // Among installations for which we have account info
								 // about ~70% of the ones with missing bvdid are NOT aircrafts