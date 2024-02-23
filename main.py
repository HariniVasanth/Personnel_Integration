import os
import sys
import time
import logging
import json

import requests

import planon
from planon import Person

from ipaas import utils
from typing import List, Dict

# *********************************************************************
# LOGGING
# *********************************************************************

log_level = os.environ.get("LOG_LEVEL", "INFO")
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(stream=sys.stdout, level=log_level, format=log_format)

# Set the log to use GMT time zone
logging.Formatter.converter = time.gmtime

# Add milliseconds
logging.Formatter.default_msec_format = "%s.%03d"

log = logging.getLogger(__name__)

# *********************************************************************
# SETUP
# *********************************************************************

planon.PlanonResource.set_site(site=os.environ["PLANON_API_URL"])
planon.PlanonResource.set_header(jwt=os.environ["PLANON_API_KEY"])

# Planon API
PLANON_API_URL = os.environ["PLANON_API_URL"]
PLANON_API_KEY = os.environ["PLANON_API_KEY"]
log.debug(f"{PLANON_API_URL}")

# Dartmouth iPaaS
DARTMOUTH_API_URL = os.environ["DARTMOUTH_API_URL"]
DARTMOUTH_API_KEY = os.environ["DARTMOUTH_API_KEY"]
log.debug(f"{DARTMOUTH_API_URL}")

headers = {"Authorization": DARTMOUTH_API_KEY}
scopes = "urn:dartmouth:employees:read.sensitive"

# ***********************************************************************
# SOURCE DARTMOUTH DATA
# ***********************************************************************

# Load the crew codes from a separate JSON file
with open('crew_codes_to_exclude.json', 'r') as f:
    excluded_crew_codes = json.load(f)
    
dart_jwt = utils.get_jwt(url=f"{DARTMOUTH_API_URL}/api/jwt", key=DARTMOUTH_API_KEY, scopes=scopes, session=requests.Session())

log.info("Getting Dart employees with iPass from HRMS")
dart_employees = {dc_emp["netid"]: dc_emp for dc_emp in utils.get_emp(jwt=dart_jwt, url=f"{DARTMOUTH_API_URL}/api/employees", session=requests.Session())}
log.info(f"Total number of dart_employees: {len(dart_employees)}")

dart_emp_keys = dart_employees.keys()
len(dart_emp_keys)
len(set(dart_emp_keys))

# # #TODO - manage a stand alone file  for crew codes that needs to be excluded - Json format maybe - DONE
# #     # sync them but with empty trade
# # log.info("Getting Dart employees with iPass from HRMS") # excluding crew codes from json file
# # dart_employees = {
# #     dc_emp["netid"]: dc_emp 
# #     for dc_emp in utils.get_emp(jwt=dart_jwt, url=f"{DARTMOUTH_API_URL}/api/employees", session=requests.Session()) 
# #     if dc_emp.get('jobs') and utils.get_active_crew_code(dc_emp) not in excluded_crew_codes
# }
# log.info(f"Total number of dart_employees: {len(dart_employees)}")


# ***********************************************************************
# SOURCE PLANON DATA
# ***********************************************************************

#TRADES
log.info("Getting Planon trades")
pln_trades = planon.Trade.find()

pln_trades_by_syscodes = {trade.Syscode: trade for trade in pln_trades}
log.debug(f"{pln_trades_by_syscodes.keys()=}")

pln_trades_by_codes = {trade.Code: trade for trade in pln_trades}
log.debug(f"{pln_trades_by_codes.keys()=}")

pln_trades_by_codes['HLS']

log.info(f"Total number of Planon trades: {len(pln_trades_by_codes)}")

#LABOR_GROUPS
log.info("Getting Planon labor rates")
pln_laborgroups = planon.WorkingHoursTariffGroup.find()

for pln_laborgroup in pln_laborgroups:
    assert pln_laborgroup.Code is not None, f"Code is None for {pln_laborgroup}"

pln_laborgroups_by_syscode = {laborgroup.Syscode: laborgroup for laborgroup in pln_laborgroups}
log.debug(f"pln_laborgroup{pln_laborgroups_by_syscode.keys()=}")

pln_laborgroups_by_codes = {laborgroup.Code: laborgroup for laborgroup in pln_laborgroups if pln_laborgroup.Code is not None} 
log.debug(f"pln_laborgroup{pln_laborgroups_by_codes.keys()=}")

pln_laborgroups_by_codes['HLS']

log.info(f"Total number of Planon labor rates: {len(pln_laborgroups)}")

#PERSONS
log.info("Getting Planon persons")
pln_persons: Dict[str,Person] = {pln_person.NetID: pln_person for pln_person in planon.Person.find() if pln_person.NetID is not None}
for pln_person in pln_persons.values():
    assert pln_person.NetID is not None, f"NetID is None for {pln_person}"

log.info(f"Total number of planon persons for updates: {len(pln_persons)}")

# ****************************************************************************************************************
# MAIN - UPDATES
# ****************************************************************************************************************
# ****************************************************************************************************************
# ===========================================================================================
# UDPATES  for trade and labor group that has changes for techs
# ===========================================================================================
log.info("Starting trade and labor group feed to Planon for UPDATES")

dart_employees_inserts = {dc_emp["netid"]: dc_emp for dc_emp in utils.get_emp(jwt=dart_jwt, url=f"{DARTMOUTH_API_URL}/api/employees", session=requests.Session()) if dc_emp["netid"] == "f007c04"} #d12225j # f002870

pln_filter_inserts = {
    "filter": {
        # "EmploymenttypeRef": {"eq": "8"},  # Personnel>Employment types> 5 = Staff   #Supervisior:Internal Coordinator-'12';Internal
        # "FreeString7": {"exists": True}, # NetID, this ensures we only get Person records that have a NetID
        "FreeString7": {"eq": "f007c04"},  # d12225j-null #f003841-null #d35444l-57  #d20171b-HLS #f002870-ML  #d17283s-CEOPS  #f0033wq-ST(HLS,GSGO) #d28941t-TS,TS  #d10918g-JD
        # "FreeString2": {"eq": "Active"},  # dartmouth account status
        "IsArchived": {"eq": False},
        # "FreeInteger2": {'exists':False}, # Trade
        # "WorkingHoursTariffGroupRef": {"exists": False},
        # "FirstName":{"eq": "Jason"} #remove
    }
}
pln_persons_inserts = {pln_emp.NetID: pln_emp for pln_emp in planon.Person.find(pln_filter_inserts)}
log.info(f"Total number of planon_employees for INSERTS : {str(len(pln_persons_inserts))}")

updated_netids = []
skipped_netids = []
failed_netids = []
no_match_netids= []

for dart_employee in dart_employees.values():
    log.debug(f"Processing {dart_employee['netid']}")

    try:
        
        if dart_employee['netid'] not in pln_persons: #ENV related , in Prod it will run after People sync , so you would not fins any not matching Planon person
            log.debug(f"Record {dart_employee['netid']} not found in Planon")
            no_match_netids.append(dart_employee['netid'])
            continue  #exit loop and move to next iteration

        pln_person = pln_persons[dart_employee['netid']]

        active_crew_code = utils.get_active_crew_code(dart_employee)

        if active_crew_code in excluded_crew_codes:
            active_crew_code =''
        else:
            active_crew_code = active_crew_code 

        #check planon person_trade to syscode equivalent    
        pln_person_trade = pln_trades_by_syscodes.get(pln_person.TradeRef) if pln_person.TradeRef else ""
        pln_person_laborgroup = pln_laborgroups_by_syscode.get(pln_person.WorkingHoursTariffGroupRef) if pln_person.WorkingHoursTariffGroupRef else ""

        if not pln_person_trade and not pln_person_laborgroup and not active_crew_code :
            log.debug(f"Record {pln_person.NetID} skipped, already has the correct trade & labor group for {active_crew_code}")
            skipped_netids.append(pln_person.NetID)
            continue # exit loop and move to next iteration
             
        # Convert planon employee's crew code to similar to IPaas format
        person_ipaas = {
            "trade": active_crew_code,
            "labor_group": active_crew_code,
        }

        person_pln = {
            "trade": pln_person_trade.Code if pln_person_trade else "",
            "labor_group": pln_person_laborgroup.Code if pln_person_laborgroup else "",
        }

        #TODO- eliminate get on trades
        #TODO- compare empry string for ml, ceops when it get to dict -DONE
        
        # pln_trade = pln_trades_by_codes.get(active_crew_code) #without get for missing crew codes keyerror     
        # pln_laborgroup = pln_laborgroups_codes.get(active_crew_code)
        
        #Keyerror if None - handle it - Done
        pln_trade = pln_trades_by_codes[active_crew_code] if active_crew_code else "" 
        pln_laborgroup = pln_laborgroups_by_codes[active_crew_code]  if active_crew_code else ""
         
        #TODO - make missing_crew_code under failed _netids -DONE
        #TODO - ML,CEOPS(excluded crew codes) should have empty crew codes when  it is being comapred to Planon - DONE
        # UPDATES to trade and labor group:
        if person_ipaas!= person_pln:
            log.debug(f"Syncing {pln_person.NetID}")
            pln_person.WorkingHoursTariffGroupRef = pln_laborgroup.Syscode if pln_laborgroup else None
            pln_person.TradeRef = pln_trade.Syscode if pln_trade else None
            pln_person = pln_person.save()
            updated_netids.append(pln_person.NetID)
        else:
            log.debug(f"Record {pln_person.NetID} skipped, already has the correct trade & labor group for {active_crew_code}")
            skipped_netids.append(pln_person.NetID)
                    
    except Exception as ex:
            log.info(f"Failed to update {dart_employee['netid']} due to {ex}")       
            failed_netids.append(dart_employee['netid'])
         
log.info(f"Total number of successful trade and labor group updates: {len(updated_netids)}")
log.info(f"Total number of skipped employees, who have correct crew in Planon: {len(skipped_netids)}")
log.info(f"Total number of failures : {len(failed_netids)}")
log.info(f"Total number of failures : {len(no_match_netids)}")

# ========================================================================================================================================== #

log.info(
    f"""Logging results\n
# ======================= RESULTS ======================= #

UPDATED:
Employees updated with trade and labor group: {len(updated_netids)} {updated_netids} \n

SKIPPED:
Employees skipped : {len(skipped_netids)} \n

FAILED:
Employees failed updating: {len(failed_netids)} {failed_netids}\n

FAILED:
Employees with no matching Planon person: {len(no_match_netids)} {no_match_netids}\n

"""
)
