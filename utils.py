import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

# *********************************************************************
# LOGGING - set of log messages
# *********************************************************************

log = logging.getLogger(__name__)

# *********************************************************************
# SETUP - of API KEY,header
# retry session , if error
# *********************************************************************

PAGE_SIZE = 1000
RETRIES = 3

session = requests.Session()
session.headers["Accept"] = "application/json"

MAX_RETRY = 5
MAX_RETRY_FOR_SESSION = 5
BACK_OFF_FACTOR = 1
TIME_BETWEEN_RETRIES = 1000
ERROR_CODES = (400, 401, 405, 500, 502, 503)

### Retry mechanism for server error ### https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library###
# {backoff factor} * (2 ** ({number of total retries} - 1))
retry_strategy = Retry(total=25, backoff_factor=1, status_forcelist=ERROR_CODES)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

# *********************************************************************
# FUNCTIONS -
# get login_jwt - get auth key & assign the requests to reponse using post method
# get_coa: get chart of accounts based on segment type

# assign the requests to reponse using get method
# append the list to coa
# In case, if error occurs retry
# *********************************************************************


# Generate jwt
def get_jwt(url: str, key: str, scopes: str, session: requests.Session = session) -> str:
    """Returns a jwt for authentication to the iPaaS APIs

    Args:
        url (str): LOGIN_URL= https://api.dartmouth.edu/api/jwt
        key (str): API_KEY

    Returns:
        _type_: str
    """

    headers = {"Authorization": key}

    if scopes:
        url = url + "?scope=" + scopes
    else:
        url = url

    response = session.post(url=url, headers=headers)

    if response.ok:
        response_json = response.json()
        jwt = response_json["jwt"]
    else:
        response_json = response.json()
        error = response_json["Failed to obtain a jwt"]
        raise Exception(error)

    return jwt

# Get_emp: access all employees
def get_emp(jwt: str, url: str, session: requests.Session = session) -> list[dict[str, Any]]:  # type: ignore
    """Returns all the employees from dart_api
    Args:
        jwt (str): JWT token from .env file
        url (str): URL of the API (e.g., https://api.dartmouth.edu/employees)
        session (requests.Session): Optional session for making requests
    Returns:
        List[Dict]: List of employee records
    """

    headers: dict = {"Authorization": "Bearer " + jwt, "Content-Type": "application/json"}
    page_number: int = 1
    emp = []
    prev_records = emp

    try:
        while True:  # Infinite loop, will break when no more data is returned or same as previously returned records
            emp_url = f"{url}?pagesize={PAGE_SIZE}&page={page_number}"

            response = session.get(url=emp_url, headers=headers)
            response.raise_for_status()  # Raise an exception for bad responses

            response_json = response.json()

            if response_json == prev_records:
                break

            emp += response_json
            prev_records = response_json
            current_page_number: int = page_number

            log.debug(f"Ending on loop {current_page_number}")
            log.debug(f"Records returned, so far: {len(emp)}")

            page_number += 1

    except requests.RequestException as e:
        log.error(f"Request exception: {e}")
        raise

    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        raise

    return emp

# # Get_emp: access all employees
# def get_emp(jwt: str, url: str, session: requests.Session = requests.Session()) -> list[dict[str, Any]]:
#     """Returns all the employees from dart_api
#     Args:
#         jwt (str): JWT token from .env file
#         url (str): URL of the API (e.g., https://api.dartmouth.edu/employees)
#         session (requests.Session): Optional session for making requests
#     Returns:
#         List[Dict]: List of employee records
#     """

#     headers: dict[str, str] = {"Authorization": "Bearer " + jwt, "Content-Type": "application/json"}
#     page_number: int = 1
#     emp = []
#     prev_records = None
#     total_records = float('inf')  # Initialize with infinity to ensure it's higher than any possible record count

#     try:
#         while len(emp) < total_records:
#             emp_url = f"{url}?pagesize={PAGE_SIZE}&page={page_number}"

#             response = session.get(url=emp_url, headers=headers)
#             response.raise_for_status()

#             response_json = response.json()
#             total_records = int(response.headers.get('x-count', 0))  # Get the total count of records

#             if not response_json:
#                 break

#             if response_json == prev_records:
#                 break

#             emp += response_json
#             prev_records = response_json.copy()
#             current_page_number: int = page_number

#             log.debug(f"Ending on loop {current_page_number}")
#             log.debug(f"Records returned, so far: {len(emp)}")

#             page_number += 1

#     except requests.RequestException as e:
#         log.error(f"Request exception: {e}")
#         raise

#     except Exception as e:
#         log.error(f"An unexpected error occurred: {e}")
#         raise

#     return emp




def get_active_crew_code(employee: dict) -> str:
    """Returns a single active crew code from dart_api
    # Convert to set to identify if there are >2 unique crew codes, if so raise Value error
    # Pop one non-unique element from the set
    """
    active_crew_codes = set()

    if employee["jobs"] is None:
        log.debug(f"Employee with netid '{employee['netid']}' has no jobs")
        return ""

    for job in employee.get("jobs", []):
        if "maintenance_crew" in job and job["maintenance_crew"]["crew_code"] is not None \
           and job["job_current_status"] == "Active":
            active_crew_codes.add(job["maintenance_crew"]["crew_code"])

    if len(set(active_crew_codes)) > 1:
        raise ValueError(f"Employee with netid '{employee['netid']}' has multiple active crew codes: {active_crew_codes}")

    active_crew_code = active_crew_codes.pop() if active_crew_codes else ""

    return active_crew_code




