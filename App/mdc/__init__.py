import requests
import pandas as pd
from pandas import json_normalize

base_api_url = 'https://mdc-contextual-service-prod.run.aws-usw02-pr.ice.predix.io/bac-details-service/v2/report/'
base_api_error_string = 'Error in API Call.\nURL Attempted: {}\nResponse Code: {}\nServer Reason: {}'


class ApiError(Exception):
    """ Raised when API response is not 200 (OK)"""
    pass


def get_plant_model(
        headers: dict) -> pd.DataFrame:  # Gets all Enterprises, Enterprise IDs, Plants, and PlantIDs in the P&G Tenant model
    """
    Get names and ids of all plants in P&G and their owning business units- useful for feeding additional calls

    :param headers: Tenant ID and Token

    :return: DataFrame with company plant model if API successful, else none.
    """
    url = base_api_url + 'plantmodel/owningBusinessUnits'

    res = requests.get(url,
                       headers=headers)  # returns nested json starting at 'PG' tenant, and drills down to site level with metadata.
    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res_status == 200:
            res = res.json()
            flat = json_normalize(res['enterprises'], ['owningBusinessUnits', 'sites'],
                                  [['owningBusinessUnits',
                                    'name']])  # Flattening to site level and adding back enterprise metadata.
            return flat
        else:
            raise ApiError
    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return


def get_line_model(headers: dict, site: str) -> pd.DataFrame:
    """
    Get Lines by Site and Area.

    :param headers: Dict of API headers, (Tenant ID + Token)
    :param site: Site Name e.g. 'St.Louis' as appears in get_plant_model(), for multiple use comma separated list

    :return: Dataframe if API Call successful, else None
    """

    url = base_api_url + 'plantmodel/owningBusinessUnits/lines?site={}'.format(site)

    # returns nested json starting at 'PG' tenant, and drills down to site level with metadata.
    res = requests.get(url, headers=headers)
    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res_status == 200:
            res = res.json()
            flat = json_normalize(res['enterprises'], ['owningBusinessUnits', 'sites', 'areas', 'lines'],
                                  [['owningBusinessUnits', 'name'],
                                   ['owningBusinessUnits', 'sites', 'name'],
                                   ['owningBusinessUnits', 'sites', 'areas', 'name']])  # flattening to Line Level
            return flat
        else:
            raise ApiError
    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return


def get_equipment_model(headers: dict, site: str, **kwargs: str) -> pd.DataFrame:
    """
    Get equipment (units) by Line, Area, Site, and Business Unit

    :param headers: Dict of API headers, (Tenant ID + Token)
    :param site: Site Name e.g. 'St.Louis' as appears in get_plant_model(), for multiple use comma separated list
    :param **operatingBusinessUnits: Business Unit Name as appears in get_plant_model(), for multiple use comma separated list
    :param **area: Area Name as appears in get_line_model(), for multiple use comma separated list
    :param **line: Line Name as appears in get_line_model(), for multiple use comma separated list

    :return: Dataframe if API Call successful, else None
    """

    url = base_api_url + 'plantmodel/operatingBusinessUnits?'

    params = {'site': site}

    for key, value in kwargs.items():
        params[key] = value

    # returns nested json starting at 'PG' tenant, and drills down to site level with metadata.
    res = requests.get(url, headers=headers, params=params)
    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res_status == 200:
            res = res.json()
            flat = json_normalize(res['enterprises'], ['owningBusinessUnits', 'sites', 'areas', 'lines', 'units'],
                                  [['owningBusinessUnits', 'name'],
                                   ['owningBusinessUnits', 'sites', 'name'],
                                   ['owningBusinessUnits', 'sites', 'areas', 'name'],
                                   ['owningBusinessUnits', 'sites', 'areas', 'lines',
                                    'name']])  # flattening to unit Level
            return flat
        else:
            raise ApiError
    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return


def get_brandcodes(headers: dict, site: str) -> pd.DataFrame:
    """
    get brandcodes available at a site.

    :param headers: Dict of API headers, (Tenant ID + Token)
    :param site: Site Name e.g. 'St.Louis' as appears in get_plant_model(), for multiple use comma separated list

    :return: Dataframe if API Call successful, else None
    """

    url = base_api_url + 'products?site={}'.format(site)

    res = requests.get(url,
                       headers=headers)  # returns nested json starting at 'PG' tenant, and drills down to site level with metadata.
    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res.status_code == 200:
            res = res.json()
            flat = json_normalize(res)
            return flat
        else:
            raise ApiError
    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return


def get_raw_data(headers: dict, table: str, site: str, startDate: str, **kwargs: str) -> pd.DataFrame:
    """
    Get all Production Events over a time frame

    :param headers: Dict of API Headers (Tenant ID + Auth Token)
    :param table: pe, dt, po <- pe = production event, de = downtime event, po = process order
    :param site: Site Name e.g. 'St.Louis' as appears in get_plant_model(), for multiple use comma separated list
    :param startDate: Start Time in format 'YYYY-MM-DD HH:MM:SS'
    :param endDate: (optional) End Time in format 'YYYY-MM-DD HH:MM:SS, if not defined, uses current date & time'
    :param line: Line Name as appears in get_line_model(), for multiple use comma separated list
    :param area: Area Name as appears in get_line_model(), for multiple use comma separated list
    :param unit: Unit Name as appears in get_equipment_model(), for multiple use comma separated list

    :return: DataFrame or None if API Error
    """

    table_switcher = {
        'pe': 'productionEventsReports?',
        'dt': 'downtimeReports?',
        'po': 'processOrders??'
    }

    url = base_api_url + table_switcher.get(table.lower(), "ERROR_INCORRECT_TABLE PLEASE USE pe,dt,po")

    params = {'site': site,
              'startDate': startDate,
              'rowSize': '9999'
              }

    for key, value in kwargs.items():
        params[key] = value

    res = requests.get(url, headers=headers, params=params)
    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res_status == 200:
            res = res.json()

            # Method to page through the MDC queries. default records is 10000. if more than 10000 records returned, go to next
            if 'next' in res['_links'].keys():
                next_page_link = res['_links']['next']['href']
            else:
                next_page_link = None

            flat = json_normalize(res['_embedded'], ['reports'])

            # we don't know how many pages there will be for large queries, so need to go until there are no more.
            # testing has indicated that if there are fewer than 1000 records on the page, there is no 'next' field returned.
            while next_page_link is not None:
                next_res = requests.get(next_page_link, headers=headers).json()
                next_flat = json_normalize(next_res['_embedded'], ['reports'])

                if 'next' in next_res['_links'].keys():
                    next_page_link = next_res['_links']['next']['href']
                else:
                    next_page_link = None

                flat = flat.append(next_flat)
            return flat
        else:
            raise ApiError
    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return


def get_token(auth_uaa: str, token_headers: dict, auth_api_user_name: str, auth_api_user_password: str,
              auth_pmdc_client_id: str, auth_pmdc_client_secret: str) -> str:
    """
    Uses P&G Auth Information to generate Authentication token for subsequent API Call Headers.

    :param auth_uaa:
    :param token_headers: {'Authorization','Postman-Token','cache-control','content-type'}
    :param auth_api_user_name:
    :param auth_api_user_password:
    :param auth_pmdc_client_id:
    :param auth_pmdc_client_secret:
    :return: token (str)
    """

    url = 'https://' + auth_uaa + '.predix-uaa.run.aws-usw02-pr.ice.predix.io/oauth/token'

    token_body = {'grant_type': 'password', 'username': auth_api_user_name, 'password': auth_api_user_password}

    res = requests.post(url, data=token_body, headers=token_headers,
                        auth=(auth_pmdc_client_id, auth_pmdc_client_secret))

    res_status = res.status_code
    res_reason = res.reason
    res_url = res.url

    try:
        if res_status == 200:
            token = res.json()
            return token['access_token']
        else:
            raise ApiError

    except ApiError:
        print(base_api_error_string.format(res_url, res_status, res_reason))
        return None
