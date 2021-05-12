import requests
from dotenv import load_dotenv
import os
import json
#import psycopg2
import pyodbc
from datetime import datetime

from pandas import json_normalize

import App.etl.overall_etl
import App.mdc as mdc
import sqlalchemy as sa
import pandas as pd

from App.utils.encrypt import get_secret
from App.utils.sp_auth import get_sharepoint_token
from App.utils.time_utils import get_analysis_time_bounds

from App.utils.email_util import send_email
import logging
import urllib

# Configuring Logging file
logging.basicConfig(filename='logs/log.txt', format='%(asctime)s %(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S', filemode='a', level=logging.DEBUG)

# Load Environment Variables - .env file must be in gitignore and act as secret in github in order to commit.
load_dotenv()
uaa = os.getenv('auth_uaa')
api_user_name = os.getenv('auth_api_user_name')
pmdc_client_id = os.getenv('auth_pmdc_client_id')
tenant_id = os.getenv('tenant_id')
token_authorization = os.getenv('token_authorization')
token_postman_token = os.getenv('token_postman_token')
token_cache_control = os.getenv('token_cache_control')
token_content_type = os.getenv('token_content-type')

az_client_id = os.getenv('az_client_id')
az_scope = ['Sites.Manage.All', 'User.ReadBasic.All']
az_authority_url = os.getenv('az_authority_url')
az_endpoint = os.getenv('az_endpoint')
sp_hostname = os.getenv('sp_hostname')
sp_site_name = os.getenv('sp_sitename')
sp_site_id = os.getenv('sp_site_id')
site_config_list_id = os.getenv('site_config_list_id')
line_config_list_id = os.getenv('line_config_list_id')

enc_key = os.getenv('enc_key')

# Secrets Management - uses encryption key in encryptsecret.py
db_encrypted_pass = 'gAAAAABfd40TN3utbEFtRM3iWkT_B_9chrnbeyjlVmUcJQ9cVwj9GxxJD4WJ6_owhDSeTwm876poLINAq_' \
                    '-5WpM9g2zreX08Cg== '
sp_encrypted_secret = 'gAAAAABfbhGLdwoa27c9ggnz3YSgAj70VqIXd7ILJCmyC7gg4dKxa' \
                      '-p7JKgQ199T5RdBY4x5hUiDjW9HgyHhTHmdsATX6ihaOUN_mc8bUlWmMtznmzxqNjAoF1eRX65C0U25_8TR8f5O'
pmdc_encrypted_secret = 'gAAAAABfbjwu19DZHy3SPp3mDYm65j1XOTpgB6fNUrxbWnQ1jWnLezAe1R4f' \
                        '-OPcri0GM5GX1VhfrhwpJNiFA36gbqAnfyMnE1Hl--z09Le3-ya5QO92T1o='
api_encrypted_password = 'gAAAAABfbjywlyoToaQRbxz1CaGEBD' \
                         '-yw1zEEoBTOsP61FM4xjOCz1iuNkQlDGmxY5jTWyojCMyvAoz2d7MKTAp0Qs5za7J1ZA=='


# Function to easily drop prefixes from Sharepoint column names
def drop_prefix(self, prefix):
    self.columns = self.columns.str.lstrip(prefix)
    return self


pd.core.frame.DataFrame.drop_prefix = drop_prefix


def run_rco_analysis(site_cfg, line_cfg, db_connection):
    """
    This function acts in the same manner as the site R scripts and the .bat files which call them from the server.
    instead of having these parameters hard-coded in an R script, the data is located in a sharepoint table.

    1. Create a log dictionary object to track completion / success / failure of each individual site analysis
        (new functionality in python, not from the R script)

    2. from the site-config sharepoint table, iterate over the list of sites

    3. Get Time Boundaries for analysis from the data already existing in the destination database (we want to
        overlap our analysis by a day or two from the last available data point to ensure any updates made by the sites
        are captured)

        This snippet is located in Overall ETL in the R script, but there's no reason it needs to be down that far
        in the logic tree.

    4. Call the Overall ETL script with respect to the current iteration of the site configuration

    5. paste the success or failure of that operation to the log.
    """
    etl_log = {}
    for index, row in site_cfg.iterrows():
        # Changing Column names (sharepoint only keeps initial column name even if you rename later) + bundling so
        # I don't have to pass a million parameters through the functions
        site_params = {
            'SiteServer': row['Server'],
            'SiteMDCName': row['MDC_Site_Name'],
            'MachineLevel': (True if row['Run_Machine_Level_Analysis'] == 'Yes' else False),
            'FirstStop': (True if row['Run_First_Stop_After_CO_Analysis'] == 'Yes' else False),
            'MultiConstraint': (True if row['Run_Multi_Constraint_Analysis'] == 'Yes' else False),
            'SplitCOsOnCause': (True if row['Split_COs_based_on_Cause_Model'] == 'Yes' else False),
            'SUDSpecific': (True if row['SUD_specific_RCO_script'] == 'Yes' else False),
            'COTrigger': row['CO_Trigger_Parameter'],
            'querySL': row['querySL'],
            'queryML': row['queryML']
        }

        # pass only line params related to the site we're analyzing
        line_params = line_cfg[line_cfg['SiteNameLookupId'].astype(int) == (index + 1)]

        # call the actual ETL script. Returns string 'Success' or 'Failure (Reason) which is why it returns to Log.
        etl_log[row['Server']] = App.etl.overall_etl.site_server_overall_etl(mdc_api_headers,
                                                                             site_params,
                                                                             line_params,
                                                                             db_connection
                                                                             )
        # returns T/F if ETL succeeded
    return etl_log


def get_sharepoint_config(sharepoint_token):
    if sharepoint_token:
        az_headers = {'Authorization': 'Bearer ' + sharepoint_token}

        # Test request to Sharepoint to ensure graph api is working properly
        me_result = requests.get(
            f'{az_endpoint}/me',
            headers=az_headers
        ).json()

        # print results so we can observe what's happening in the console
        print('Results: %s' % json.dumps(me_result, indent=2))

        # ask Sharepoint to return the contents of our configuration lists - returns as json object
        site_config_json = requests.get(
            f'{az_endpoint}/sites/{sp_site_id}/lists/{site_config_list_id}/items?expand=fields',
            headers=az_headers) \
            .json()

        # convert json object to pandas dataframe object using json normalize
        site_config_df = json_normalize(site_config_json['value'])

        # Sharepoint Lists have a number of additional metadata columns that we don't care about in the return,
        # here, we're specifying both which columns from the sharepoint return we care about (key),
        # and what we want to rename the column to (Value) so it's a) easier to read later and b) makes it so the
        # only breaks in one place if column names end up changing.
        filter_col = {'fields.Title': 'Server',
                      'fields.CO_Trigger_Parameter': 'CO_Trigger_Parameter',
                      'fields.Run_Machine_Level_Analysis': 'Run_Machine_Level_Analysis',
                      'fields.Run_First_Stop_After_CO_Analysis': 'Run_First_Stop_After_CO_Analysis',
                      'fields.Run_Multi_Constraint_Analysis': 'Run_Multi_Constraint_Analysis',
                      'fields.Split_COs_based_on_Cause_Model': 'Split_COs_based_on_Cause_Model',
                      'fields.SUD_specific_RCO_script': 'SUD_specific_RCO_script',
                      'fields.SiteName': 'MDC_Site_Name',
                      'fields.changeover_query_single_line': 'querySL',
                      'fields.changeover_query_multi_line': 'queryML'}

        # filter down to the columns we care about
        site_config_df = site_config_df.loc[:, filter_col.keys()]

        # rename the columns to our defined naming scheme
        site_cfg = site_config_df.rename(columns=filter_col)

        # request to sharepoint to get the Line configuration table (all sites) as json
        line_config_json = requests.get(
            f'{az_endpoint}/sites/{sp_site_id}/lists/{line_config_list_id}/items?expand=fields',
            headers=az_headers) \
            .json()

        # normalize to pandas dataframe object
        line_config_df = json_normalize(line_config_json['value'])

        # define columns to filter and rename
        filter_col = {'fields.Title': 'MDC_Line_Name',
                      'fields.Department': 'Department',
                      'fields.Constraint_Machine_String': 'Constraint_Machine_String',
                      'fields.SiteNameLookupId': 'SiteNameLookupId',
                      'fields.System': 'System',
                      'fields.Line_Configuration': 'Line_Configuration'}

        # filter the columns we care about
        line_config_df = line_config_df.loc[:, filter_col.keys()]

        # rename the columns
        line_cfg = line_config_df.rename(columns=filter_col)

        # return the properly formatted dataframes for use in run_rco_analysis
        return site_cfg, line_cfg


# Run the actual script
if __name__ == '__main__':

    # MAKING CHANGES!!!

    # Token Authentication to MDC itself
    token_headers = {
        'Authorization': token_authorization,
        'Postman-Token': token_postman_token,
        'cache-control': token_cache_control,
        'content-type': token_content_type}
    token = 'Bearer ' + mdc.get_token(uaa, token_headers, api_user_name, get_secret(api_encrypted_password, enc_key),
                                      pmdc_client_id, get_secret(pmdc_encrypted_secret, enc_key))
    mdc_api_headers = {'tenant': tenant_id, 'authorization': token}

    # Connect to Destination Database - mine is a local postgres database, but this could be anything (azure, local mssql, etc.)
    # docs for how to form this connection string are here: https://docs.sqlalchemy.org/en/14/core/engines.html
    # Note, you will need to import the dialect of the database you are talking to at the top, even though it is not explicitly
    # called from the function - in pycharm it will complain about an unused import for the statement 'import psycopg2'
    # but this is where it's used. for mssql you would use 'pyodbc' or 'pymssql' like in the linked documentation.

    # rco_database = sa.create_engine(
    #     f'postgresql://{db_user}:{get_secret(db_encrypted_pass, enc_key)}@localhost:5432/rco')

    # Using local db
    # rco_database = sa.create_engine('postgresql://postgres:postgresql@localhost/rco')
    password='abc123@@'
    p = 'DRIVER='+'{ODBC Driver 17 for SQL Server}'+ ';' \
        'SERVER=' + '192.168.1.7\\MSSQLSERVER2014' + ';' \
        'DATABASE=' + 'RCO' + ';' \
        'UID=' + 'SA' + ';' \
        'PWD=' + password + ';' \
        'PORT=' + '1433' + ';'
            
    p = urllib.parse.quote_plus(p)
    
    rco_database = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' %p)
    
    # rco_database=sa.create_engine("mssql+pyodbc://sa:'+password+'@INHYNBCG-084\\MSSQLSERVER2014/RCO?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0")
    # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    # rco_database=sa.create_engine("mssql+pyodbc://sa:'+password+'@INHYNBCG-084\\MSSQLSERVER2014/RCO?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0")
    # rco_database = sa.create_engine('mssql+pyodbc://username:password@host:port/databasename')

    dir_path = os.path.dirname(os.path.realpath(__file__))
    # caching of sharepoint authentication so that you don't need to do the sharepoint terminal auth more than once
    CACHE_FILE = os.path.join(dir_path, 'utils', 'token_cache.bin')

    # Sharepoint Connection - get the token that will allow us to authenticate (terminal prompt)
    # todo: when we run this un-supervised there needs to be a way to have this token not expire.
    sp_token = get_sharepoint_token(CACHE_FILE, az_client_id, az_authority_url, az_scope)

    # Retrieve Configurations from Sharepoint
    site_configuration, line_configuration = get_sharepoint_config(sp_token)

    # Run ETL on data, collect dataframes
    log = run_rco_analysis(site_configuration, line_configuration, rco_database)

    # print(log)  # todo: have this line write an email with log contents to a mailing group.
    # send_email('', log)

    # create a log file of this specific run in the logs folder. todo: currently this throws an error every time, not sure why
    with open(f'logs/log_{datetime.now().strftime("%Y%m%d-%H%M%S")}.json', 'w+') as outfile:
        json.dump(log, outfile)

    # For viewing dataframes and testing - in PyCharm I print something at the very end of the script and place
    # a breakpoint on the print instruction, that way I can view all of the dataframes and variables in their end
    # state before the script exits and the memory clears.
    print('foo')
