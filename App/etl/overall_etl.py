import pandas as pd
import os
from datetime import datetime, timedelta
import logging

from App.etl.extract.data_from_mdc import dt_data_extract, prod_data_extract, runtime_data_extract, brandcode_data_extract
from App.etl.transform.first_stop import first_stop_analysis, sud_first_stop
from App.etl.transform.mes_etl import mes_etl_main
from App.utils.time_utils import get_analysis_time_bounds


def append_data_to_sql(db_connection, new_rows: pd.DataFrame, table_name: str) -> str:
    """
    Function to append data to existing SQL tables. It looks at the column data types in SQL and revises the data types and then performs the appending.

    :params db_connection:
    :params new_rows:
    :params table_name:
    :returns: message:
    """
    # todo write append data to existing data tables
    #query = f'exec sp_columns {table_name}'
    ## FOR LOCAL DATABASE
    query = 'SELECT column_name, data_type ' \
         'FROM information_schema.columns ' \
             'WHERE table_name=?'
    result = db_connection.execute(query, table_name).fetchall()
    #result = db_connection.execute(query).fetchall()
    columns_in_sql = pd.DataFrame(data=result, columns=['COLUMN_NAME', 'DATA_TYPE'])
    new_table = pd.DataFrame(columns=list(columns_in_sql['COLUMN_NAME']))
    new_rows.columns = new_rows.columns.str.lower()
    for column in new_table.columns:
        if column in new_rows.columns:
            new_table[column] = new_rows[column]
        else:
            new_table[column] = pd.NA

    try:
        new_table.to_sql(table_name, db_connection, if_exists='append', index=False)
    except:
        return False

    return True


def data_type_replace(data_to_be_replaced,data_to_be_used):
    # todo: no idea if this will be necessary in the python. R lines 79-116
    print('pretending to replace datatypes while code is being written')
    return 'temporary list of data to be replaced, data to be used'


def site_server_overall_etl(mdc_header: dict, params: dict, line_params: pd.DataFrame, db_connection) -> bool:
    """
    Runs the logic from RCO_Overall_Orchestrator.R
    Line numbers unless otherwise noted are referring to lines in that R script

    :param mdc_header:
    :param params:
    :param line_params:
    :param db_connection:
    :return:
    """

    start_marker = datetime.now()

    ###########
    # EXTRACT #
    ###########
    print(params['SiteServer'], params['SiteMDCName'])

    # Line 56 - 76 RCO_Overall_Orchestrator
    # Line 74 - 129 RCO_Overall_Orchestrator New
    start_time, end_time = get_analysis_time_bounds(db_connection, params['SiteServer'])

    # entire contents of RCO_ProficyiODS_Orchestrator - they are the 'extract' portion of ETL, lines 68-179
    line_dt, line_dt_full, machine_dt, machine_dt_full = dt_data_extract(mdc_header, params, line_params, start_time, end_time)
    prod_data = prod_data_extract(mdc_header, params, line_params, start_time, end_time)
    ## FOR TESTING
    # runtime_per_day_data, day_starttime_per_line = runtime_data_extract(mdc_header, params, line_params, start_time, end_time)
    # brandcode_data = brandcode_data_extract(params, prod_data)
    runtime_per_day_data, day_starttime_per_line = (None,) * 2
    brandcode_data = None


    #############
    # TRANSFORM #
    #############

    # RCO_MES_ETL.R and all of its functionality will go in this area - in the R script, the Maple or Proficy orchestator
    # calls RCO_MES_ETL.R directly

    logging.info('ETL Started')

    co_aggregated_data, \
    co_event_log, \
    first_stop_after_co_data,\
    gantt_data, \
    event_log_for_gantt = mes_etl_main(params, line_params, line_dt, line_dt_full, machine_dt)

    # The output of [these scripts] gives out the data frames [CO_Aggregated_Data], [CO_Event_Log], [Runtime_per_Day_data], [BRANDCODE_data], [First_Stop_after_CO_Data], [Gantt_Data] and [Event_Log_for_Gantt].
    # The next section of this script mainly performs appending this new data to historical data already stored in Transformed Data Storage.

    '''
    if co_event_log:  # based on line 73, 74 of rco_mes_etl.R - if nothing is found, should be None and this will not execute
        print('this is what happens if etl returns changeover events')

        # script branches like the following analysis probably go here
        if params['first_stop_after_CO_analysis']:
            first_stop_analysis(line_dt)

        if params['SUDSpecific'] and machine_dt:
            sud_first_stop(line_dt, machine_dt)

    else:
        print('this is what happens if no changeovers are found')
    '''

    logging.info('ETL Completed')

    ########
    # LOAD #
    ########

    # todo: Define SQL table names to be used in Transformed Data Storage - can do this in the .env file, lines 16-26
    sql_tablename_co_aggregated_data = os.getenv('sql_tablename_co_aggregated_data')
    sql_tablename_co_event_log = os.getenv('sql_tablename_co_event_log')
    sql_tablename_script_data = os.getenv('sql_tablename_script_data')
    sql_tablename_runtime_per_day_data = os.getenv('sql_tablename_runtime_per_day_data')
    sql_tablename_brandcode_data = os.getenv('sql_tablename_brandcode_data')
    sql_tablename_gantt_data = os.getenv('sql_tablename_gantt_data')
    sql_tablename_event_log_for_gantt = os.getenv('sql_tablename_event_log_for_gantt')
    sql_tablename_first_stop_after_co_data = os.getenv('sql_tablename_first_stop_after_co_data')

    # todo: determine if converting non-latin characters to utf-16 is needed (lines 4-8, 43-53).
    #  Hoshin does not have this issue as far as I can tell.

    # todo: if there is at least one CO available in the data, perform few post-treatment steps.
    if len(co_event_log):
        co_event_log_full = co_event_log
        co_aggregated_data_full = co_aggregated_data
        co_aggregated_data_full['Brandcode_Status'].fillna('Unknown', inplace=True)

        # substitute character "'" which creates issues when writing/reading data to SQL.
        if params['MachineLevel']:
            gantt_data_full = gantt_data
            event_log_for_gantt_full = event_log_for_gantt
            event_log_for_gantt_full['OPERATOR_COMMENT'] = event_log_for_gantt_full['OPERATOR_COMMENT'].str.replace("'", " ", regex=True)
            event_log_for_gantt_full['CAUSE_LEVELS_3_NAME'] = event_log_for_gantt_full['CAUSE_LEVELS_3_NAME'].str.replace("'", " ", regex=True)
            event_log_for_gantt_full['CAUSE_LEVELS_4_NAME'] = event_log_for_gantt_full['CAUSE_LEVELS_4_NAME'].str.replace("'", " ", regex=True)
        if params['FirstStop']:
            first_stop_after_co_data_full = first_stop_after_co_data
            first_stop_after_co_data_full['OPERATOR_COMMENT'] = first_stop_after_co_data_full['OPERATOR_COMMENT']
            first_stop_after_co_data_full['CAUSE_LEVELS_3_NAME'] = first_stop_after_co_data_full['CAUSE_LEVELS_3_NAME']
            first_stop_after_co_data_full['CAUSE_LEVELS_4_NAME'] = first_stop_after_co_data_full['CAUSE_LEVELS_4_NAME']
            first_stop_after_co_data_full.dropna(subset=['START_TIME'], inplace=True)
        if len(co_event_log_full) > 0:
            co_event_log_full['OPERATOR_COMMENT'] = co_event_log_full['OPERATOR_COMMENT'].str.replace("'", " ", regex=True)
            co_event_log_full['CAUSE_LEVELS_3_NAME'] = co_event_log_full['CAUSE_LEVELS_3_NAME'].str.replace("'", " ", regex=True)
            co_event_log_full['CAUSE_LEVELS_4_NAME'] = co_event_log_full['CAUSE_LEVELS_4_NAME'].str.replace("'", " ", regex=True)
        if brandcode_data is not None and len(brandcode_data) > 0:      ## FOR TESTING
            brandcode_data['BRANDNAME'] = brandcode_data['BRANDNAME'].str.replace("'", " ", regex=True)

        # add blank column [Total_Uptime_till_Next_CO] if the First Stop after CO sub-RTL is not enabled.
        if 'Total_Uptime_till_Next_CO' not in co_aggregated_data_full.columns:
            co_aggregated_data_full['Total_Uptime_till_Next_CO'] = None

    # Runtime_per_Day_data$Server < - Server_Name
    # Runtime_per_Day_data$Runtime < - round(Runtime_per_Day_data$Runtime, 1)
    # Runtime_per_Day_data_full < - Runtime_per_Day_data

    time_pass = round((datetime.now() - start_marker).total_seconds() / 60, 1)
    logging.info('Time passed for MES data extraction & ETL: {} min'.format(time_pass))

    # NUMBER OF CONSTRAINTS DATA
    if line_dt is not None and len(line_dt) > 0:
        number_of_constraints_data = line_dt.groupby(by=['LINE', 'MACHINE'], as_index=False).agg(UPTIME=('UPTIME', sum))
        number_of_constraints_data = number_of_constraints_data.groupby(by='LINE', as_index=False) \
                                                           .agg(Number_of_Constraints=('LINE', 'count'))

    # todo: run Transformed Data Storage appending per line
    for index in line_params.index:
        start_marker = datetime.now()
        system = line_params['System'][index]
        line_name = line_params['MDC_Line_Name'][index]
        logging.info('Equipment #: {} ({}) started'.format(index, system))

        # check if this line is already available in [Script_Data] and if not, add it.
        query = "SELECT * " \
                f"FROM {sql_tablename_script_data} " \
                "WHERE MES_Line_Name=? AND Server=?"
        new_rows = db_connection.execute(query, line_name, params['SiteServer']).fetchall()
        if len(new_rows) == 0:
            query = 'SELECT TOP 1 * ' \
                    f'FROM {sql_tablename_script_data}'
            ## FOR LOCAL DATABASE
            # query = 'SELECT * ' \
            #         f'FROM {sql_tablename_script_data} ' \
            #         'LIMIT 1'
            new_rows = db_connection.execute(query).fetchone()
            new_rows = pd.DataFrame(columns=['System', 'Data_Update_Time', 'First_Available_Data_Point', 'Last_Available_Data_Point',
                                'MES_Line_Name', 'Server', 'Day_Start_hours', 'BU', 'Number_of_Constraints'])
            # new_rows['System'] = system
            # new_rows['MES_Line_Name'] = line_name
            # new_rows['Server'] = params['SiteServer']
            # new_rows['BU'] = 'FHC'
            # new_rows['Data_Update_Time'] = datetime.now()

            query = "SELECT min(CO_StartTime) as Min_time, max(CO_StartTime) as Max_time " \
                    f"FROM {sql_tablename_co_aggregated_data} " \
                    "WHERE Line=? AND Server=?"
            result = db_connection.execute(query, line_name, params['SiteServer']).first()
            # new_rows['First_Available_Data_Point'] = result[0]
            # new_rows['Last_Available_Data_Point'] = result[1]
            new_rows = new_rows.append({'System': system, 'MES_Line_Name': line_name, 'Server': params['SiteServer'], 'BU': 'FHC', 'Data_Update_Time': datetime.now(), 'First_Available_Data_Point': result[0], 'Last_Available_Data_Point': result[1]}, ignore_index=True)

            # day_start_hours = day_starttime_per_line[day_starttime_per_line['LINE'] == line_name]['Day_Start_hours'].values
            day_start_hours = []        ## FOR TESTING
            if len(day_start_hours) > 0:
                new_rows['Day_Start_hours'].iloc[0] = day_start_hours[0]
            else:
                new_rows['Day_Start_hours'] = 6

            if pd.isna(new_rows['Number_of_Constraints'].iloc[0]):
                new_rows['Number_of_Constraints'].iloc[0] = 1

            if params['MultiConstraint']:
                temp = number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]
                if len(temp) > 0:
                    if temp['Number_of_Constraints'].iloc[0] > new_rows['Number_of_Constraints'].iloc[0]:
                        new_rows['Number_of_Constraints'].iloc[0] = temp['Number_of_Constraints'].iloc[0]

            flag = append_data_to_sql(db_connection, new_rows, sql_tablename_script_data)

    # todo: change this to not always be true
    success = True

    if success:
        return 'Success'
    else:
        return 'Failure'