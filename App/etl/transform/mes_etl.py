import pandas
import pandas as pd
import numpy as np

from App.etl.transform.first_stop import first_stop_analysis, sud_first_stop, sud_converter_constraint_stops_after_co
from App.etl.transform.machine_level import machine_level_analysis


def isolate_changeover_events(line_dt_df: pd.DataFrame, overall_params: dict, line_params: pd.DataFrame):
    print('this is where we take the DT raw data and Production raw data and do the magic to return list of C/O events')
    isolated_changeovers = None

    # get the query from sharepoint which is correct for the site/server being analyzed.
    if overall_params is not None:
        query_text = overall_params['querySL']

    if line_dt_df is not None and not pd.isna(query_text):
        isolated_changeovers = line_dt_df.query(query_text)

    return isolated_changeovers


def mes_etl_main(params, lineparams, line_dt, line_dt_full, machine_dt):
    """
    MES_ETL_MAIN.R - all line numbers in this document are for the RCO_MES_ETL.R file unless otherwise specified.

    onur used a lot of 'temp' files to perform operations on individual dataframes. this is not necessary in python
    as the dataframes are mutable (can add columns directly to dataframes).

    :param line_dt:
    :param line_dt_full:
    :param machine_dt:
    :param machine_dt_full:
    :param prod_data:
    :return:
    """

    co_aggregated_data, co_event_log, first_stop_after_co_data, gantt_data, event_log_for_gantt = (None,)*5   # MODIFYING TO DECLARE ALL VARIABLES AT THE SAME TIME

    # Filter CO Events - lines 6-69
    co_event_log = isolate_changeover_events(line_dt, params, lineparams)
    co_event_log = pd.DataFrame(co_event_log)

    if len(co_event_log):
        print('this is what happens if it returns changeover events')

        # todo: define [CO_Trigger_Column] based on Cause Lvl 1/2/3. This column is then used in the logic for detecting split CO events belonging to same CO.
        # line 77 | 122
        co_event_log['CO_Trigger_Column'] = co_event_log['CAUSE_LEVELS_1_NAME'] + co_event_log['CAUSE_LEVELS_2_NAME'] + co_event_log['CAUSE_LEVELS_3_NAME']

        # todo: order data per line and starttime. this ordering becomes crucial in combining split CO events into single CO as we look at previous row to make the decision.
        # line 89 | 134
        co_event_log.sort_values(by=['LINE', 'START_TIME'])
        co_event_log.dropna(subset=['START_TIME'], inplace=True)

        # CONVERTING START_TIME AND TO PANDAS DATETIME OBJECT
        co_event_log['START_TIME'] = pd.to_datetime(co_event_log['START_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')

        # todo: add number of seconds of downtime, the endtime of the downtime and row index
        # line 94 | 139
        co_event_log['DOWNTIME_SEC'] = co_event_log['DOWNTIME'] * 60
        co_event_log['END_TIME'] = co_event_log['START_TIME'] + pd.to_timedelta(co_event_log['DOWNTIME_SEC'], unit='s')
        co_event_log['index'] = co_event_log.index

        # todo: add previous rows' relevant data to current row, and calculate the minutes difference between end-time of previous CO event and start-time of current CO event
        # Line 99 | 144
        co_event_log['BRANDCODE'] = co_event_log['BRANDCODE'].astype(str)
        co_event_log['Previous_BRANDCODE'] = co_event_log['BRANDCODE'].shift(1, fill_value=0)
        co_event_log['Previous_LINE'] = co_event_log['LINE'].shift(1, fill_value=0)
        co_event_log['Previous_CO_Trigger_Column'] = co_event_log['CO_Trigger_Column'].shift(1, fill_value=0)
        co_event_log['Previous_END_TIME'] = co_event_log['END_TIME'].shift(1, fill_value=pd.NaT)
        co_event_log['MinutesDifference_vs_PreviousRow'] = (co_event_log['START_TIME'] - co_event_log['Previous_END_TIME'])\
                                                            .apply(lambda x: x.total_seconds() / 60)

        # todo: add [CO_Trigger] column, which tells whether this row is a new CO vs the previous row. Conditions to confirm whether two events belong to same CO (at least one of them needs to be true):
        #   -if Cause Model Lvl 1/2/3 is same vs previous row, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than the parameter defined at site-level input script?
        #   -if Brandcode is same vs previous row, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than the parameter defined at site-level input script?
        #   -if both above is true, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than 4/3 times the parameter defined at site-level input script?
        #   -if none of above is true, then is the minute diffence between end-time of previous CO event and start-time of current CO event is less than 2/3 times the parameter defined at site-level input script?
        # Line 112 | 156
        '''
        co_event_log['CO_Trigger'] = np.where(((co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger']
                                                    and co_event_log['CO_Trigger_Column'] == co_event_log['Previous_CO_Trigger_Column']
                                                    and co_event_log['LINE'] == co_event_log['Previous_LINE'])
                                                or (co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger'] * 4 / 3
                                                    and co_event_log['CO_Trigger_Column'] == co_event_log['Previous_CO_Trigger_Column']
                                                    and co_event_log['BRANDCODE'] == co_event_log['Previous_BRANDCODE']
                                                    and co_event_log['LINE'] == co_event_log['Previous_LINE'])
                                                or (co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger']
                                                    and co_event_log['BRANDCODE'] == co_event_log['Previous_BRANDCODE']
                                                    and co_event_log['LINE'] == co_event_log['Previous_LINE'])
                                                or (co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger'] * 3 / 2
                                                    and co_event_log['LINE'] == co_event_log['Previous_LINE'])
                                               ), 0, 1)
        '''
        co_event_log.loc[(((co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger'])
                            & (co_event_log['CO_Trigger_Column'] == co_event_log['Previous_CO_Trigger_Column'])
                            & (co_event_log['LINE'] == co_event_log['Previous_LINE']))
                           | ((co_event_log['MinutesDifference_vs_PreviousRow'] < (params['COTrigger'] * 4 / 3))
                            & (co_event_log['CO_Trigger_Column'] == co_event_log['Previous_CO_Trigger_Column'])
                            & (co_event_log['BRANDCODE'] == co_event_log['Previous_BRANDCODE'])
                            & (co_event_log['LINE'] == co_event_log['Previous_LINE']))
                           | ((co_event_log['MinutesDifference_vs_PreviousRow'] < params['COTrigger'])
                            & (co_event_log['BRANDCODE'] == co_event_log['Previous_BRANDCODE'])
                            & (co_event_log['LINE'] == co_event_log['Previous_LINE']))
                           | ((co_event_log['MinutesDifference_vs_PreviousRow'] < (params['COTrigger'] * 3 / 2))
                            & (co_event_log['LINE'] == co_event_log['Previous_LINE']))
                          ), 'CO_Trigger'] = 0
        co_event_log['CO_Trigger'].fillna(1, inplace=True)
        co_event_log['CO_Trigger'] = co_event_log['CO_Trigger'].astype(int)

        # todo: this is exception - for certain plants, we have to split CO events into separate COs if the Cause Model Lvl 1/2/3 is different. The input parameter is set at site's input script.
        # Line 126 | 171
        if (params['SplitCOsOnCause']):
            co_event_log.loc[co_event_log['CO_Trigger_Column'] != co_event_log['Previous_CO_Trigger_Column'], 'CO_Trigger'] = 1

        # todo: exception handling - for Lima SUD, there is specific request not ot split CO events if the Cause Model includes "Changeover Failure" and the minute difference vs previous row is less than 2hr.
        # Line 134 | 179
        if (params['SiteServer'] == 'Lima SUD'):
            co_event_log.loc[(co_event_log['CO_Trigger'] & co_event_log['CO_Trigger_Column'].str.contains('Changeover Failure')
                              & co_event_log['MinutesDifference_vs_PreviousRow'] < 120
                              & co_event_log['LINE'] == co_event_log['Previous_LINE']), 'CO_Trigger'] = 0

        # todo: For rows which is the first events of a new CO, add [CO_Identifier], and fill the rest of the events belonging to the same CO with same [CO_Identifier].
        # Line 141 | 186
        co_event_log.loc[co_event_log['CO_Trigger'] == 1, 'CO_Identifier'] = co_event_log['LINE'] + ' - ' + \
                                                                             co_event_log['START_TIME'].astype(str).str[0:9] + \
                                                                             ' - ' + co_event_log['downtime_id'].astype(str)
        co_event_log['CO_Identifier'] = co_event_log['CO_Identifier'].ffill()

        # todo: Generate [CO_Aggregated data]
        # Line 149 | 194
        co_aggregated_data = co_event_log.groupby(['CO_Identifier', 'LINE'], as_index=False)\
                                         .agg(CO_StartTime=('START_TIME', min),
                                              CO_EndTime=('END_TIME', max),
                                              Index_of_First_CO_Event=('index', min),
                                              Index_of_Last_CO_Event=('index', max),
                                              CO_DOWNTIME=('DOWNTIME', sum))\
                                         .sort_values(by=['LINE', 'CO_StartTime'])

        # todo: fix datetime fields' data type <- May not be neededin Python
        # Line 157 | 202
        co_aggregated_data['CO_StartTime'] = pd.to_datetime(co_aggregated_data['CO_StartTime'], format='%Y-%m-%d %H:%M:%S')
        co_aggregated_data['CO_EndTime'] = pd.to_datetime(co_aggregated_data['CO_EndTime'], format='%Y-%m-%d %H:%M:%S')

        # todo: add Downtime PK to first/last events <- for MDC data thi is the
        # Line 206
        temp = co_event_log[['index', 'downtime_id']].rename(columns={'downtime_id': 'downtime_id_of_First_CO_Event'})
        co_aggregated_data = pd.merge(co_aggregated_data, temp, left_on=['Index_of_First_CO_Event'], right_on=['index'], how='inner')

        temp = co_event_log[['index', 'downtime_id']].rename(columns={'downtime_id': 'downtime_id_of_Last_CO_Event'})
        co_aggregated_data = pd.merge(co_aggregated_data, temp, left_on=['Index_of_Last_CO_Event'], right_on=['index'], how='inner')


        # BRANDCODE DETERMINATION
        # todo: add columns on next CO StartTime
        # Line 179 | 223
        co_aggregated_data['Next_CO_StartTime'] = co_aggregated_data['CO_StartTime'].shift(-1, fill_value=pd.NaT)
        co_aggregated_data['Next_Line'] = co_aggregated_data['LINE'].shift(-1, fill_value=0)

        # todo: If this is the last CO for the line, then assume there 60min timespan we can explore for Next Brandcode after CO
        # Line 226
        for i in range(0, len(co_aggregated_data)):
            if co_aggregated_data['LINE'][i] != co_aggregated_data['Next_Line'][i]:
                co_aggregated_data['Next_CO_StartTime'][i] = co_aggregated_data['CO_EndTime'][i] + pd.Timedelta(seconds=60 * 60)

        co_aggregated_data['Previous_CO_EndTime'] = co_aggregated_data['CO_EndTime'].shift(1, fill_value=pd.NaT)
        co_aggregated_data['Previous_Line'] = co_aggregated_data['LINE'].shift(1, fill_value=0)

        # todo: if this is the first CO for the line, then assume there 60min timespan we can explore for Current Brandcode before CO
        # Line 235
        for i in range(0, len(co_aggregated_data)):
            if co_aggregated_data['LINE'][i] != co_aggregated_data['Previous_Line'][i]:
                co_aggregated_data['Previous_CO_EndTime'][i] = co_aggregated_data['CO_StartTime'][i] - pd.Timedelta(seconds=60 * 60)

        # CONVERTING TIME TO PANDAS DATETIME OBJECT
        line_dt_full['START_TIME'] = pd.to_datetime(line_dt_full['START_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')
        # ADDED TO MAKE THE FOR LOOP BELOW WORK
        line_dt_full['START_TIME_of_Uptime'] = pd.NaT

        for index in line_dt_full.index:
            line_dt_full['END_TIME'][index] = line_dt_full['START_TIME'][index] + pd.Timedelta(seconds=line_dt_full['DOWNTIME'][index] * 60)
            line_dt_full['START_TIME_of_Uptime'][index] = line_dt_full['START_TIME'][index] - pd.Timedelta(seconds=line_dt_full['UPTIME'][index] * 60)

        '''
        # NOT ABLE TO FIGURE OUT WHY THIS REFUSES TO WORK
        line_dt_full['END_TIME'] = line_dt_full['START_TIME'] + pd.Timedelta(seconds=line_dt_full['DOWNTIME'] * 60)
        line_dt_full['START_TIME_of_Uptime'] = line_dt_full['START_TIME'] + pd.Timedelta(seconds=line_dt_full['UPTIME'] * 60)
        '''

        co_aggregated_data['Current_BRANDCODE'] = ''
        co_aggregated_data['Next_BRANDCODE'] = ''

        # per CO, look at the line downtime log to determine brandcodes.
        # todo: For Current Brandcode, by default the full timespan between the end of previous CO until the start of current CO is investigated. And the last available brandcode is taken. i.e. In this timespan, if two brandcodes are observed, then the one observed latest is used.
        #   For Next Brandcode, by default the full timespan between the end of current CO until the start of next CO is investigated. And the first available brandcode that is NOT equal to Current Brandcode is taken. If no such brandcode is available, then Next Brandcode is made equal to Current Brandcode.
        # Line 203 | 251
        for index in co_aggregated_data.index:
            temp = line_dt_full[(line_dt_full['LINE'] == co_aggregated_data['LINE'][index])
                                & (line_dt_full['START_TIME'] > co_aggregated_data['Previous_CO_EndTime'][index])
                                & (line_dt_full['START_TIME'] <= co_aggregated_data['CO_StartTime'][index])]
            temp2 = temp[temp['START_TIME_of_Uptime'] < co_aggregated_data['CO_StartTime'][index]]
            if len(temp2) > 0:
                co_aggregated_data['Current_BRANDCODE'][index] = temp2['BRANDCODE'].iloc[len(temp2) - 1]
            else:
                if len(temp) > 0:
                    co_aggregated_data['Current_BRANDCODE'][index] = temp['BRANDCODE'].iloc[len(temp) - 1]

            temp = line_dt_full[(line_dt_full['LINE'] == co_aggregated_data['LINE'][index])
                                & (line_dt_full['START_TIME_of_Uptime'] > co_aggregated_data['CO_StartTime'][index])
                                & (line_dt_full['START_TIME_of_Uptime'] < co_aggregated_data['Next_CO_StartTime'][index])]
            if len(temp) > 0:
                temp2 = temp[temp['BRANDCODE'] != co_aggregated_data['Current_BRANDCODE'][index]]
                if len(temp2) > 0:
                    co_aggregated_data['Next_BRANDCODE'][index] = temp2['BRANDCODE'].iloc[0]
                else:
                    co_aggregated_data['Next_BRANDCODE'][index] = co_aggregated_data['Current_BRANDCODE'][index]

        # todo: add column indicating whether brandcode is changed.
        # Line 228 | 273
        co_aggregated_data['Brandcode_Status'] = np.where(co_aggregated_data['Current_BRANDCODE'] == co_aggregated_data['Next_BRANDCODE'], 'Not Changed', 'OK')

        # todo: run specific logic for multi-constraint lines to report downtime per constraint that has been actively changed over for the given CO
        # Line 277
        if params['MultiConstraint']:
            temp = co_event_log.groupby(by=['CO_Identifier', 'MACHINE']).agg(DOWNTIME=('DOWNTIME', sum))
            temp = temp.groupby(by=['CO_Identifier']).agg(Number_of_Machines=('CO_Identifier', 'count'))
            co_aggregated_data = pd.merge(co_aggregated_data, temp, on='CO_Identifier')
            co_aggregated_data['Number_of_Machines'].fillna(1, inplace=True)
            co_aggregated_data['CO_DOWNTIME'] = co_aggregated_data['CO_DOWNTIME'] / co_aggregated_data['Number_of_Machines']

        # todo: clean-up columns
        # Line 233 | 286
        co_aggregated_data = co_aggregated_data[['CO_Identifier', 'LINE', 'CO_StartTime', 'CO_EndTime', 'CO_DOWNTIME',
                                                 'Current_BRANDCODE', 'Next_BRANDCODE', 'downtime_id_of_First_CO_Event',
                                                 'downtime_id_of_Last_CO_Event', 'Brandcode_Status']]
        co_aggregated_data['Server'] = params['SiteServer']
        co_aggregated_data.sort_values(by='CO_StartTime', inplace=True)

        co_event_log = co_event_log[['CO_Identifier', 'LINE', 'CAUSE_LEVELS_1_NAME', 'CAUSE_LEVELS_2_NAME',
                                     'CAUSE_LEVELS_3_NAME', 'CAUSE_LEVELS_4_NAME', 'START_TIME', 'UPTIME', 'DOWNTIME',
                                     'BRANDCODE', 'TEAM', 'SHIFT', 'OPERATOR_COMMENT', 'CO_Trigger_Column', 'END_TIME',
                                     'downtime_id', 'ProdDesc', 'ProcessOrder']]

        # double check to ensure no events in this table that does not appear in [CO_Aggregated_Data].
        co_event_log = co_event_log[co_event_log['CO_Identifier'].isin(co_aggregated_data['CO_Identifier'].unique())]
        co_event_log['Server'] = params['SiteServer']
        co_event_log.sort_values(by='START_TIME', inplace=True)

        # todo: replace characters which are later causing issues in SQL or csv writing, and PowerBI reading
        # Line 274 | 329
        co_event_log['OPERATOR_COMMENT'] = co_event_log['OPERATOR_COMMENT'].str.replace('\\r\\n', ' ')
        co_event_log['OPERATOR_COMMENT'] = co_event_log['OPERATOR_COMMENT'].str.replace('\\n', ' ')
        co_event_log = co_event_log[co_event_log['LINE'].notnull()]

        # todo: Run stops after CO and Machine stops ETL - generates specific timestamp data used in PowerBI machine level visualization.
        #   Output tables are called [Event_Log_for_Gantt] and [Gantt_Data].
        # Line 282 | 337
        if params['MachineLevel']:
            event_log_for_gantt, gantt_data = machine_level_analysis(params, lineparams, machine_dt, line_dt, line_dt_full, co_event_log, co_aggregated_data)

        # todo: Run First Stop after CO ETL - used to log first Unplanned Constraint Stop after CO and the total uptime passed till that stop.
        #   Output table is called [First_Stop_after_CO_Data].
        if params['FirstStop']:
            # todo: for these program subroutines, make a new file under transform to put the code.
            #   note how the 'first_stop_analysis' function is defined in another file and imported - this can be
            #   done automatically through pycharm by using the right click -> refactor -> move after highlighting
            #   the code
            first_stop_after_co_data = first_stop_analysis(params, lineparams, line_dt_full, co_event_log, co_aggregated_data)

        # todo: Run Machine Stops after CO ETL - used to log all the Machine Stops after CO till start of next CO.
        #  If Converter CO, includes machine stops for Converter_plus_Legs, and if Leg CO, machine stops for the specific Leg.
        #  Includes both constraint and non-constraint machines.
        #  Output table is called [MACHINE_DOWNTIME_Final].
        if params['SUDSpecific']:
            machine_dt_final = sud_first_stop('see what parameters this part of the script needs')

        # todo: Run Converter Constraint Stops after CO ETL - used to log all the Constraint Stops after CO.
        #   Output table is called [Converter_Downtime_Final].
        # CURRENTLY DISABLED.
        if params['SUDSpecific']:
            converter_downtime_final = sud_converter_constraint_stops_after_co()

    else:
        print('this is what happens if nothing is returned... nothing')



    return co_aggregated_data, co_event_log, first_stop_after_co_data, gantt_data, event_log_for_gantt
