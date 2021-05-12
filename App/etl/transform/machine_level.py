import pandas as pd
import logging


# todo add machine level stop functions
def machine_level_analysis(site_params: dict, line_params: pd.DataFrame, machine_dt: pd.DataFrame, line_dt: pd.DataFrame, line_dt_full: pd.DataFrame, co_event_log: pd.DataFrame, co_aggregated_data: pd.DataFrame):
    """
    RCO_subETL_Gantt_Data_generator.R - copied over comments and line numbers for reference

    :param line_params:
    :param machine_dt:
    :param line_dt_full:
    :param co_event_log:
    :param co_aggregated_data:
    :return: event_log_for_gantt, gantt_data
    """
    # todo: Define the range of generation before/after CO
    #  KEEP THEM HARDCODED?
    # Line 2
    minutes_to_take_data_before_CO = 5
    minutes_to_take_data_after_CO = 15

    # todo: determine number of constraints by looking at unique Machine count per line
    number_of_constraints_data = line_dt.groupby(by=['LINE', 'MACHINE'], as_index=False).agg(UPTIME=('UPTIME', sum))
    number_of_constraints_data = number_of_constraints_data.groupby(by='LINE', as_index=False).agg(Number_of_Constraints=('LINE', 'count'))

    machine_dt['START_TIME'] = pd.to_datetime(machine_dt['START_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')
    machine_dt['END_TIME'] = pd.to_datetime(machine_dt['END_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')
    machine_dt = machine_dt.astype({'MACHINE': 'str', 'LINE': 'str', 'downtime_id': 'str'})

    # todo: exception handling - add/remove rows or tables if they were missed in MES data extraction stage.
    if 'PUDesc' in line_dt_full.columns:
        line_dt_full.rename({'PUDesc': 'MACHINE'}, inplace=True)
    if 'PLC_CODE' in line_dt_full.columns:
        line_dt_full.rename({'PLC_CODE': 'Fault'}, inplace=True)
    if 'PLC_CODE' in line_dt_full.columns:
        machine_dt.rename({'PLC_CODE': 'Fault'}, inplace=True)


    ## GANTT DATA GENERATION (FOR NON-CONSTANT MACHINES)
    # By looking at the downtime log of each MES Machine, [Gantt_Data] log is created
    # todo: For the data to be properly visualized in PowerBI, for every uptime and downtime, a data point is created both at the beginning and at the end of each uptime and downtime.
    #  [DOWNTIME_STATUS] key: "2": Downtime / "3": Uptime.
    #  For every row generated in [Gantt_data], it's downtime log primary key [DOWNTIME_PK] is stored, and all those downtime log rows are stored in [Event_Log_for_Gantt].
    gantt_data = pd.DataFrame(columns=['StartTime', 'Line', 'Machine', 'Downtime_Status', 'downtime_id', 'CO_Identifier'])
    gantt_data['StartTime'] = pd.to_datetime(gantt_data['StartTime'])
    gantt_data['Downtime_Status'] = pd.to_numeric(gantt_data['Downtime_Status'])

    event_log_for_gantt = machine_dt.iloc[0].to_frame().transpose()
    event_log_for_gantt['CO_Identifier'] = ''
    event_log_for_gantt = pd.DataFrame().reindex_like(event_log_for_gantt)
    event_log_for_gantt.drop(event_log_for_gantt.index, inplace=True)

    # todo: loop per line.
    for index in line_params['MDC_Line_Name'].index:
        line_name = line_params['MDC_Line_Name'][index]
        co_aggregated_data_temp = co_aggregated_data[co_aggregated_data['LINE'] == line_name]
        machine_dt_temp = machine_dt[machine_dt['LINE'] == line_name]

        # todo: if there is at least one CO available for that line..
        if len(co_aggregated_data_temp) > 0:

            # todo: loop per CO
            for i in range(len(co_aggregated_data_temp)):
                # define the time window boundaries of the Gantt data taking.
                co_endtime = co_aggregated_data_temp['CO_EndTime'].iloc[i]
                max_time = co_endtime + pd.Timedelta(seconds=minutes_to_take_data_after_CO * 60)
                co_starttime = co_aggregated_data_temp['CO_StartTime'].iloc[i]
                min_time = co_starttime - pd.Timedelta(seconds=minutes_to_take_data_before_CO * 60)

                # todo: filter Machine stops during this time window.
                machine_stops = machine_dt_temp[(machine_dt_temp['END_TIME'] > min_time)
                                                & (machine_dt_temp['START_TIME'] < max_time)]
                machine_stops.dropna(subset=['START_TIME'], inplace=True)

                if len(machine_stops) > 0:
                    machines = machine_stops['MACHINE'].unique()

                    # todo: loop per machine
                    for j in range(len(machines)):
                        machine_name = machines[j]
                        stops_of_machine = machine_stops[machine_stops['MACHINE'] == machine_name]

                        # todo: convert uptime/downtime to seconds
                        stops_of_machine['DOWNTIME'] = stops_of_machine['DOWNTIME'] * 60
                        stops_of_machine['UPTIME'] = stops_of_machine['UPTIME'] * 60
                        # todo: add data of previous uptime's end moment
                        # stops_of_machine['Previous_Uptime_End'] = stops_of_machine['START_TIME'] - pd.Timedelta(seconds=stops_of_machine['UPTIME'])
                        stops_of_machine['Previous_Uptime_End'] = pd.NaT
                        for ind in stops_of_machine.index:
                            stops_of_machine['Previous_Uptime_End'][ind] = stops_of_machine['START_TIME'][ind] - pd.Timedelta(seconds=stops_of_machine['UPTIME'][ind])

                        # todo: create data with first row
                        gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = min_time
                        gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                        gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                        gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                        gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                        # todo: here, if there was an uptime passing thru the StartTime of time window boundary for the CO, then an extra point is added at this StartTime of time window boundary.
                        #  note that when creating data, of a certain uptime or downtime event is less than or equal to 1sec, then that event is skipped, and that 1sec is automatically considered as continuation of previous status of the machine.
                        if min_time < stops_of_machine['START_TIME'].iloc[0] and min_time > stops_of_machine['Previous_Uptime_End'].iloc[0]:
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3

                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[0] - pd.Timedelta(seconds=1)
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[0]
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                            if stops_of_machine['DOWNTIME'].iloc[0] > 1:
                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[0]
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]
                        else:
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                            if stops_of_machine['DOWNTIME'].iloc[0] > 1:
                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[0]
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                        # todo: create data for rest of the rows for the given machine
                        if len(stops_of_machine) > 1:
                            for k in range(1, len(stops_of_machine)):
                                if stops_of_machine['UPTIME'].iloc[k] >= 2:
                                    gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                    gantt_data['StartTime'].iloc[len(gantt_data) - 1] = gantt_data['StartTime'].iloc[len(gantt_data) - 2] + pd.Timedelta(seconds=1)
                                    gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                    gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                    gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                                    gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[k]
                                    gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                                    gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                    gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[k] - pd.Timedelta(seconds=1)
                                    gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                    gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                    gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                                    gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[k]
                                    gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                                if stops_of_machine['DOWNTIME'].iloc[k] > 1:
                                    gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                    gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[k]
                                    gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                    gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                    gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                                    gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[k]
                                    gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                                    gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                    gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[k]
                                    gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                    gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                    gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 2
                                    gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[k]
                                    gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                        # todo: here, if there is an uptime passing thru the Endtime of time window boundary for the CO, then an extra point is added at this Endtime of the windown boundary.
                        if gantt_data['StartTime'].iloc[len(gantt_data) - 1] > max_time:
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = max_time
                        else:
                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = gantt_data['StartTime'].iloc[len(gantt_data) - 2] + pd.Timedelta(seconds=1)
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = ''
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = max_time
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = ''
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                    machine_stops['CO_Identifier'] = co_aggregated_data_temp['CO_Identifier'].iloc[i]
                    event_log_for_gantt = event_log_for_gantt.append(machine_stops, ignore_index=True)

    # todo: non-constraint data is stored temporarily separately.
    gantt_data.dropna(subset=['StartTime'], inplace=True)
    gantt_data_temp = gantt_data
    event_log_for_gantt_temp = event_log_for_gantt


    # GANTT DATA GENERATION (FOR CONSTRAINT MACHINES)
    # todo: By looking at the downtime log of each MES Machine, [Gantt_Data] is created.
    #  For the data to be properly visualized in PowerBI, for every uptime and downtime, a data point is created both at the beginning and at the end of each uptime and downtime.
    #  [DOWNTIME_STATUS] key: '1': CO Event / '1.7': Planned Downtime / '2.3': Unplanned Downtime / '3': Uptime / '4': Idle.
    #  For every row generated in [Gantt_Data], it's downtime log primary key [DOWNTIME_PK] is stored, and all those downtime log rows are stored in [Event_Log_for_Gantt].
    temp = co_event_log[['LINE', 'downtime_id']]
    temp['CO_Event'] = 1
    line_dt_for_gantt = line_dt_full.merge(temp, on=['LINE', 'downtime_id'], how='left')
    line_dt_for_gantt['CO_Event'].fillna(0, inplace=True)
    line_dt_for_gantt.sort_values(by='START_TIME', inplace=True)
    line_dt_for_gantt = line_dt_for_gantt.astype({'downtime_id': 'str'})

    gantt_data.drop(gantt_data.index, inplace=True)
    event_log_for_gantt.drop(event_log_for_gantt.index, inplace=True)

    # this loop is very similar to above loop for non-constraint machines and therefore not commented.
    # Only differences are that there is no separate loop done per machine (as anyway only single-constraint machines go thru this loop).
    # Also the [DOWNTIME_STATUS] assignment is different, as explained with the key above.
    for i in range(len(line_params)):
        line_name = line_params['MDC_Line_Name'].iloc[i]
        co_aggregated_data_temp = co_aggregated_data[co_aggregated_data['LINE'] == line_name]
        line_dt_temp = line_dt_for_gantt[line_dt_for_gantt['LINE'] == line_name]

        if len(number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]) > 0:
            number_of_constraints = number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]['Number_of_Constraints'].iloc[0]
        else:
            number_of_constraints = 1

        co_aggregated_data_temp.reset_index(drop=True)

        if len(co_aggregated_data_temp) > 0 and number_of_constraints == 1:
            for j in range(len(co_aggregated_data_temp)):
                co_endtime = co_aggregated_data_temp['CO_EndTime'].iloc[j]
                max_time = co_endtime + pd.Timedelta(seconds=minutes_to_take_data_after_CO * 60)
                co_starttime = co_aggregated_data_temp['CO_StartTime'].iloc[j]
                min_time = co_starttime - pd.Timedelta(seconds=minutes_to_take_data_before_CO * 60)

                stops_of_machine = line_dt_temp[(line_dt_temp['END_TIME'] > min_time) & (line_dt_temp['START_TIME'] < max_time)]
                stops_of_machine.dropna(subset=['START_TIME'], inplace=True)

                machine_name = stops_of_machine['MACHINE'].unique()[0]

                stops_of_machine['DOWNTIME'] = stops_of_machine['DOWNTIME'] * 60
                stops_of_machine['UPTIME'] = stops_of_machine['UPTIME'] * 60
                # stops_of_machine['Previous_Uptime_End'] = stops_of_machine['START_TIME'] - pd.Timedelta(seconds=stops_of_machine['UPTIME'])
                stops_of_machine['Previous_Uptime_End'] = pd.NaT
                for ind in stops_of_machine.index:
                    stops_of_machine['Previous_Uptime_End'][ind] = stops_of_machine['START_TIME'][ind] - pd.Timedelta(seconds=stops_of_machine['UPTIME'][ind])

                if len(stops_of_machine) > 0:
                    # todo: create data for first row
                    gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                    gantt_data['StartTime'].iloc[len(gantt_data) - 1] = min_time
                    gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                    gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                    gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                    gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                    if min_time < stops_of_machine['START_TIME'].iloc[0] and min_time > stops_of_machine['Previous_Uptime_End'].iloc[0]:
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3

                        gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[0] - pd.Timedelta(seconds=1)
                        gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                        gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                        gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                        gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                        gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[0]
                        gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                        gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[0] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[0] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[0] == 0 else 4)))
                        gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                        gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                        if stops_of_machine['DOWNTIME'].iloc[0] > 1:
                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[0]
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[0] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[0] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[0] == 0 else 4)))
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                    else:
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[0] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[0] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[0] == 0 else 4)))
                        if stops_of_machine['DOWNTIME'].iloc[0] > 1:
                            gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                            gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[0]
                            gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                            gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                            gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[0] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[0] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[0] == 0 else 4)))
                            gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[0]
                            gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                    # todo: create data for rest of the rows
                    if len(stops_of_machine) > 1:
                        for row_no in range(1, len(stops_of_machine)):
                            if stops_of_machine['UPTIME'].iloc[row_no] >= 2:
                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = gantt_data['StartTime'].iloc[len(gantt_data) - 2] + pd.Timedelta(seconds=1)
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[row_no]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[row_no] - pd.Timedelta(seconds=1)
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[row_no]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                            if stops_of_machine['DOWNTIME'].iloc[row_no] > 1:
                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['START_TIME'].iloc[row_no]
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[row_no] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[row_no] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[row_no] == 0 else 4)))
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[row_no]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                                gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                                gantt_data['StartTime'].iloc[len(gantt_data) - 1] = stops_of_machine['END_TIME'].iloc[row_no]
                                gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                                gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                                gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = (1 if stops_of_machine['CO_Event'].iloc[row_no] == 1 else (1.7 if stops_of_machine['Planned_Stop_Check'].iloc[row_no] == 1 else (2.3 if stops_of_machine['Idle_Check'].iloc[row_no] == 0 else 4)))
                                gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = stops_of_machine['downtime_id'].iloc[row_no]
                                gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                    if gantt_data['StartTime'].iloc[len(gantt_data) - 1] > max_time:
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = max_time
                    else:
                        gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = gantt_data['StartTime'].iloc[len(gantt_data) - 2] + pd.Timedelta(seconds=1)
                        gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                        gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                        gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = ''
                        gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                        gantt_data = gantt_data.append(pd.Series(), ignore_index=True)
                        gantt_data['StartTime'].iloc[len(gantt_data) - 1] = max_time
                        gantt_data['Line'].iloc[len(gantt_data) - 1] = line_name
                        gantt_data['Machine'].iloc[len(gantt_data) - 1] = machine_name
                        gantt_data['Downtime_Status'].iloc[len(gantt_data) - 1] = 3
                        gantt_data['downtime_id'].iloc[len(gantt_data) - 1] = ''
                        gantt_data['CO_Identifier'].iloc[len(gantt_data) - 1] = co_aggregated_data_temp['CO_Identifier'].iloc[j]

                    stops_of_machine['CO_Identifier'] = co_aggregated_data_temp['CO_Identifier'].iloc[j]
                    event_log_for_gantt = event_log_for_gantt.append(stops_of_machine, ignore_index=True)


    # todo: remove data for constraint machines generated in non-constraint level data. (as the data for these constraint machines are already generated in constraint level data).
    #  only exception is that, for multi-constraint lines the constraint machines' data is kept in non-constraint level data.
    for i in range(len(line_params)):
        line_name = line_params['MDC_Line_Name'].iloc[i]
        number_of_constraints = 1
        if len(number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]) > 0:
            number_of_constraints = number_of_constraints_data[number_of_constraints_data['LINE'] == line_name]['Number_of_Constraints'].iloc[0]
        if number_of_constraints == 1:
            if len(gantt_data[gantt_data['Line'] == line_name]) > 0:
                machine = gantt_data[gantt_data['Line'] == line_name]['Machine'].iloc[0]
                gantt_data_temp = gantt_data_temp[~((gantt_data_temp['Line'] == line_name) & (gantt_data_temp['Machine'] == machine))]
                event_log_for_gantt_temp = event_log_for_gantt_temp[~((event_log_for_gantt_temp['LINE'] == line_name) & (event_log_for_gantt_temp['MACHINE'] == machine))]

    # todo: revert the downtime and uptime to minutes
    event_log_for_gantt['DOWNTIME'] = event_log_for_gantt['DOWNTIME'] / 60
    event_log_for_gantt['UPTIME'] = event_log_for_gantt['UPTIME'] / 60

    # todo: append non-constraint and constraint level data for [Gantt_Data]
    gantt_data = gantt_data_temp.append(gantt_data)
    gantt_data['Server'] = site_params['SiteServer']      # NEEDS TO BE VERIFIED

    # todo: (exception handling) if downtime status can't be detected, allocate it to unplanned stop
    gantt_data['Downtime_Status'].fillna(2.3, inplace=True)

    # todo: (exception handling) correct StartTime if same with next row
    for i in range(1, len(gantt_data) - 1):
        if gantt_data['CO_Identifier'].iloc[i] == gantt_data['CO_Identifier'].iloc[i + 1] and gantt_data['Machine'].iloc[i] == gantt_data['Machine'].iloc[i + 1]:
            if gantt_data['StartTime'].iloc[i] > gantt_data['StartTime'].iloc[i + 1] and gantt_data['Downtime_Status'].iloc[i] != gantt_data['Downtime_Status'].iloc[i + 1]:
                logging.info(i)
                if gantt_data['StartTime'].iloc[i] > (gantt_data['StartTime'].iloc[i - 1] + pd.Timedelta(seconds=1)):
                    gantt_data['StartTime'].iloc[i] = gantt_data['StartTime'].iloc[i] - pd.Timedelta(seconds=1)

    # todo: (exception handling) remove non-used columns in even log before appending
    event_log_for_gantt = event_log_for_gantt.drop(columns=['Previous_Uptime_End', 'CO_Event', 'PUDesc', 'MACHINE',
                                                            'IS_CONSTRAINT', 'IS_STOP', 'Planned_Stop_Check', 'Idle_Check',
                                                            'END_TIME', 'START_TIME_of_Uptime'], errors='ignore')

    # todo: append non-constraint and constraint level data for [Event_log_for_Gantt]
    event_log_for_gantt = event_log_for_gantt_temp.append(event_log_for_gantt)

    # todo: clean up columns
    event_log_for_gantt = event_log_for_gantt[['START_TIME', 'DOWNTIME', 'UPTIME', 'Fault', 'CAUSE_LEVELS_1_NAME',
                                               'CAUSE_LEVELS_2_NAME', 'CAUSE_LEVELS_3_NAME', 'CAUSE_LEVELS_4_NAME', 'BRANDCODE',
                                               'OPERATOR_COMMENT', 'LINE', 'MACHINE', 'downtime_id', 'CO_Identifier']]

    event_log_for_gantt['Server'] = site_params['SiteServer']     # NEEDS TO BE VERIFIED
    event_log_for_gantt['DOWNTIME'] = event_log_for_gantt['DOWNTIME'].apply(lambda x: round(x))
    event_log_for_gantt['UPTIME'] = event_log_for_gantt['UPTIME'].apply(lambda x: round(x))

    return event_log_for_gantt, gantt_data
