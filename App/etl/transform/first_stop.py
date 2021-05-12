import pandas as pd


def first_stop_analysis(site_params: pd.DataFrame, line_params: pd.DataFrame, line_dt_full: pd.DataFrame, co_event_log: pd.DataFrame, co_aggregated_data: pd.DataFrame) -> pd.DataFrame:
    """
    RCO_subETL_First_Stop_after_CO.R - copied over comments and line numbers for reference
    This sub-ETL looks at the Constraint Downtime Log per CO to identify the first Constraint Unplanned Stop after the CO.
    It logs these first stops after CO in [First_Stop_after_CO_Data] table. It also adds the [Total_Uptime_till_Next_CO] to [CO_Aggregate_Data], which is the total uptime in constraint until the next CO starts.
    This [Total_Uptime_till_Next_CO] is used later in PowerBI if there is zero unplanned stop between two COs, to report the uptime after CO.
    In other words, [First_Stop_after_CO_Data] may not include a row for all COs.

    :params line_params:
    :params line_dt_full:
    :params co_aggregated_data:
    :return: first_stop_after_co_data:
    """

    # todo write first stop analysis etl script
    co_aggregated_data['Total_Uptime_till_Next_CO'] = 0
    first_co_flag = 0

    for line_index in line_params['MDC_Line_Name'].index:
        # todo: filter data per line
        co_aggregated_data_temp = co_aggregated_data[co_aggregated_data['LINE'] == line_params['MDC_Line_Name'][line_index]]
        line_dt_temp = line_dt_full[line_dt_full['LINE'] == line_params['MDC_Line_Name'][line_index]]
        co_aggregated_data_temp.sort_values(by='CO_StartTime', inplace=True)

        if len(co_aggregated_data_temp) > 0:
            # todo: if there is at least one CO for the given line, perform the analysis per CO.
            for i in range(len(co_aggregated_data_temp)):
                # todo: define the time range to explore. By default, it is the time between end of CO till start of next CO.
                #  If it's the last CO, then only look at 30mins after the CO.
                # RENAMING temp1 -> co_end_time TO MAKE IT MORE READABLE
                # RENAMING temp2 -> next_co_start_time TO MAKE IT MORE READABLE
                co_end_time = co_aggregated_data_temp['CO_EndTime'].iloc[i]
                if len(co_aggregated_data_temp) > 1 and i < len(co_aggregated_data_temp) - 1:
                    next_co_start_time = co_aggregated_data_temp['CO_StartTime'].iloc[i + 1]
                else:
                    next_co_start_time = co_end_time + pd.Timedelta(seconds=30 * 60 * 60 * 24)

                # filter downtime events for the given CO
                line_stops = line_dt_temp[(line_dt_temp['START_TIME'] >= co_end_time)
                                          & (line_dt_temp['START_TIME'] < next_co_start_time)]
                line_stops_all = line_stops
                total_uptime_till_next_co = 0

                if len(line_stops) > 0:
                    line_stops['Uptime_cumul'] = 0
                    line_stops['UptimeDowntime_cumul'] = 0

                    line_stops['Uptime_cumul'].iloc[0] = line_stops['UPTIME'].iloc[0]
                    line_stops['UptimeDowntime_cumul'].iloc[0] = line_stops['UPTIME'].iloc[0] + line_stops['DOWNTIME'].iloc[0]

                    # Calculate the cumulative uptime and downtime per downtime event
                    if len(line_stops) > 1:
                        for j in range(1, len(line_stops)):
                            line_stops['Uptime_cumul'].iloc[j] = line_stops['Uptime_cumul'].iloc[j - 1] + line_stops['UPTIME'].iloc[j]
                            line_stops['UptimeDowntime_cumul'].iloc[j] = line_stops['UptimeDowntime_cumul'].iloc[j - 1] + line_stops['DOWNTIME'].iloc[j]

                    line_stops['UptimeDowntime_at_Start'] = line_stops['UptimeDowntime_cumul'] - line_stops['DOWNTIME']
                    line_stops.drop(columns='UptimeDowntime_cumul', inplace=True)
                    line_stops['CO_Identifier'] = co_aggregated_data_temp['CO_Identifier'].iloc[i]

                    # filter only unplanned stops that are not to be excluded from PR calculations.
                    line_stops = line_stops[(line_stops['Planned_Stop_Check'] == 0) & (line_stops['Idle_Check'] == 0)
                                            & (line_stops['isExcluded'] == False)]

                    # if at least one unplanned stop is found, append its first row (i.e. first stop after CO) to [First_Stop_after_CO_cumul] (i.e. temporary log First Stop after CO).
                    # REPLACING TEMP DATAFRAME 'first_stop_after_co_cumul' WITH THE FINAL DATAFRAME 'first_stop_after_co_data'
                    if len(line_stops) > 0:
                        if first_co_flag == 0:
                            first_stop_after_co_data = line_stops.iloc[0].to_frame().transpose()
                            first_co_flag = 1
                        else:
                            first_stop_after_co_data = first_stop_after_co_data.append(line_stops.iloc[0])

                    # calculate total uptime until next CO by summing up the uptime for all events (i.e. NOT only the unplanned stops).
                    total_uptime_till_next_co = sum(line_stops_all['UPTIME'])

                # add the total uptime until next CO to [CO_Aggregate_Data] IF it's NOT the last CO for a given line.
                if i < (len(co_aggregated_data_temp) - 1):
                    temp = co_event_log[co_event_log['CO_Identifier'] == co_aggregated_data_temp['CO_Identifier'].iloc[i + 1]]
                    temp = temp.sort_values(by='START_TIME')
                    total_uptime_till_next_co = total_uptime_till_next_co + temp['UPTIME'].iloc[0]      # REMOVED SUM() FROM R CODE AS IT'S CALLED ON A SINGLE VALUE

                # total_uptime_till_next_co = round(total_uptime_till_next_co, 2)
                co_aggregated_data.loc[co_aggregated_data['CO_Identifier'] == co_aggregated_data_temp['CO_Identifier'].iloc[i], 'Total_Uptime_till_Next_CO'] = round(total_uptime_till_next_co, 2)

    # clean up columns & round up numerical columns
    first_stop_after_co_data = first_stop_after_co_data[['START_TIME', 'DOWNTIME', 'UPTIME', 'Uptime_cumul', 'Fault',
                                                         'CAUSE_LEVELS_1_NAME', 'CAUSE_LEVELS_2_NAME', 'CAUSE_LEVELS_3_NAME',
                                                         'CAUSE_LEVELS_4_NAME', 'BRANDCODE', 'OPERATOR_COMMENT', 'LINE',
                                                         'downtime_id', 'CO_Identifier']]
    first_stop_after_co_data['Server'] = site_params['SiteServer']
    first_stop_after_co_data['DOWNTIME'] = first_stop_after_co_data['DOWNTIME'].apply(lambda x: round(x, 2))
    first_stop_after_co_data['UPTIME'] = first_stop_after_co_data['UPTIME'].apply(lambda x: round(x, 2))
    first_stop_after_co_data['Uptime_cumul'] = first_stop_after_co_data['Uptime_cumul'].apply(lambda x: round(x, 2))


    return first_stop_after_co_data


def sud_first_stop(df1):
    print('sud_foo')
    # todo write sud first stop after changeover script
    return None


def sud_converter_constraint_stops_after_co():
    return None
