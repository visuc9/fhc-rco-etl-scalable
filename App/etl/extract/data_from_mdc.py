import numpy as np
import pandas as pd
import logging

from App import mdc


def dt_data_extract(mdc_header: dict, params: dict, line_params: pd.DataFrame, start_time, end_time):
    # pull all downtime data for site from MDC
    full_data = mdc.get_raw_data(headers=mdc_header, table='dt', site=params['SiteMDCName'], startDate=start_time,
                                 endDate=end_time, line=','.join(line_params['MDC_Line_Name']))
    # Line = comma separated list of all 'MDC_Line_Name' in Line Config in Sharepoint

    logging.info('Machine Downtime log extracted')

    # todo: Why did I choose to align to MAPLE column names? this is an objectively terrible idea.
    #  Likely cause: Current tool is based on MAPLE data model and we'd have to change things about the PowerBI
    #  which we should be doing anyway. Fix this before it becomes harder to un-do.

    column_mapping = {
        'downtimeId': 'downtime_id',
        'downtimeCategory': 'dtCategory',
        'fault.fault': 'Fault',
        'time.startTime': 'START_TIME',
        'time.endTime': 'END_TIME',
        'product.productCode': 'BRANDCODE',
        'product.productDescription': 'ProdDesc',
        'order.processOrder': 'ProcessOrder',
        'reason.reason1': 'CAUSE_LEVELS_1_NAME',
        'reason.reason2': 'CAUSE_LEVELS_2_NAME',
        'reason.reason3': 'CAUSE_LEVELS_3_NAME',
        'reason.reason4': 'CAUSE_LEVELS_4_NAME',
        'equipment.lineName': 'LINE',
        'equipment.unitName': 'MACHINE',
        'time.duration': 'DOWNTIME',
        'time.uptime': 'UPTIME',
        'people.crew': 'TEAM',
        'people.shift': 'SHIFT',
        'reason.comment': 'OPERATOR_COMMENT',
        'schedule.productionStatusReason': 'LineStatus',
        'schedule.nptStartTime': 'NPTStartTime',
        'schedule.nptEndTime': 'NPTEndTime'
    }

    line_downtime = None
    line_downtime_full = None
    machine_downtime = None
    machine_downtime_full = None

    if full_data is not None:

        less_data = full_data.loc[:, column_mapping.keys()]  # reduce columns to ones we care about
        less_data['time.duration'] = less_data['time.duration'].divide(60, fill_value=0,
                                                                       axis=0)  # convert from seconds to minutes
        less_data['time.uptime'] = less_data['time.uptime'].divide(60, fill_value=0,
                                                                   axis=0)  # convert from seconds to minutes
        renamed_data = less_data.rename(
            columns=column_mapping)  # rename the columns to easier ones to deal with

        # Isolate downtime that is on the Constraint Machine as defined in Line Params
        # Assumption: MACHINE column is always Line Name + Constraint Machine Cause Level 1 Name
        line_params['Constraint_MACHINE'] = line_params['MDC_Line_Name'] + ' ' + line_params['Constraint_Machine_String']
        '''
        COMMENTED AND REPLACED WITH THE LINE BELOW SINCE IT WAS BREAKING
        LAMBDA FUNCTIONS ARE WORKING WHEN APPLIED ON A SERIES AND NOT THE ENTIRE DATAFRAME
        renamed_data['isConstraint'] = renamed_data.apply(
        lambda x: True if x.MACHINE.isin(line_params['Constraint_MACHINE']) else False)
        '''
        renamed_data['isConstraint'] = np.where(renamed_data['MACHINE'].isin(line_params['Constraint_MACHINE']), True, False)
        # Determine if DT is 'PR Out' or 'Excluded' - LineStatus will be 'PR Out: Reason' if not PR In
        '''
        COMMENTED AND REPLACED WITH THE LINES BELOW SINCE IT WAS BREAKING
        LAMBDA FUNCTIONS ARE WORKING WHEN APPLIED ON A SERIES AND NOT THE ENTIRE DATAFRAME
        renamed_data['isExcluded'] = renamed_data.apply(lambda x: False if x.LineStatus.astype(str) == 'None' else True)
        renamed_data['isStop'] = renamed_data.apply(
            lambda x: False if any(item in x.dtCategory for item in ['DTMach-Blocked', 'DTMach-Starved']) else True)
        '''
        renamed_data['isExcluded'] = np.where(renamed_data['LineStatus'].astype(str) == 'None', False, True)
        renamed_data['isStop'] = renamed_data['dtCategory'].apply(lambda x: False if any(item in x for item in ['DTMach-Blocked', 'DTMach-Starved']) else True)

        # ADDING Planned_Stop_Check & Idle_Check
        # renamed_data['Planned_Stop_Check'] = np.where('Planned' in renamed_data['dtCategory'].astype(str), 1, 0)
        renamed_data['Planned_Stop_Check'] = renamed_data['dtCategory'].apply(lambda x: 1 if any('Planned' in item for item in x) else 0)
        renamed_data['Planned_Stop_Check'].fillna(0, inplace=True)
        renamed_data['Idle_Check'] = np.where(renamed_data['isExcluded'], 1, 0)

        # Extract Line Downtime
        # line_downtime_full = renamed_data.loc[:, renamed_data['isConstraint'] == True]
        line_downtime_full = renamed_data[renamed_data['isConstraint'] == True]

        if params['MultiConstraint'] and line_downtime_full is not None:
            # if special multi-constraint logic will be run, remove items with identical start times on the same 'Line'
            line_downtime_full['temp'] = line_downtime_full['START_TIME'] + line_downtime_full['LINE']
            line_downtime_full = line_downtime_full.drop_duplicates('temp', inplace=True)

        # get rid of records with 'PR Out' ... what if DT is only partial PR Out? watch for under-counted downtime.
        # MODIFYING HOW SUBSET IS PASSED
        line_downtime = line_downtime_full.loc[line_downtime_full['isExcluded'] == False, :].dropna(subset=['START_TIME'])

        if params['MachineLevel'] and renamed_data is not None:  # need all the data if doing Machine Level Data.
            # Note: in Onur's Script he assigns 'Machine_Downtime_Full' to the one where he excludes Idle Events
            machine_downtime_full = renamed_data.dropna(subset=['START_TIME'])      # MODIFYING HOW SUBSET IS PASSED

            if not params['MultiConstraint']:
                machine_downtime_full = machine_downtime_full[machine_downtime_full['isConstraint'] == False]
                # todo: make sure the 'MultiConstraint' param is the only one needed....

            machine_downtime = machine_downtime_full[machine_downtime_full['isExcluded'] == False]

    print('bar')
    return line_downtime, line_downtime_full, machine_downtime, machine_downtime_full


def prod_data_extract(mdc_header: dict, params: dict, line_params: pd.DataFrame, start_time, end_time):
    full_data = mdc.get_raw_data(headers=mdc_header, table='pe', site=params['SiteMDCName'], startDate=start_time,
                                 endDate=end_time, line=','.join(line_params['MDC_Line_Name']))     # CHANGING LINE_PARAMS
    # Line = comma separated list of all 'MDC_Line_Name' in Line Config in Sharepoint

    logging.info('Production log extracted')

    # ['tenantId', 'enterpriseId', 'siteId', 'puId', 'eventId', 'eventNum',
    #  '_links.self.href', 'time.startTime', 'time.startTimeUTC',
    #  'time.chainStartTime', 'time.endTime', 'time.endTimeUTC',
    #  'equipment.businessUnit', 'equipment.companyName',
    #  'equipment.enterpriseName', 'equipment.siteName', 'equipment.areaName',
    #  'equipment.lineName', 'equipment.unitName',
    #  'productPlanned.productFamilyDescription',
    #  'productPlanned.productFamilyDescriptionGlobal',
    #  'productPlanned.productDescription',
    #  'productPlanned.productDescriptionGlobal', 'productPlanned.productCode',
    #  'productPlanned.productId', 'productActual.productFamilyDescription',
    #  'productActual.productFamilyDescriptionGlobal',
    #  'productActual.productDescription',
    #  'productActual.productDescriptionGlobal', 'productActual.productCode',
    #  'productActual.productId', 'order.bomName', 'order.processOrder',
    #  'order.path', 'people.shift', 'people.crew', 'people.user',
    #  'schedule.productionStatusReason', 'schedule.nptStartTime',
    #  'schedule.nptEndTime', 'schedule.productionStatusReasonGlobal',
    #  'eventDetails.enteredOn', 'eventDetails.productionCount',
    #  'eventDetails.inventoryCount', 'eventDetails.initialQuantityX',
    #  'eventDetails.initialQuantityY', 'eventDetails.initialQuantityZ',
    #  'eventDetails.initialQuantityA', 'eventDetails.finalQuantityX',
    #  'eventDetails.finalQuantityY', 'eventDetails.finalQuantityZ',
    #  'eventDetails.finalQuantityA', 'eventDetails.category',
    #  'eventDetails.productionVariable',
    #  'eventDetails.productionVariableValue',
    #  'dataAvailability.mdcCreatedTime', 'dataAvailability.mdcUpdatedTime',
    #  'dataAvailability.paModifiedTime']

    column_mapping = {
        'eventId': 'event_id',
        'time.startTime': 'START_TIME',
        'time.endTime': 'END_TIME',
        'time.chainStartTime': 'Chain_Start_Time',
        'productPlanned.productCode': 'BRANDCODE_PLANNED',
        'productActual.productCode': 'BRANDCODE',                   # CHANGED | TO BE VERIFIED
        'order.processOrder': 'Process_Order',
        'equipment.lineName': 'LINE',
        'equipment.unitName': 'MACHINE',
        'people.crew': 'TEAM',
        'people.shift': 'SHIFT',
        'schedule.productionStatusReason': 'LineStatus',
        'schedule.nptStartTime': 'NPTStartTime',
        'schedule.nptEndTime': 'NPTEndTime',
        'eventDetails.initialQuantityY': 'initial_Quantity',        # CHANGED
        'eventDetails.finalQuantityX': 'final_Quantity',
        'productActual.productDescription': 'ProdDesc',             # TO BE VERIFIED
        'productActual.productFamilyDescription': 'ProdFam',        # TO BE VERIFIED
        'productActual.productId': 'ProdGroup',                     # TO BE VERIFIED
        'eventDetails.initialQuantityX': 'FirstPackCount',          # TO BE VERIFIED
        'eventDetails.productionCount': 'StatFactor'                # TO BE VERIFIED
    }

    pr_in_data = None

    if full_data is not None:
        less_data = full_data.loc[:, column_mapping.keys()]  # reduce columns to ones we care about
        # todo calculate duration of production event using starttime and endtime
        renamed_data = less_data.rename(
            columns=column_mapping)  # rename the columns to easier ones to deal with

        # get rid of records with 'PR Out' ... what if DT is only partial PR Out? watch for under-counted downtime.
        pr_in_data = renamed_data.loc[renamed_data['LineStatus'].astype(str) == 'In Production', :]

    if full_data is not None and pr_in_data is not None:
        return pr_in_data
    else:
        return None


def runtime_data_extract(mdc_header: dict, params: dict, line_params: pd.DataFrame, start_time, end_time):
    """
    RUNTIME PER DAY data generation

    :params mdc_header:
    :params params:
    :params line_params:
    :params start_time:
    :params end_time:
    :return: None:
    """
    full_data = mdc.get_raw_data(headers=mdc_header, table='dt', site=params['SiteMDCName'], startDate=start_time,
                                 endDate=end_time, line=','.join(line_params['MDC_Line_Name']))     # TO BE VERIFIED
    logging.info('Scheduled Time data extracted')

    column_mapping = {
        'downtimeId': 'downtime_id',
        'downtimeCategory': 'dtCategory',
        'fault.fault': 'Fault',
        'time.startTime': 'START_TIME',
        'time.endTime': 'END_TIME',
        'product.productCode': 'BRANDCODE',
        'product.productDescription': 'ProdDesc',
        'order.processOrder': 'ProcessOrder',
        'reason.reason1': 'CAUSE_LEVELS_1_NAME',
        'reason.reason2': 'CAUSE_LEVELS_2_NAME',
        'reason.reason3': 'CAUSE_LEVELS_3_NAME',
        'reason.reason4': 'CAUSE_LEVELS_4_NAME',
        'equipment.lineName': 'LINE',
        'equipment.unitName': 'MACHINE',
        'time.duration': 'DOWNTIME',
        'time.uptime': 'UPTIME',
        'people.crew': 'TEAM',
        'people.shift': 'SHIFT',
        'reason.comment': 'OPERATOR_COMMENT',
        'schedule.productionStatusReason': 'LineStatus',
        'schedule.nptStartTime': 'NPTStartTime',
        'schedule.nptEndTime': 'NPTEndTime'
    }

    full_data.rename(columns=column_mapping, inplace=True)
    full_data['START_TIME'] = pd.to_datetime(full_data['START_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')
    full_data['Date'] = pd.to_datetime(full_data['START_TIME'].dt.date)
    full_data['UPTIME'] = pd.to_numeric(full_data['UPTIME'])
    full_data = full_data.merge(line_params, on='LINE')

    runtime_per_day_data = full_data.groupby(by=['Date', 'LINE'], as_index=False).agg(Runtime=('UPTIME', sum))      # TO BE VERIFIED

    if not params['SUDSpecific']:
        full_data = mdc.get_raw_data(headers=mdc_header, table='dt', site=params['SiteMDCName'], startDate=start_time,
                                 endDate=end_time, line=','.join(line_params['MDC_Line_Name']))     # TO BE VERIFIED
        full_data.rename(columns=column_mapping, inplace=True)
        full_data['START_TIME'] = pd.to_datetime(full_data['START_TIME'], format='%Y-%m-%dT%H:%M:%S.%f')
        full_data['Date'] = pd.to_datetime(full_data['START_TIME'].dt.date)
        full_data['DOWNTIME'] = pd.to_numeric(full_data['DOWNTIME'])
        full_data = full_data.merge(line_params, on='LINE')
        msu_per_day_data = full_data.groupby(by=['Date', 'LINE'], as_index=False).agg(Production_MSU=('DOWNTIME', sum)) # TO BE VERIFIED.

        runtime_per_day_data = runtime_per_day_data.merge(msu_per_day_data, on=['Date', 'LINE'], how='left')

    full_data = full_data.groupby(by=['Date', 'LINE'], as_index=False).agg(START_TIME=('START_TIME', min))

    # todo: get Dat startTime per Line - this is needed to automatically detect per line, at what hour the production day starts.
    #  This info is later used in PowerBI when grouping COs in production days.
    full_data['Shift_Start_hours'] = pd.to_numeric((full_data['START_TIME'] - full_data['Date']) / pd.Timedelta('1 hour'))
    full_data = full_data.groupby(by=['LINE', 'Shift_Start_hours'], as_index=False).agg('LINE', 'count')
    temp = full_data.groupby(by='LINE', as_index=False).agg(max_tally=('tally', max))
    full_data = full_data.merge(temp, on='LINE')
    full_data = full_data[full_data['tally'] > full_data['max_tally'] / 2]
    day_starttime_per_line = full_data.groupby(by='LINE').agg(Day_Start_hours=('Shift_Start_hours', min))
    if params['SiteServer'] == 'Gebze HDW':
        day_starttime_per_line = full_data.groupby(by='LINE', as_index=False).agg(Day_Start_hours=('Shift_Start_hours', max))

    return runtime_per_day_data, day_starttime_per_line


def brandcode_data_extract(params: dict, prod_data: pd.DataFrame) -> pd.DataFrame:
    """
    BRANDCODE Data Generation - this part is more complex for Proficy than Maple, as iODS does not have a [BRANDCODE] table.
    Here we are trying to re-create it from the columns available from [PRODUCTION_LOG], as well as re-creating some
    other columns via site-specific rules.

    :params params:
    :params prod_data:
    :return: brandcode_data:
    """

    # todo: Create BRANDCODE meta data from PRODUCTION LOG
    temp = prod_data[['BRANDCODE', 'ProdDesc', 'ProdFam', 'ProdGroup', 'FirstPackCount', 'StatFactor']]
    temp2 = temp.groupby(by=['BRANDCODE', 'ProdDesc', 'ProdFam', 'ProdGroup', 'FirstPackCount'], as_index=False)\
                .agg(tally=('StatFactor', 'count'))\
                .sort_values(by='tally', ascending=False)\
                .drop_duplicates('BRANDCODE')
    temp3 = temp[temp['StatFactor'] > 0]
    temp3 = temp3.groupby(by='BRANDCODE', as_index=False).agg(StatFactor=('StatFactor', max))
    temp3.dropna(subset=['BRANDCODE'], inplace=True)
    temp2 = temp2.merge(temp3, on='BRANDCODE', how='left')
    temp2['ProdDesc'] = temp2['ProdDesc'].str.replace('-', ':', regex=True)
    temp2[['A', 'B']] = temp2['ProdDesc'].str.split(':', expand=True)
    temp2['B'] = np.where(temp2['B'].isna(), temp2['A'], temp2['B'])
    temp3 = pd.unique(temp2['B'])
    if params['SiteServer'] == 'Gebze HDW':
        temp2.rename(columns={'ProdDesc': 'BRANDNAME'}, inplace=True)
    else:
        if len(temp3) > 2:
            temp2.rename(columns={'B': 'BRANDNAME'}, inplace=True)
        else:
            temp2.rename(columns={'ProdDesc': 'BRANDNAME'}, inplace=True)

    brandcode_data = temp2[['BRANDCODE', 'BRANDNAME', 'ProdFam', 'ProdGroup', 'FirstPackCount', 'StatFactor']]
    brandcode_data.rename(columns={'FirstPackCount': 'UNITS_PER_CASE'}, inplace=True)
    brandcode_data['Server'] = params['SiteServer']

    # todo: add [SIZE] and make fixes per server if needed
    if params['SiteServer'] == 'Tabler HDW':
        temp2[['A', 'B']] = brandcode_data['BRANDNAME'].str.split('/', expand=True)
        temp2[['C', 'D']] = temp2['B'].str.split(' ', expand=True)
        temp2['Case_Count'] = ''
        temp2['Case_Count'] = temp2['A'].apply(lambda x: x.str.split(' ')[-1])
        temp2['Case_Count'] = pd.to_numeric(temp2['Case_Count'])
        temp2['UNITS_PER_CASE'] = np.where(temp2['UNITS_PER_CASE'].isna(), temp2['Case_Count'], temp2['UNITS_PER_CASE'])
        temp2.rename(columns={'C': 'SIZE'}, inplace=True)
        temp2 = temp2[['BRANDCODE', 'UNITS_PER_CASE', 'SIZE']]
        brandcode_data = brandcode_data.drop(columns='UNITS_PER_CASE', errors='ignore')
        brandcode_data = brandcode_data.merge(temp2, on='BRANDCODE', how='left')

    elif params['SiteServer'] == 'StLouis':
        temp = brandcode_data
        temp['BRANDNAME'] = temp['BRANDNAME'].str.replace('w/Oxi', 'wOxi', regex=True)
        temp2[['A', 'B']] = temp['BRANDNAME'].str.split('/', expand=True)
        temp2[['B', 'C']] = temp2['B'].str.split(' ', expand=True)
        temp2['B'] = temp2['B'].str.replace('ct', '', regex=True)
        temp2['B'] = pd.to_numeric(temp2['B'])
        temp2['A'] = temp2.A.str[-1]
        temp2['A'] = pd.to_numeric(temp2['A'])
        temp2.rename(columns={'A': 'UNITS_PER_CASE', 'B': 'SIZE'})

        temp2 = temp2[['BRANDCODE', 'UNITS_PER_CASE', 'SIZE']]
        brandcode_data = brandcode_data.drop(columns='UNITS_PER_CASE', errors='ignore')
        brandcode_data = brandcode_data.merge(temp2, on='BRANDCODE', how='left')

    elif params['SiteServer'] == 'Gebze HDW':
        temp = brandcode_data
        temp2[['A', 'B']] = temp.BRANDNAME.str.split('X', expand=True)
        temp2['temp'] = temp2.B.str[0:5]
        temp2['temp2'] = temp2['temp'].apply(lambda x: 'ML' if 'ML' in x else ('L' if 'L' in x else ''))
        temp2[['B', 'C']] = temp2.temp.str.split('ML', expand=True)
        temp2[['B', 'C']] = temp2.B.str.split('L', expand=True)
        temp2['B'] = temp2.B.str.replace('[^0-9.-]', '', regex=True)
        temp2['B'] = pd.to_numeric(temp2['B'])
        temp2['Size_Raw'] = temp2['B']
        temp2['B'].fillna(0, inplace=True)
        for i in range(len(temp2)):
            if temp2['temp2'].iloc[i] == 'L' or temp2['B'].iloc[i] < 100:
                temp2['Size_Raw'].iloc[i] = temp2['Size_Raw'].iloc[i] * 1000
            if pd.isna(temp2['temp'].iloc[i]):
                temp3 = pd.to_numeric(temp2['BRANDNAME'].iloc[i].replace('[^0-9.-]', '', regex=True))
                if pd.isna(temp3):
                    temp3 = 0
                if temp3 > 100:
                    temp2['Size_Raw'].iloc[i] = temp3
                else:
                    temp2['Size_Raw'].iloc[i] = temp3 * 1000

        temp2.rename(columns={'Size_Raw': 'SIZE'}, inplace=True)
        temp2 = temp2[['BRANDCODE', 'SIZE']]
        brandcode_data = brandcode_data.merge(temp2, on='BRANDCODE', how='left')

    else:
        brandcode_data['SIZE'] = None

    return brandcode_data
