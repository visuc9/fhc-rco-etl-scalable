from datetime import datetime
from dateutil import relativedelta as rd
import logging


def get_analysis_time_bounds(engine, site_name):
    # Get current time
    sys_time = datetime.now()
    current_hour = sys_time.hour
    # todo: perhaps un-hardcode the table name
    time_query = """SELECT MAX(Data_Update_Time) FROM rco_v1_script_data WHERE Server= ?"""
    last_update = engine.execute(time_query, site_name).first()[0]
    if not last_update:
        last_update = sys_time  # if we have nothing for this Site, get last X days from today.
        # todo make this longer - like 3 months?.

    """
    COMMENTING AND CHANGING IT TO MATCH CURRENT R SCRIPT
    if current_hour != 3:  # if we're running this in the hour of 3am, get last 7 days instead of 3.
        # todo there has to be a better way to do accomplish this.
        start_time = last_update + rd.relativedelta(days=-3)
    else:
        start_time = last_update + rd.relativedelta(days=-7)
    """
    if current_hour == 3:
        start_time = last_update + rd.relativedelta(days=-7)
        logging.info('Number of days data extracted: 7')
        logging.info('Run Machine Level Analysis: no')
        logging.info('Brandcode Data Updating Active Status: yes')
        logging.info('Modification of Only New or Deleted COs Active Status: no')
    elif current_hour == 20:
        start_time = last_update + rd.relativedelta(days=-2)
        logging.info('Number of days data extracted: 2')
        logging.info('Brandcode Data Updating Active Status: no')
        logging.info('Modification of Only New or Deleted COs Active Status: no')
    elif current_hour == 21:
        start_time = last_update + rd.relativedelta(days=-14)
        logging.info('Number of days data extracted: 14')
        logging.info('Run Machine Level Analysis: no')
        logging.info('Brandcode Data Updating Active Status: yes')
        logging.info('Modification of Only New or Deleted COs Active Status: no')
    elif current_hour == 22:
        start_time = last_update + rd.relativedelta(days=-7)
        logging.info('Number of days data extracted: 7')
        logging.info('Brandcode Data Updating Active Status: yes')
        logging.info('Modification of Only New or Deleted COs Active Status: no')
    else:
        start_time = last_update + rd.relativedelta(days=-3)
        logging.info('Number of days data extracted: 3')
        logging.info('Brandcode Data Updating Active Status: no')
        logging.info('Modification of Only New or Deleted COs Active Status: yes')

    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = sys_time + rd.relativedelta(days=1)
    end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
    return start_time, end_time
