import combine_gtfs_feeds.cli.log_controller as log_controller
import pandas as pd
import numpy as np
import os as os
import argparse
import sys
from datetime import datetime, timedelta
import time

def add_run_args(parser, multiprocess=True):
    """
    Run command args
    """
    parser.add_argument('-g', '--gtfs_dir',
                        type=str,
                        metavar='PATH',
                        help='path to GTFS dir (default: %s)' % os.getcwd())

    parser.add_argument('-s', '--service_date',
                        type=int,
                        metavar='SERVICEDATE',
                        help='date for service in yyyymmdd integer format\
                       (default: %s)'
                        % os.getcwd())

    parser.add_argument('-o', '--output_dir',
                        type=str,
                        metavar='PATH',
                        help='path to ourput directory (default: %s)'
                        % os.getcwd())


def get_service_ids(calendar, calendar_dates, day_of_week, service_date):
    """
    Returns a list of valid service_id(s) from each feed
    using the user specified service_date.
    """

    regular_service_dates = calendar[(calendar['start_date'] <= service_date
                                      ) & (calendar['end_date'] >= service_date
                                           ) & (calendar[day_of_week] == 1
                                                )]['service_id'].tolist()
    exceptions_df = calendar_dates[calendar_dates['date'] == service_date]
    add_service = exceptions_df.loc[exceptions_df
                                    ['exception_type'] == 1][
                                        'service_id'].tolist()
    remove_service = exceptions_df[exceptions_df
                                   ['exception_type'] == 2][
                                       'service_id'].tolist()
    service_id_list = [x for x in (
        add_service + regular_service_dates) if x not in remove_service]

    return service_id_list


def create_id(df, feed, id_column):
    """
    Changes id_column by prepending each value with 
    the feed parameter.
    """
    df[id_column] = feed + '_' + df[id_column].astype(str)
    return df


def dt_to_yyyymmdd(dt_time):
    """
    Converts a date time object to 
    YYYYMMDD format.
    """
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day


def get_start_end_date(my_date):
    """
    Gets the day before and after
    the user parameter service_date
    in YYYYMMDD format. 
    """
    start_date = dt_to_yyyymmdd(my_date - timedelta(days=1))
    end_date = dt_to_yyyymmdd(my_date + timedelta(days=1))
    return start_date, end_date


def merge_and_export(feed_dict, output_loc,  file_list):
    """
    Merges each GTFS file/df into one
    then exports to user paramter output_dir
    """
    for file_name in file_list:
        df = pd.DataFrame()
        for feed in feed_dict:
            df = pd.concat([df, feed_dict[feed][file_name]])
        df.to_csv(os.path.join(output_loc, file_name + '.txt'), index=None)


def get_weekday(my_date):
    """
    Gets the day of week from user parameter
    service date. 
    """
    week_days = ['monday', 'tuesday', 'wednesday',
                 'thursday', 'friday', 'saturday', 'sunday']
    return week_days[my_date.weekday()]


def convert_to_seconds(value):
    """
    Converts hh:mm:ss format to number
    of seconds after midnight. 
    """
    h, m, s = value.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


def to_hhmmss(value):
    """
    Converts to hhmmss format.
    """
    return time.strftime('%H:%M:%S', time.gmtime(value))


def frequencies_to_trips(frequencies, trips, stop_times):
    """
    For each trip_id in frequencies.txt, calculates the number
    of trips and creates records for each trip in trips.txt and 
    stop_times.txt. Deletes the original represetative trip_id
    in both of these files. 
    """

    frequencies['start_time_secs'] = frequencies['start_time'].apply(
        convert_to_seconds)

    frequencies['end_time_secs'] = frequencies['end_time'].apply(
        convert_to_seconds)

    # following assumes that the total number of trips
    # includes a final one that leaves first
    # stop at end_time in frequencies
    frequencies['total_trips'] = ((
        (frequencies['end_time_secs']-frequencies
         ['start_time_secs']) / frequencies['headway_secs']) + 1).astype(int)

    trips_update = trips.merge(frequencies, on='trip_id')
    trips_update = trips_update.loc[trips_update.index.repeat(
        trips_update['total_trips'])].reset_index(drop=True)
    trips_update['counter'] = trips_update.groupby('trip_id').cumcount() + 1
    trips_update['trip_id'] = trips_update['trip_id'].astype(str) + '_' + trips_update['counter'].astype(str)

    stop_times_update = stop_times.merge(frequencies, on='trip_id')
    stop_times_update['arrival_time_secs'] = stop_times_update[
        'arrival_time'].apply(convert_to_seconds)
    stop_times_update['departure_time_secs'] = stop_times_update[
        'departure_time'].apply(convert_to_seconds)
    stop_times_update = stop_times_update.loc[
        stop_times_update.index.repeat(
            stop_times_update['total_trips'])].reset_index(drop=True)
    stop_times_update['counter'] = stop_times_update.groupby(
        ['trip_id', 'stop_id']).cumcount()
    stop_times_update['departure_time_secs'] = stop_times_update[
        'departure_time_secs'] + (stop_times_update[
            'counter'] * stop_times_update['headway_secs'])
    stop_times_update['arrival_time_secs'] = stop_times_update[
        'arrival_time_secs'] + (stop_times_update[
            'counter'] * stop_times_update['headway_secs'])
    stop_times_update['departure_time'] = stop_times_update[
        'departure_time_secs'].apply(to_hhmmss)
    stop_times_update['arrival_time'] = stop_times_update[
        'arrival_time_secs'].apply(to_hhmmss)
    stop_times_update['counter'] = stop_times_update['counter'] + 1
    stop_times_update['trip_id'] = stop_times_update[
        'trip_id'].astype(str) + '_' + stop_times_update['counter'].astype(str)

    # remove trip_ids that are in frequencies
    stop_times = stop_times[~stop_times['trip_id'].isin(
        frequencies['trip_id'])]

    trips = trips[~trips['trip_id'].isin(frequencies['trip_id'])]

    # get rid of some columns
    stop_times_update = stop_times_update[stop_times.columns]
    trips_update = trips_update[trips.columns]

    # add new trips/stop times
    trips = pd.concat([trips, trips_update])
    stop_times = pd.concat([stop_times, stop_times_update])

    return trips, stop_times


def run(args):
    """
    Implements the 'run' sub-command, which combines
    gtfs files from each feed and writes them out to 
    a single feed. 
    """
    output_loc = args.output_dir

    if not os.path.isdir(output_loc):
         print('Output Directory path : {} does not exist.'.format(output_loc))
         print('Exiting application early!')
         sys.exit()

    logger = log_controller.setup_custom_logger('main_logger', output_loc)
    logger.info(
        '------------------combine_gtfs_feeds Started----------------')

    dir = args.gtfs_dir
    
    service_date = args.service_date
    str_service_date = str(service_date)
    my_date = datetime(int(str_service_date[0:4]), int(
        str_service_date[4:6]), int(str_service_date[6:8]))
    gtfs_file_list = ['agency', 'trips',
                      'stop_times', 'stops', 'routes', 'shapes']

    logger.info('GTFS Directory path is: {}'.format(dir))
    logger.info('Output Directory path is: {}'.format(output_loc))
    logger.info('Service Date is: {}'.format(str_service_date))

    if not os.path.isdir(dir):
         logger.info('GTFS Directory path : {} does not exist.'.format(dir))
         logger.info('Exiting application early!')
         sys.exit()

    start_date, end_date = get_start_end_date(my_date)
    day_of_week = get_weekday(my_date)
    feed_list = next(os.walk(dir))[1]
    feed_dict = {}
    feed_path_list = list()

    if len(feed_list)==0:
        logger.info('There are no GTFS feeds in GTFS Directory path : {}.'.format(dir))
        logger.info('Exiting application early!')
        sys.exit()


    for feed in feed_list:
        feed_dict[feed] = {}
        # read data

        calendar = pd.read_csv(os.path.join(dir, feed, 'calendar.txt'))
        if os.path.exists(os.path.join(
                dir, feed, 'calendar_dates.txt')) is False:
            calendar_dates = pd.DataFrame(
                    columns=['service_id', 'date', 'exception_type'])
        else:
            calendar_dates = pd.read_csv(
                os.path.join(dir, feed, 'calendar_dates.txt'))

        service_id_list = get_service_ids(
            calendar, calendar_dates, day_of_week, service_date)

        if len(service_id_list) == 0:
            logger.info('There are no service ids for service\
                date {} for feed {}'.format(str_service_date, feed))
            logger.info('Exiting application early!')
            sys.exit()

        for id in service_id_list:
            logger.info('Adding service_id {} for feed {}'.format(id, feed))

        trips = pd.read_csv(os.path.join(dir, feed, 'trips.txt'))
        stops = pd.read_csv(os.path.join(dir, feed, 'stops.txt'))
        stop_times = pd.read_csv(os.path.join(dir, feed, 'stop_times.txt'))

        if os.path.exists(os.path.join(dir, feed, 'frequencies.txt')):
            
            frequencies = pd.read_csv(
                os.path.join(dir, feed, 'frequencies.txt'))
            if len(frequencies) > 0:
                logger.info('Feed {} contains frequencies.txt...'.format(feed)) 
                logger.info('...Unique trips will be added to outputs based on headways in frequencies.txt')
                trips, stop_times = frequencies_to_trips(frequencies, trips, stop_times)

        routes = pd.read_csv(os.path.join(dir, feed, 'routes.txt'))
        shapes = pd.read_csv(os.path.join(dir, feed, 'shapes.txt'))
        agency = pd.read_csv(os.path.join(dir, feed, 'agency.txt'))

        # create new IDs
        trips = create_id(trips, feed, 'trip_id')
        trips = create_id(trips, feed, 'route_id')
        trips = create_id(trips, feed, 'shape_id')
        stop_times = create_id(stop_times, feed, 'trip_id')
        stop_times = create_id(stop_times, feed, 'stop_id')
        stops = create_id(stops, feed, 'stop_id')
        routes = create_id(routes, feed, 'route_id')
        shapes = create_id(shapes, feed, 'shape_id')

        # trips
        trips = trips.loc[trips['service_id'].isin(service_id_list)]
        trips['service_id'] = 1
        trip_id_list = np.unique(trips['trip_id'].tolist())
        route_id_list = np.unique(trips['route_id'].tolist())
        shape_id_list = np.unique(trips['shape_id'].tolist())
        # stop times
        stop_times = stop_times.loc[stop_times['trip_id'].isin(trip_id_list)]
        stop_id_list = np.unique(stop_times['stop_id'].tolist())
        # stops
        stops = stops.loc[stops['stop_id'].isin(stop_id_list)]
        # routes
        routes = routes.loc[routes['route_id'].isin(route_id_list)]
        routes['route_short_name'].fillna(routes['route_id'], inplace=True)
        # shapes
        shapes = shapes.loc[shapes['shape_id'].isin(shape_id_list)]

        # pass data to the dictionary
        feed_dict[feed]['agency'] = agency
        feed_dict[feed]['trips'] = trips
        feed_dict[feed]['stop_times'] = stop_times
        feed_dict[feed]['stops'] = stops
        feed_dict[feed]['routes'] = routes
        feed_dict[feed]['shapes'] = shapes

    # calendar
    calendar = pd.DataFrame(columns=['service_id', 'monday', 'tuesday',
                                     'wednesday', 'thursday', 'friday',
                                     'saturday', 'sunday', 'start_date',
                                     'end_date'])

    calendar.loc[0] = 0
    calendar['service_id'] = 1
    calendar[day_of_week] = 1
    calendar['start_date'] = start_date
    calendar['end_date'] = end_date
    calendar.to_csv(os.path.join(output_loc, 'calendar.txt'), index=None)

    merge_and_export(feed_dict, output_loc, gtfs_file_list)

    logger.info('Finished running combine_gtfs_feeds')
    sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))
