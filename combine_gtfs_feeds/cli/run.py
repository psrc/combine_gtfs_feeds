import combine_gtfs_feeds.cli.log_controller as log_controller
import pandas as pd
import numpy as np 
import os as os
import argparse
import sys
from datetime import datetime, timedelta
import time



# Note: KC Metro has extra files - 'block': block, 'block_trip': block_trip

def add_run_args(parser, multiprocess=True):
    """Run command args
    """
    parser.add_argument('-g', '--gtfs_dir',
                        type=str,
                        metavar='PATH',
                        help='path to GTFS dir (default: %s)' % os.getcwd())
    parser.add_argument('-s', '--service_date',
                        type=int,
                        metavar='SERVICEDATE',
                        help='date for service in yyyymmdd integer format (default: %s)' % os.getcwd())
    parser.add_argument('-o', '--output_dir',
                        type=str,
                        metavar='PATH',
                        help='path to ourput directory (default: %s)' % os.getcwd())

def get_service_ids(calendar, calendar_dates, day_of_week, service_date):
    regular_service_dates = calendar[(calendar['start_date']<= service_date) & (calendar['end_date'] >= service_date) & (calendar[day_of_week] == 1)]['service_id'].tolist()
    exceptions_df = calendar_dates[calendar_dates['date'] == service_date]
    add_service = exceptions_df.loc[exceptions_df['exception_type'] == 1]['service_id'].tolist() 
    remove_service = exceptions_df[exceptions_df['exception_type'] == 2]['service_id'].tolist()
    service_id_list = [x for x in (add_service + regular_service_dates) if x not in remove_service]
    return service_id_list

def create_id(df, feed, id_name, psrc_id_name):
    df[psrc_id_name] = feed + '_' + df[id_name].astype(str)
    return df      

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day


def get_start_end_date(my_date):
    start_date = to_integer(my_date - timedelta(days=1))
    end_date = to_integer(my_date + timedelta(days=1))
    return start_date, end_date

def save_sum(df_name, sum_name, feed_dict, dir, output_loc):
    sum_df = pd.DataFrame()
    for feed in feed_dict: 
        sum_df = pd.concat([sum_df, feed_dict[feed][df_name]])
    sum_df.to_csv(os.path.join(output_loc, sum_name), index=None)

def get_weekday(my_date):
    week_days = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    return week_days[my_date.weekday()]

def convert_to_seconds(value):
        h, m, s = value.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

def to_hhmmss(value):
    return time.strftime('%H:%M:%S', time.gmtime(value))

def frequencies_to_trips(frequencies, trips, stop_times):
    frequencies['start_time_secs'] = frequencies['start_time'].apply(convert_to_seconds)
    frequencies['end_time_secs'] = frequencies['end_time'].apply(convert_to_seconds)
    # following assumes that the total number of trips includes a final one that leaves first
    # stop at end_time in frequencies
    frequencies['total_trips'] = ((frequencies['end_time_secs']-frequencies['start_time_secs']) / frequencies['headway_secs']) + 1
    trips_update = trips.merge(frequencies, on = 'trip_id')
    trips_update = trips_update.loc[trips_update.index.repeat(trips_update['total_trips'])].reset_index(drop=True)
    trips_update['counter'] = trips_update.groupby('trip_id').cumcount() + 1
    trips_update['trip_id'] = trips_update['trip_id'] + '_' + trips_update['counter'].astype(str)
    
    stop_times_update = stop_times.merge(frequencies, on = 'trip_id')
    stop_times_update['arrival_time_secs'] = stop_times_update['arrival_time'].apply(convert_to_seconds)
    stop_times_update['departure_time_secs'] = stop_times_update['departure_time'].apply(convert_to_seconds)


    stop_times_update = stop_times_update.loc[stop_times_update.index.repeat(stop_times_update['total_trips'])].reset_index(drop=True)
    stop_times_update['counter'] = stop_times_update.groupby(['trip_id', 'stop_id']).cumcount()
    stop_times_update['departure_time_secs'] = stop_times_update['departure_time_secs'] + (stop_times_update['counter'] * stop_times_update['headway_secs'])
    stop_times_update['arrival_time_secs'] = stop_times_update['arrival_time_secs'] + (stop_times_update['counter'] * stop_times_update['headway_secs'])
    stop_times_update['departure_time'] = stop_times_update['departure_time_secs'].apply(to_hhmmss) 
    stop_times_update['arrival_time'] = stop_times_update['arrival_time_secs'].apply(to_hhmmss) 
    stop_times_update['counter'] = stop_times_update['counter'] + 1
    stop_times_update['trip_id'] = stop_times_update['trip_id'] + '_' + stop_times_update['counter'].astype(str)

    # remove trip_ids that are in frequencies
    stop_times = stop_times[~stop_times['trip_id'].isin(frequencies['trip_id'])]
    trips = trips[~trips['trip_id'].isin(frequencies['trip_id'])]
    
    # get rid of some columns
    stop_times_update = stop_times_update[stop_times.columns]
    trips_update = trips_update[trips.columns]

    # add new trips/stop times
    trips = pd.concat([trips, trips_update])
    stop_times = pd.concat([stop_times, stop_times_update])

    return trips, stop_times

def run(args):
    output_loc = args.output_dir

    logger = log_controller.setup_custom_logger('main_logger', output_loc)
    logger.info('------------------------Network Builder Started----------------------------------------------')

    dir = args.gtfs_dir
    service_date = args.service_date
    str_service_date = str(service_date)
    my_date = datetime(int(str_service_date[0:4]), int(str_service_date[4:6]), int(str_service_date[6:8]))

    logger.info('GTFS Directory path is: {}'.format(dir))
    logger.info('Output Directory path is: {}'.format(output_loc))
    logger.info('Service Date is: {}'.format(str_service_date))

    
    start_date, end_date = get_start_end_date(my_date)
    day_of_week = get_weekday(my_date)
    feed_list = next(os.walk(dir))[1]
    feed_dict = {}
    feed_path_list = list()
    
    for feed in feed_list: 
        print (feed)
        feed_dict[feed] = {}
        # read data

        calendar = pd.read_csv(os.path.join(dir, feed, 'calendar.txt'))
        if os.path.exists(os.path.join(dir, feed, 'calendar_dates.txt')) is False:
            calendar_dates = pd.DataFrame(columns=['service_id', 'date', 'exception_type']) 
        else:
            calendar_dates = pd.read_csv(os.path.join(dir, feed,'calendar_dates.txt'))

        service_id_list = get_service_ids(calendar, calendar_dates, day_of_week, service_date)

        if len(service_id_list) == 0:
            print ('There are no service ids for service date {} for feed {}'.format(str_service_date, feed))
            sys.exit()

        for id in service_id_list:
            logger.info('Adding service_id {} for feed {}'.format(id, feed))

        trips = pd.read_csv(os.path.join(dir, feed,'trips.txt'))
        stops = pd.read_csv(os.path.join(dir, feed,'stops.txt'))
        stop_times = pd.read_csv(os.path.join(dir, feed, 'stop_times.txt'))

        if os.path.exists(os.path.join(dir, feed, 'frequencies.txt')):
            frequencies = pd.read_csv(os.path.join(dir, feed, 'frequencies.txt'))
            if len(frequencies) > 0:
                trips, stop_times = frequencies_to_trips(frequencies, trips, stop_times)
            
        routes = pd.read_csv(os.path.join(dir, feed,'routes.txt'))
        shapes = pd.read_csv(os.path.join(dir, feed,'shapes.txt'))
        agency = pd.read_csv(os.path.join(dir, feed, 'agency.txt'))

        # create new IDs 
        trips = create_id(trips, feed, 'trip_id', 'trip_id')
        trips = create_id(trips, feed, 'route_id', 'route_id')
        trips = create_id(trips, feed, 'shape_id', 'shape_id')
        stop_times = create_id(stop_times, feed, 'trip_id', 'trip_id')
        stop_times = create_id(stop_times, feed, 'stop_id', 'stop_id')
        stops = create_id(stops, feed, 'stop_id', 'stop_id')
        routes = create_id(routes, feed, 'route_id', 'route_id')
        shapes = create_id(shapes, feed, 'shape_id', 'shape_id')
        
        # trips
        trips_df = trips.loc[trips['service_id'].isin(service_id_list)]
        trips_df['service_id'] = 1
        trip_id_list = np.unique(trips_df['trip_id'].tolist())
        route_id_list = np.unique(trips_df['route_id'].tolist())
        shape_id_list = np.unique(trips_df['shape_id'].tolist())
        # stop times
        stop_times_df = stop_times.loc[stop_times['trip_id'].isin(trip_id_list)]
        stop_id_list = np.unique(stop_times_df['stop_id'].tolist())
        # stops
        stops_df = stops.loc[stops['stop_id'].isin(stop_id_list)]
        # routes
        routes_df = routes.loc[routes['route_id'].isin(route_id_list)]
        routes_df['route_short_name'].fillna(routes_df['route_id'], inplace=True)
        # shapes
        shapes_df = shapes.loc[shapes['shape_id'].isin(shape_id_list)]
        
        # pass data to the dictionary
        feed_dict[feed]['agency_df'] = agency
        
        #feed_dict[feed]['calendar_dates_df'] = calendar_dates_df
        feed_dict[feed]['trips_df'] = trips_df
        feed_dict[feed]['stop_times_df'] = stop_times_df
        feed_dict[feed]['stops_df'] = stops_df
        feed_dict[feed]['routes_df'] = routes_df
        feed_dict[feed]['shapes_df'] = shapes_df
        #feed_dict[feed]['fare_rules_df'] = fare_rules_df
        #feed_dict[feed]['fare_attributes_df'] = fare_attributes_df
        #feed_dict[feed]['service_id_df'] = pd.DataFrame({'service_id': service_id_list})

    # calendar
    calendar_df = pd.DataFrame(columns = ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'])
    calendar_df.loc[0] = 0
    calendar_df['service_id'] = 1
    calendar_df[day_of_week] = 1
    calendar_df['start_date'] = start_date
    calendar_df['end_date'] = end_date
    calendar_df.to_csv(os.path.join(output_loc, 'calendar.txt'), index=None)

    save_sum('agency_df', 'agency.txt', feed_dict, dir, output_loc)
    save_sum('trips_df', 'trips.txt', feed_dict, dir, output_loc)
    save_sum('stops_df', 'stops.txt', feed_dict, dir, output_loc)
    save_sum('stop_times_df', 'stop_times.txt', feed_dict, dir, output_loc)
    save_sum('routes_df', 'routes.txt', feed_dict, dir, output_loc)
    save_sum('shapes_df', 'shapes.txt', feed_dict, dir, output_loc)
    #save_sum('service_id_df', 'service_id.txt', feed_dict, dir, output_loc)

    logger.info('Finished running combine_gtfs_feeds')
    sys.exit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))


