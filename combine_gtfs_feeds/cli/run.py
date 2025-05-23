from __future__ import annotations

import combine_gtfs_feeds.cli.log_controller as log_controller  # type: ignore

try:
    from .gtfs_schema import GTFS_Schema
except Exception:
    from gtfs_schema import GTFS_Schema

import argparse
import os as os
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


class Combined_GTFS:
    file_list = ["agency", "trips", "stop_times", "stops", "routes", "shapes"]

    def __init__(self, df_dict: dict, output_dir: str):
        """
        Initializes the Combined_GTFS class with the provided dataframes and output directory.
        """

        # self.agency_df = df_dict["agency"]
        self.output_dir = output_dir
        self.agency_df = GTFS_Schema.Agency.validate(df_dict["agency"])
        self.agency_df = self.agency_df[
            [col for col in GTFS_Schema.agency_columns if col in self.agency_df.columns]
        ]

        # self.routes_df = df_dict["routes"]
        self.routes_df = GTFS_Schema.Routes.validate(df_dict["routes"])
        self.routes_df = self.routes_df[
            [col for col in GTFS_Schema.routes_columns if col in self.routes_df.columns]
        ]

        # self.stops_df = df_dict["stops"]
        self.stops_df = GTFS_Schema.Stops.validate(df_dict["stops"])
        self.stops_df = self.stops_df[
            [col for col in GTFS_Schema.stops_columns if col in self.stops_df.columns]
        ]

        # self.stop_times_df = df_dict["stop_times"]
        self.stop_times_df = GTFS_Schema.Stop_Times.validate(df_dict["stop_times"])
        self.stop_times_df = self.stop_times_df[
            [
                col
                for col in GTFS_Schema.stop_times_columns
                if col in self.stop_times_df.columns
            ]
        ]

        # self.shapes_df = df_dict["shapes"]
        self.shapes_df = GTFS_Schema.Shapes.validate(df_dict["shapes"])
        self.shapes_df = self.shapes_df[
            [col for col in GTFS_Schema.shapes_columns if col in self.shapes_df.columns]
        ]

        # self.trips_df = df_dict["trips"]
        self.trips_df = GTFS_Schema.Trips.validate(df_dict["trips"])
        self.trips_df = self.trips_df[
            [col for col in GTFS_Schema.trips_columns if col in self.trips_df.columns]
        ]

        # self.calendar_df = df_dict["calendar"]
        self.calendar_df = GTFS_Schema.Calendar.validate(df_dict["calendar"])
        self.calendar_df = self.calendar_df[
            [
                col
                for col in GTFS_Schema.calendar_columns
                if col in self.calendar_df.columns
            ]
        ]

    def export_feed(self):
        """
        Exports the combined GTFS feed to the output directory.
        """
        dir = Path(self.output_dir)
        self.agency_df.to_csv(dir / "agency.txt", index=None)
        self.routes_df.to_csv(dir / "routes.txt", index=None)
        self.stops_df.to_csv(dir / "stops.txt", index=None)
        self.stop_times_df.to_csv(dir / "stop_times.txt", index=None)
        self.shapes_df.to_csv(dir / "shapes.txt", index=None)
        self.trips_df.to_csv(dir / "trips.txt", index=None)
        self.calendar_df.to_csv(dir / "calendar.txt", index=None)


def add_run_args(parser, multiprocess=True):
    """
    Run command args
    """
    parser.add_argument(
        "-g",
        "--gtfs_dir",
        type=str,
        metavar="PATH",
        help="path to GTFS dir (default: %s)" % os.getcwd(),
    )

    parser.add_argument(
        "-s",
        "--service_date",
        type=int,
        metavar="SERVICEDATE",
        help=(
            "date for service in yyyymmdd integer format                      "
            " (default: %s)"
        )
        % os.getcwd(),
    )

    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        metavar="PATH",
        help="path to ourput directory (default: %s)" % os.getcwd(),
    )


def get_service_ids(
    calendar: pd.DataFrame,
    calendar_dates: pd.DataFrame,
    day_of_week: str,
    service_date: int,
) -> list:
    """
    Returns a list of valid service_id(s) from each feed
    using the user specified service_date.
    """

    if not calendar.empty:
        regular_service_dates = calendar[
            (calendar["start_date"] <= service_date)
            & (calendar["end_date"] >= service_date)
            & (calendar[day_of_week] == 1)
        ]["service_id"].tolist()
    else:
        regular_service_dates = []

    if not calendar_dates.empty:
        exceptions_df = calendar_dates[calendar_dates["date"] == service_date]
        add_service = exceptions_df.loc[exceptions_df["exception_type"] == 1][
            "service_id"
        ].tolist()
        remove_service = exceptions_df[exceptions_df["exception_type"] == 2][
            "service_id"
        ].tolist()
    else:
        add_service = []
        remove_service = []

    service_id_list = [
        x for x in (add_service + regular_service_dates) if x not in remove_service
    ]

    return service_id_list


def create_id(df: pd.DataFrame, feed: str, id_column: str) -> pd.DataFrame:
    """
    Changes id_column by prepending each value with
    the feed parameter.
    """
    # df[id_column] = feed + "_" + df[id_column].astype(str)
    df[id_column] = np.where(
        ~df[id_column].isnull(), feed + "_" + df[id_column].astype(str), ""
    )
    return df


def dt_to_yyyymmdd(dt_time: datetime) -> int:
    """
    Converts a date time object to
    YYYYMMDD format.
    """
    return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day


def get_start_end_date(my_date: datetime) -> tuple[int, int]:
    """
    Gets the day before and after
    the user parameter service_date
    in YYYYMMDD format.
    """
    start_date = dt_to_yyyymmdd(my_date - timedelta(days=1))
    end_date = dt_to_yyyymmdd(my_date + timedelta(days=1))
    return start_date, end_date


def get_weekday(my_date: datetime) -> str:
    """
    Gets the day of week from user parameter
    service date.
    """
    week_days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    return week_days[my_date.weekday()]


def convert_to_seconds(value: str) -> int:
    """
    Converts hh:mm:ss format to number
    of seconds after midnight.
    """
    h, m, s = value.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def to_hhmmss(value: float) -> str:
    """
    Converts to hhmmss format.
    """
    return time.strftime("%H:%M:%S", time.gmtime(value))


def interpolate_arrival_departure_time(stop_times: pd.DataFrame) -> pd.DataFrame:
    """
    Interpolates missing arrival and departure times in stop_times.txt.
    The times are sorted by trip_id and stop_sequence. 0:00:00 is used as a
    placeholder for missing times. The times are then converted to seconds,
    and the missing values are interpolated. Finally, the times are converted
    back to hh:mm:ss format.
    """

    stop_times.sort_values(["trip_id", "stop_sequence"], inplace=True)
    for col_name in ["arrival_time", "departure_time"]:
        stop_times[col_name].fillna("00:00:00", inplace=True)
        stop_times["temp"] = stop_times[col_name].apply(convert_to_seconds)
        stop_times["temp"].replace(0, np.NaN, inplace=True)
        stop_times["temp"].interpolate(inplace=True)
        stop_times[col_name] = stop_times["temp"].apply(to_hhmmss)
        stop_times.drop(columns=["temp"], inplace=True)
    return stop_times


def frequencies_to_trips(
    frequencies: pd.DataFrame, trips: pd.DataFrame, stop_times: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each trip_id in frequencies.txt, calculates the number
    of trips and creates records for each trip in trips.txt and
    stop_times.txt. Deletes the original represetative trip_id
    in both of these files.
    """

    # some feeds will use the same trip_id for multiple rows
    # need to create a unique id for each row
    frequencies["frequency_id"] = frequencies.index

    frequencies["start_time_secs"] = frequencies["start_time"].apply(convert_to_seconds)

    frequencies["end_time_secs"] = frequencies["end_time"].apply(convert_to_seconds)

    # following is coded so the total number of trips
    # does not include a final one that leaves the first
    # stop at end_time in frequencies. I think this is the
    # correct interpretation of the field description:
    # 'Time at which service changes to a different headway
    # (or ceases) at the first stop in the trip.'

    # Rounding total trips to make sure all trips are counted
    # when end time is in the following format: 14:59:59,
    # instead of 15:00:00.

    frequencies["total_trips"] = (
        (
            (frequencies["end_time_secs"] - frequencies["start_time_secs"])
            / frequencies["headway_secs"]
        )
        .round(0)
        .astype(int)
    )

    trips_update = trips.merge(frequencies, on="trip_id")
    trips_update = trips_update.loc[
        trips_update.index.repeat(trips_update["total_trips"])
    ].reset_index(drop=True)
    trips_update["counter"] = trips_update.groupby("trip_id").cumcount() + 1
    trips_update["trip_id"] = (
        trips_update["trip_id"].astype(str) + "_" + trips_update["counter"].astype(str)
    )

    stop_times_update = frequencies.merge(stop_times, on="trip_id", how="left")
    stop_times_update["arrival_time_secs"] = stop_times_update["arrival_time"].apply(
        convert_to_seconds
    )
    stop_times_update["departure_time_secs"] = stop_times_update[
        "departure_time"
    ].apply(convert_to_seconds)

    stop_times_update["elapsed_time"] = stop_times_update.groupby(
        ["trip_id", "start_time"]
    )["arrival_time_secs"].transform("first")
    stop_times_update["elapsed_time"] = (
        stop_times_update["arrival_time_secs"] - stop_times_update["elapsed_time"]
    )
    stop_times_update["arrival_time_secs"] = (
        stop_times_update["start_time_secs"] + stop_times_update["elapsed_time"]
    )

    # for now assume departure time is the same as arrival time.
    stop_times_update["departure_time_secs"] = (
        stop_times_update["start_time_secs"] + stop_times_update["elapsed_time"]
    )

    stop_times_update = stop_times_update.loc[
        stop_times_update.index.repeat(stop_times_update["total_trips"])
    ].reset_index(drop=True)
    # handles cae of repeated trip_ids
    stop_times_update["counter"] = stop_times_update.groupby(
        ["frequency_id", "stop_id"]
    ).cumcount()
    stop_times_update["departure_time_secs"] = stop_times_update[
        "departure_time_secs"
    ] + (stop_times_update["counter"] * stop_times_update["headway_secs"])
    stop_times_update["arrival_time_secs"] = stop_times_update["arrival_time_secs"] + (
        stop_times_update["counter"] * stop_times_update["headway_secs"]
    )

    # now we want to get the cumcount based on trip_id
    stop_times_update["counter"] = (
        stop_times_update.groupby(["trip_id", "stop_id"]).cumcount() + 1
    )
    stop_times_update["departure_time"] = stop_times_update[
        "departure_time_secs"
    ].apply(to_hhmmss)

    stop_times_update["arrival_time"] = stop_times_update["arrival_time_secs"].apply(
        to_hhmmss
    )

    stop_times_update["trip_id"] = (
        stop_times_update["trip_id"].astype(str)
        + "_"
        + stop_times_update["counter"].astype(str)
    )

    # remove trip_ids that are in frequencies
    stop_times = stop_times[~stop_times["trip_id"].isin(frequencies["trip_id"])]

    trips = trips[~trips["trip_id"].isin(frequencies["trip_id"])]

    # get rid of some columns
    stop_times_update = stop_times_update[stop_times.columns]
    trips_update = trips_update[trips.columns]

    # add new trips/stop times
    trips = pd.concat([trips, trips_update])
    stop_times = pd.concat([stop_times, stop_times_update])

    return trips, stop_times


def get_schedule_pattern(
    merged_stops_times: pd.DataFrame, route_field="route_id"
) -> dict:
    """
    Returns a nested diciontary where the first level key is route_id and
    values are respresentative trip_ids that have unique stop sequences.
    These are are used as keys for the second level where each value is a
    dictinary that includes a list of trips_id's that share this stop
    pattern and a list of ordered stops.

    {route_id : trip_id {trips_ids : [list of trip ids], stops :
    [list of stops]}}

    """
    stop_sequence_dict = {
        k: list(v)
        for k, v in merged_stops_times.groupby(["trip_id", route_field])["stop_id"]
    }

    # Empty dictionary to store unique stop sequences
    my_dict = {}
    for key, value in stop_sequence_dict.items():
        trip_id = key[0]
        route_id = key[1]
        # Handle some branching later on
        found = False
        # If this is the first trip for this route, just add it
        if route_id not in my_dict.keys():
            my_dict[route_id] = {trip_id: {"stops": value, "trip_ids": [trip_id]}}
        # Otherwise check to see if this stop sequence has already been
        # added for this route
        else:
            for k, v in my_dict[route_id].items():
                if value == v["stops"]:
                    # This stop sequence has already been added for this
                    # route, add the trip_id to the list of trip_ids that
                    # have this sequence in common.
                    my_dict[route_id][k]["trip_ids"].append(trip_id)
                    found = True
                    break
            if not found:
                # Add the stop sequence and route, trip info
                my_dict[route_id][trip_id] = {"stops": value, "trip_ids": [trip_id]}
        # Set back to False for next iteration
        found = False
    return my_dict


def get_schedule_pattern_df(schedule_pattern_dict: dict) -> pd.DataFrame:
    """
    Converts the schedule pattern dictionary to a DataFrame.
    The DataFrame has two columns: trip_id1 and trip_id2.
    trip_id1 is the representative trip_id for a route and trip_id2
    is the trip_id that shares the same stop pattern.
    """

    rows = []
    for route_id, trips in schedule_pattern_dict.items():
        for trip_id, data in trips.items():
            for trip in data["trip_ids"]:
                rows.append(
                    {"route_id": route_id, "trip_id1": trip_id, "trip_id2": trip}
                )
    df2 = pd.DataFrame(rows)

    return df2[["trip_id1", "trip_id2"]]


def shapes_from_stops_sequence(
    stops: pd.DataFrame, stop_times: pd.DataFrame, trips: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Used when shapes.txt is missing. Creates a new shapes.txt file
    using the stop_sequence from stop_times.txt.
    """

    trip_cols = list(trips.columns)
    if "shape_id" not in trip_cols:
        trip_cols.append("shape_id")
    merged = stop_times.merge(stops, how="left", on="stop_id")
    merged = merged.merge(trips, how="left", on="trip_id")
    schedule_pattern = get_schedule_pattern(merged)
    schedule_pattern = get_schedule_pattern_df(schedule_pattern)
    merged = merged.merge(schedule_pattern, left_on="trip_id", right_on="trip_id2")
    merged["shape_id"] = merged["trip_id1"]

    shapes = merged[merged["trip_id"].isin(merged["trip_id1"])]
    shapes_cols = {
        "stop_lat": "shape_pt_lat",
        "stop_lon": "shape_pt_lon",
        "stop_sequence": "shape_pt_sequence",
    }
    shapes = shapes.rename(columns=shapes_cols)
    keep_cols = list(shapes_cols.values())
    keep_cols.append("shape_id")
    shapes = shapes[keep_cols]

    merged = merged.groupby("trip_id").first().reset_index()
    new_trips = merged[trip_cols]

    assert len(new_trips) == len(stop_times.trip_id.unique())
    assert len(new_trips.shape_id.unique()) == len(shapes.shape_id.unique())

    return shapes, new_trips


def read_gtfs(
    path: Path,
    gtfs_file_name: str,
    is_zipped: bool,
    feed_name: str,
    logger: log_controller.logging.Logger,
    empty_df_cols=[],
) -> pd.DataFrame:
    """
    Reads in a GTFS file and returns a DataFrame.
    """

    if is_zipped:
        zf = zipfile.ZipFile(path.with_suffix(".zip"))
        try:
            # df = pd.read_csv(zf.open(gtfs_file_name), dtype_backend="pyarrow")
            df = pd.read_csv(zf.open(gtfs_file_name))
            if df.empty:
                if feed_name in GTFS_Schema.required_files:
                    logger(
                        f"Fatal! {gtfs_file_name} from feed {feed_name} is empty."
                        " Exiting program"
                    )
                    sys.exit()

                else:
                    logger(f"Warning! {gtfs_file_name} from feed {feed_name} is empty.")
        except Exception:
            if gtfs_file_name in GTFS_Schema.required_files:
                logger(
                    f"Fatal! {gtfs_file_name} from feed {feed_name} is missing. Exiting"
                    " program"
                )
                sys.exit()
            else:
                df = pd.DataFrame(columns=empty_df_cols)

    # unzipped
    else:
        try:
            # df = pd.read_csv(path / gtfs_file_name, dtype_backend="pyarrow")
            df = pd.read_csv(path / gtfs_file_name)
            if df.empty:
                if feed_name in GTFS_Schema.required_files:
                    logger.info(
                        f"Fatal! {gtfs_file_name} from feed {feed_name} is empty."
                        " Exiting program"
                    )
                    sys.exit()

                else:
                    logger(f"Warning! {gtfs_file_name} from feed {feed_name} is empty.")
        except Exception:
            if gtfs_file_name in GTFS_Schema.required_files:
                logger.info(
                    f"Fatal! {gtfs_file_name} from feed {feed_name} is missing. Exiting"
                    " program"
                )
                sys.exit()
            else:
                df = pd.DataFrame(columns=empty_df_cols)

    df.columns = df.columns.str.replace(" ", "")
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def run(args: argparse.Namespace) -> None:
    """
    Implements the 'run' sub-command, which combines
    gtfs files from each feed and writes them out to
    a single feed.
    """

    logger = log_controller.setup_custom_logger("main_logger", args.output_dir)
    logger.info("------------------combine_gtfs_feeds Started----------------")

    feeds = combine(args.gtfs_dir, args.service_date, args.output_dir, logger)

    feeds.export_feed()

    logger.info("Finished running combine_gtfs_feeds")
    sys.exit()
    # print("Finished running combine_gtfs_feeds")


def combine(gtfs_dir: str, service_date, output_dir, logger=None) -> Combined_GTFS:
    """
    Combines GTFS feeds from each feed and writes them out to a single feed.
    """
    if not logger:
        logger = log_controller.setup_custom_logger("main_logger", output_dir)
        logger.info("------------------combine_gtfs_feeds Started----------------")
    output_loc = output_dir

    if not os.path.isdir(output_loc):
        print("Output Directory path : {} does not exist.".format(output_loc))
        print("Exiting application early!")
        sys.exit()

    dir = Path(gtfs_dir)
    str_service_date = str(service_date)
    my_date = datetime(
        int(str_service_date[0:4]),
        int(str_service_date[4:6]),
        int(str_service_date[6:8]),
    )

    logger.info("GTFS Directory path is: {}".format(dir))
    logger.info("Output Directory path is: {}".format(output_loc))
    logger.info("Service Date is: {}".format(str_service_date))

    if not os.path.isdir(dir):
        logger.info("GTFS Directory path : {} does not exist.".format(dir))
        logger.info("Exiting application early!")
        sys.exit()

    start_date, end_date = get_start_end_date(my_date)
    day_of_week = get_weekday(my_date)
    feed_list = next(os.walk(dir))[1]
    if len(feed_list) == 0:
        feed_list = next(os.walk(dir))[2]
        feed_list = [i for i in feed_list if ".zip" in i]
        zipped = True
    else:
        zipped = False
    feed_dict = {}

    if len(feed_list) == 0:
        logger.info("There are no GTFS feeds in GTFS Directory path : {}.".format(dir))
        logger.info("Exiting application early!")
        sys.exit()

    for feed in feed_list:
        if zipped:
            feed = feed[:-4]
        feed_dict[feed] = {}
        # read data
        full_path = dir / feed
        calendar = read_gtfs(full_path, "calendar.txt", zipped, feed, logger)
        calendar_dates = read_gtfs(
            full_path,
            "calendar_dates.txt",
            zipped,
            feed,
            logger,
            ["service_id", "date", "exception_type"],
        )

        service_id_list = get_service_ids(
            calendar, calendar_dates, day_of_week, service_date
        )

        if len(service_id_list) == 0:
            logger.info(
                "There are no service ids for service                 date {}...".format(
                    str_service_date
                )
            )
            logger.info("for feed {}".format(feed))
            logger.info("Exiting application early!")
            sys.exit()

        for id in service_id_list:
            logger.info("Adding service_id {} for feed {}".format(id, feed))

        trips = read_gtfs(full_path, "trips.txt", zipped, feed, logger)
        stops = read_gtfs(full_path, "stops.txt", zipped, feed, logger)
        stop_times = read_gtfs(full_path, "stop_times.txt", zipped, feed, logger)
        frequencies = read_gtfs(full_path, "frequencies.txt", zipped, feed, logger)

        if len(frequencies) > 0:
            logger.info(f"Feed {feed} contains frequencies.txt...".format(feed))
            logger.info(
                "Unique trips will be added to outputs based on headways in"
                " frequencies.txt"
            )
            trips, stop_times = frequencies_to_trips(frequencies, trips, stop_times)

        routes = read_gtfs(full_path, "routes.txt", zipped, feed, logger)
        shapes = read_gtfs(full_path, "shapes.txt", zipped, feed, logger)
        agency = read_gtfs(full_path, "agency.txt", zipped, feed, logger)
        if "agency_id" not in routes.columns:
            routes["agency_id"] = agency["agency_id"][0]

        # check to make sure there are shapes
        if len(shapes) == 0:
            logger.info(
                f"Warning: feed {feed} is mising shapes.txt. Records for this file will"
                " be created using route-level unique stop sequence and location. See"
                " documentation for more information."
            )
            shapes, trips = shapes_from_stops_sequence(stops, stop_times, trips)
            # trips = create_id(trips, feed, "shape_id")

        # create new IDs
        trips = create_id(trips, feed, "trip_id")
        trips = create_id(trips, feed, "route_id")
        trips = create_id(trips, feed, "shape_id")

        shapes = create_id(shapes, feed, "shape_id")

        stop_times = create_id(stop_times, feed, "trip_id")
        stop_times = create_id(stop_times, feed, "stop_id")
        stops = create_id(stops, feed, "stop_id")
        routes = create_id(routes, feed, "route_id")

        # trips
        trips = trips.loc[trips["service_id"].isin(service_id_list)]
        if len(trips) == 0:
            logger.info(
                f"Warning! No trips found for feed {feed} using service_ids"
                f" {str(service_id_list)}"
            )
        trips["service_id"] = 1
        trip_id_list = np.unique(trips["trip_id"].tolist())
        route_id_list = np.unique(trips["route_id"].tolist())
        shape_id_list = np.unique(trips["shape_id"].tolist())

        # stop times
        stop_times = stop_times.loc[stop_times["trip_id"].isin(trip_id_list)]
        if stop_times["departure_time"].isnull().any():
            logger.info(
                "Feed {} contains missing departure/arrival times. Interpolating"
                " missing times.".format(feed)
            )
            stop_times = interpolate_arrival_departure_time(stop_times)

        stop_id_list = np.unique(stop_times["stop_id"].tolist())
        # stops
        stops = stops.loc[stops["stop_id"].isin(stop_id_list)]
        # routes
        routes = routes.loc[routes["route_id"].isin(route_id_list)]
        routes["route_short_name"].fillna(routes["route_id"], inplace=True)
        # shapes
        shapes = shapes.loc[shapes["shape_id"].isin(shape_id_list)]

        # pass data to the dictionary
        feed_dict[feed]["agency"] = agency
        feed_dict[feed]["trips"] = trips
        feed_dict[feed]["stop_times"] = stop_times
        feed_dict[feed]["stops"] = stops
        feed_dict[feed]["routes"] = routes
        feed_dict[feed]["shapes"] = shapes

    # calendar
    calendar = pd.DataFrame(
        columns=[
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ]
    )

    calendar.loc[0] = 0
    calendar["service_id"] = 1
    calendar[day_of_week] = 1
    calendar["start_date"] = start_date
    calendar["end_date"] = end_date

    combined_feed_dict = {}
    combined_feed_dict["calendar"] = calendar

    for file_name in Combined_GTFS.file_list:
        df = pd.DataFrame()
        for feed in feed_dict:
            df = pd.concat([df, feed_dict[feed][file_name]])
        combined_feed_dict[file_name] = df

    # logger.info("Finished running combine_gtfs_feeds")

    return Combined_GTFS(combined_feed_dict, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))
