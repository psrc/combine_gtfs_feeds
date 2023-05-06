from platform import platform
from numpy import float64
from pandera.typing import Series
from typing import Optional
import pandera as pa
import pandas as pd


class GTFS_Schema(object):
    class Agency(pa.SchemaModel):
        agency_id: Series[str] = pa.Field(coerce=True)
        agency_name: Series[str] = pa.Field(coerce=True)
        agency_url: Series[str] = pa.Field(coerce=True)
        agency_timezone: Series[str] = pa.Field(coerce=True)
        agency_lang: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        agency_phone: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        agency_fare_url: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        agency_email: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)

    class Stops(pa.SchemaModel):
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_code: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        stop_name: Series[str] = pa.Field(coerce=True)
        stop_desc: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        stop_lat: Series[float64] = pa.Field(coerce=True, nullable=True)
        stop_lon: Series[float64] = pa.Field(coerce=True, nullable=True)
        zone_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        stop_url: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        location_type: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2, 3, 4]
        )
        parent_station: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        stop_timezone: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        wheelchair_boarding: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2]
        )
        level_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        platform_code: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)

    class Routes(pa.SchemaModel):
        route_id: Series[str] = pa.Field(coerce=True)
        agency_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_short_name: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_long_name: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_desc: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_type: Series[int] = pa.Field(isin=[0, 1, 2, 3, 4, 5, 6, 7, 11, 12])
        route_url: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_color: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_text_color: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        route_sort_order: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True
        )
        continuous_pickup: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2, 3]
        )
        continuous_drop_off: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2, 3]
        )

    class Trips(pa.SchemaModel):
        route_id: Series[str] = pa.Field(coerce=True)
        service_id: Series[str] = pa.Field(coerce=True)
        trip_id: Series[str] = pa.Field(coerce=True)
        trip_headsign: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        trip_short_name: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        direction_id: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1]
        )
        block_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        shape_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        wheelchair_accessible: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2]
        )
        bikes_allowed: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2]
        )

    class Stop_Times(pa.SchemaModel):
        trip_id: Series[str] = pa.Field(coerce=True)
        arrival_time: Series[str] = pa.Field(coerce=True, nullable=True)
        departure_time: Series[str] = pa.Field(coerce=True, nullable=True)
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_sequence: Series[int] = pa.Field(coerce=True)
        stop_headsign: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
        pickup_type: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2, 3]
        )
        drop_off_type: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1, 2, 3]
        )
        # continuous_pickup: Optional[Series[pd.Int64Dtype]] = pa.Field(
        #     coerce=True, nullable=True, isin=[0, 1, 2, 3]
        # )
        # continuous_drop_off: Optional[Series[pd.Int64Dtype]] = pa.Field(
        #     coerce=True, nullable=True, isin=[0, 1, 2, 3]
        # )
        shape_dist_traveled: Optional[Series[float64]] = pa.Field(
            coerce=True, nullable=True, ge=0
        )
        timepoint: Optional[Series[pd.Int64Dtype]] = pa.Field(
            coerce=True, nullable=True, isin=[0, 1]
        )

    class Calendar(pa.SchemaModel):
        service_id: Series[str] = pa.Field(coerce=True)
        monday: Series[int] = pa.Field(isin=[0, 1])
        tuesday: Series[int] = pa.Field(isin=[0, 1])
        wednesday: Series[int] = pa.Field(isin=[0, 1])
        thursday: Series[int] = pa.Field(isin=[0, 1])
        friday: Series[int] = pa.Field(isin=[0, 1])
        saturday: Series[int] = pa.Field(isin=[0, 1])
        sunday: Series[int] = pa.Field(isin=[0, 1])
        start_date: Series[pd.Int64Dtype] = pa.Field(coerce=True)
        end_date: Series[pd.Int64Dtype] = pa.Field(coerce=True)

    class Calendar_Dates(pa.SchemaModel):
        service_id: Series[str] = pa.Field(coerce=True)
        date: Series[str] = pa.Field(coerce=True)
        exception_type: Series[int] = pa.Field(coerce=True, isin=[1, 2])

    class Shapes(pa.SchemaModel):
        shape_id: Series[str] = pa.Field(coerce=True)
        shape_pt_lat: Series[float64] = pa.Field(coerce=True)
        shape_pt_lon: Series[float64] = pa.Field(coerce=True)
        shape_pt_sequence: Series[int] = pa.Field(coerce=True)
        shape_dist_traveled: Optional[Series[float64]] = pa.Field(
            coerce=True, nullable=True
        )

    required_files = [
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
    ]
    agency_columns = list(Agency.__annotations__.keys())
    stops_columns = list(Stops.__annotations__.keys())
    routes_columns = list(Routes.__annotations__.keys())
    trips_columns = list(Trips.__annotations__.keys())
    stop_times_columns = list(Stop_Times.__annotations__.keys())
    calendar_columns = list(Calendar.__annotations__.keys())
    calendar_dates_columns = list(Calendar_Dates.__annotations__.keys())
    shapes_columns = list(Shapes.__annotations__.keys())
