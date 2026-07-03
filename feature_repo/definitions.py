"""Feature definitions, shared by the local and AWS profiles.

Only `batch_source` differs between the two: Parquet files read by DuckDB locally,
Glue tables read by Athena on AWS. Entities, feature views and services are identical.
"""

import os
from datetime import timedelta

import pandas as pd
from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    ValueType,
)
from feast.data_format import ParquetFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int64, String

from citibike_fs.config import get_settings

settings = get_settings()


def batch_source(table: str, path: str):
    if settings.is_aws:
        from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
            AthenaSource,
        )

        return AthenaSource(
            name=f"{table}_source",
            table=table,
            database=os.environ["ATHENA_DATABASE"],
            data_source="AwsDataCatalog",
            timestamp_field="event_timestamp",
        )
    return FileSource(
        name=f"{table}_source",
        path=f"{path}/*.parquet",
        file_format=ParquetFormat(),
        timestamp_field="event_timestamp",
    )


# --- entities ---------------------------------------------------------------

station = Entity(name="station", join_keys=["station_id"], value_type=ValueType.STRING)
hour_of_week = Entity(
    name="hour_of_week",
    join_keys=["hour_of_week"],
    value_type=ValueType.INT64,
    description="Hour of the week in local time, Monday 00:00 = 0",
)
region = Entity(name="region", join_keys=["region_id"], value_type=ValueType.STRING)

# --- stream feature views ---------------------------------------------------
# Computed by the Spark streaming job and pushed online; the batch source holds the
# same features computed by the same code over history.

station_recent_activity = FeatureView(
    name="station_recent_activity",
    description="Departures and arrivals over the trailing 15 minutes and hour",
    entities=[station],
    # Every station gets a row every 15 minutes, zero or not. An older row means the
    # pipeline has stalled; the value is then treated as missing.
    ttl=timedelta(minutes=30),
    schema=[
        Field(name="station_id", dtype=String),
        Field(name="departures_15m", dtype=Int64),
        Field(name="departures_1h", dtype=Int64),
        Field(name="arrivals_15m", dtype=Int64),
        Field(name="arrivals_1h", dtype=Int64),
    ],
    source=PushSource(
        name="station_recent_activity_push",
        batch_source=batch_source("station_recent_activity", settings.recent_activity),
    ),
)

station_live_status = FeatureView(
    name="station_live_status",
    description="Live bike and dock availability from the GBFS feed",
    entities=[station],
    ttl=timedelta(minutes=10),
    schema=[
        Field(name="station_id", dtype=String),
        Field(name="num_bikes_available", dtype=Int64),
        Field(name="num_ebikes_available", dtype=Int64),
        Field(name="num_docks_available", dtype=Int64),
        Field(name="is_renting", dtype=Int64),
    ],
    source=PushSource(
        name="station_live_status_push",
        batch_source=batch_source("station_live_status", settings.live_status),
    ),
)

weather_hourly = FeatureView(
    name="weather_hourly",
    description="Hourly weather for the region",
    entities=[region],
    ttl=timedelta(hours=3),
    schema=[
        Field(name="region_id", dtype=String),
        Field(name="temperature_c", dtype=Float64),
        Field(name="precipitation_mm", dtype=Float64),
        Field(name="wind_speed_kmh", dtype=Float64),
    ],
    source=PushSource(
        name="weather_hourly_push",
        batch_source=batch_source("weather_hourly", settings.weather),
    ),
)

# --- batch feature view -----------------------------------------------------

station_hourly_profile = FeatureView(
    name="station_hourly_profile",
    description="Typical demand per station and hour of week (mean of the last 4 weeks)",
    entities=[station, hour_of_week],
    # Trip history is published monthly, so the newest profile can be weeks old.
    ttl=timedelta(days=90),
    schema=[
        Field(name="station_id", dtype=String),
        Field(name="hour_of_week", dtype=Int64),
        Field(name="avg_departures_4w", dtype=Float64),
        Field(name="avg_arrivals_4w", dtype=Float64),
    ],
    source=batch_source("station_hourly_profile", settings.hourly_profile),
)

# --- on-demand feature view -------------------------------------------------
# Runs inside Feast at retrieval time, so training and serving get the same code.


@on_demand_feature_view(
    sources=[station_hourly_profile],
    schema=[Field(name="typical_net_flow_4w", dtype=Float64)],
    mode="pandas",
)
def profile_signals(inputs: pd.DataFrame) -> pd.DataFrame:
    """Whether the station normally fills up (positive) or drains (negative) at this hour."""
    arrivals = pd.to_numeric(inputs["avg_arrivals_4w"], errors="coerce").astype("float64")
    departures = pd.to_numeric(inputs["avg_departures_4w"], errors="coerce").astype("float64")
    return pd.DataFrame({"typical_net_flow_4w": arrivals - departures})


# --- feature services -------------------------------------------------------

demand_forecast_v1 = FeatureService(
    name="demand_forecast_v1",
    description="Inputs of the next-hour departure model",
    features=[station_recent_activity, station_hourly_profile, weather_hourly, profile_signals],
)

station_live_v1 = FeatureService(
    name="station_live_v1",
    description="Live availability shown next to the forecast",
    features=[station_live_status],
)
