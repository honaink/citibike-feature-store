"""Feature logic, written once and used by both the batch backfill and the streaming job.

Every function takes and returns a DataFrame and uses only operations that Structured
Streaming supports, so `station_activity` produces identical rows whether its input is
three months of Parquet or a live Kafka topic.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from citibike_fs.config import get_settings

ACTIVITY_WINDOW = "1 hour"
ACTIVITY_SLIDE = "15 minutes"
LABEL_HORIZON_BUCKETS = 4  # 4 x 15 minutes = the next hour

ACTIVITY_COLUMNS = ["departures_15m", "departures_1h", "arrivals_15m", "arrivals_1h"]
HEARTBEAT = "heartbeat"  # an event that marks a station as observed but counts as nothing


def hour_of_week(ts: Column) -> Column:
    """Monday 00:00 local time = 0 ... Sunday 23:00 = 167. Must match `features.hour_of_week`."""
    local = F.from_utc_timestamp(ts, get_settings().timezone)
    return (F.weekday(local) * 24 + F.hour(local)).cast("long")


def normalize_trips(raw: DataFrame) -> DataFrame:
    """Raw trip CSV rows (all strings, local wall-clock times) to typed rows in UTC."""
    tz = get_settings().timezone
    return (
        raw.select(
            "ride_id",
            F.to_utc_timestamp(F.to_timestamp("started_at"), tz).alias("started_at"),
            F.to_utc_timestamp(F.to_timestamp("ended_at"), tz).alias("ended_at"),
            "start_station_id",
            "end_station_id",
        )
        .where(F.col("started_at").isNotNull() & F.col("ended_at").isNotNull())
        .where(F.col("ended_at") >= F.col("started_at"))
        .dropDuplicates(["ride_id"])
    )


def trips_to_events(trips: DataFrame) -> DataFrame:
    """One departure and one arrival event per trip, the shape the live stream carries."""

    def events(kind: str, station: str, ts: str) -> DataFrame:
        return trips.select(
            F.concat("ride_id", F.lit(f":{kind[0]}")).alias("event_id"),
            F.col(station).alias("station_id"),
            F.lit(kind).alias("event_type"),
            F.col(ts).alias("event_ts"),
        )

    both = events("departure", "start_station_id", "started_at").unionByName(
        events("arrival", "end_station_id", "ended_at")
    )
    return both.where(F.col("station_id").isNotNull() & (F.col("station_id") != ""))


def in_scope(events: DataFrame) -> DataFrame:
    """Keep events at stations in the configured scope. Mirrors `Settings.in_scope`."""
    is_jc = F.col("station_id").rlike("^(JC|HB)")
    return events.where(is_jc if get_settings().scope == "jc" else ~is_jc)


def heartbeats(events: DataFrame) -> DataFrame:
    """One no-op event per station every 15 minutes across the span of its history.

    `station_activity` only emits a row for a window that contains an event, so a quiet
    hour would leave the previous (non-zero) row in place. Heartbeats guarantee a row, with
    zero counts, at every boundary. Live, the station status feed plays this role; there
    is no history of that feed, so the backfill synthesises the equivalent.
    """
    buckets = events.select(
        "station_id", F.window("event_ts", ACTIVITY_SLIDE).start.alias("event_ts")
    )
    grid = _dense_grid(buckets.distinct(), "event_ts", ACTIVITY_SLIDE)
    return grid.select(
        F.concat(F.lit("hb:"), "station_id", F.lit(":"), F.col("event_ts").cast("string")).alias(
            "event_id"
        ),
        "station_id",
        F.lit(HEARTBEAT).alias("event_type"),
        "event_ts",
    )


def station_activity(events: DataFrame) -> DataFrame:
    """Departures and arrivals per station over the trailing 15 minutes and 1 hour.

    One row per station per 15-minute boundary, stamped with the window end, counting
    events in [end - window, end). A window with no events at all produces no row, so
    callers include heartbeat events to get a zero row for quiet stations.
    """
    windowed = events.select(
        "station_id",
        "event_type",
        "event_ts",
        F.window("event_ts", ACTIVITY_WINDOW, ACTIVITY_SLIDE).alias("w"),
    )
    is_departure = F.col("event_type") == "departure"
    is_arrival = F.col("event_type") == "arrival"
    in_last_slide = F.col("event_ts") >= F.col("w.end") - F.expr(f"INTERVAL {ACTIVITY_SLIDE}")

    def count(condition: Column) -> Column:
        return F.sum(condition.cast("long"))

    return (
        windowed.groupBy("station_id", "w")
        .agg(
            count(is_departure & in_last_slide).alias("departures_15m"),
            count(is_departure).alias("departures_1h"),
            count(is_arrival & in_last_slide).alias("arrivals_15m"),
            count(is_arrival).alias("arrivals_1h"),
        )
        .select("station_id", F.col("w.end").alias("event_timestamp"), *ACTIVITY_COLUMNS)
    )


def _dense_grid(counts: DataFrame, time_col: str, step: str) -> DataFrame:
    """Fill the gaps between each station's first and last bucket with zero rows."""
    bounds = counts.groupBy("station_id").agg(
        F.min(time_col).alias("first"), F.max(time_col).alias("last")
    )
    grid = bounds.select(
        "station_id",
        F.explode(F.sequence("first", "last", F.expr(f"INTERVAL {step}"))).alias(time_col),
    )
    return grid.join(counts, ["station_id", time_col], "left").fillna(0)


def station_hourly_profile(events: DataFrame) -> DataFrame:
    """Typical demand for each station and hour of the week: the mean of its last four
    occurrences. Stamped with the end of the hour, when the newest count becomes known."""
    hourly = events.groupBy("station_id", F.date_trunc("hour", "event_ts").alias("hour_start")).agg(
        F.sum((F.col("event_type") == "departure").cast("long")).alias("departures"),
        F.sum((F.col("event_type") == "arrival").cast("long")).alias("arrivals"),
    )
    dense = _dense_grid(hourly, "hour_start", "1 HOUR").withColumn(
        "hour_of_week", hour_of_week(F.col("hour_start"))
    )
    last_four = (
        Window.partitionBy("station_id", "hour_of_week").orderBy("hour_start").rowsBetween(-3, 0)
    )
    return dense.select(
        "station_id",
        "hour_of_week",
        (F.col("hour_start") + F.expr("INTERVAL 1 HOUR")).alias("event_timestamp"),
        F.avg("departures").over(last_four).alias("avg_departures_4w"),
        F.avg("arrivals").over(last_four).alias("avg_arrivals_4w"),
    )


def demand_labels(events: DataFrame) -> DataFrame:
    """Training labels: departures in the hour after each 15-minute boundary."""
    buckets = (
        events.groupBy("station_id", F.window("event_ts", ACTIVITY_SLIDE).alias("w"))
        .agg(F.sum((F.col("event_type") == "departure").cast("long")).alias("departures"))
        .select("station_id", F.col("w.start").alias("event_timestamp"), "departures")
    )
    dense = _dense_grid(buckets, "event_timestamp", ACTIVITY_SLIDE)
    ahead = (
        Window.partitionBy("station_id")
        .orderBy("event_timestamp")
        .rowsBetween(0, LABEL_HORIZON_BUCKETS - 1)
    )
    return (
        dense.select(
            "station_id",
            "event_timestamp",
            F.sum("departures").over(ahead).alias("target_departures_next_1h"),
            F.count("departures").over(ahead).alias("_buckets"),
        )
        # Drop the tail, where the next hour is not fully observed yet.
        .where(F.col("_buckets") == LABEL_HORIZON_BUCKETS)
        .drop("_buckets")
    )
