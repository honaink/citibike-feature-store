"""Streaming job: Kafka topics to the online store, with an offline copy of each row.

Three independent queries share one Spark application:
  trip events    -> station_recent_activity (same transform as the batch backfill,
                    with station status as the heartbeat)
  station status -> station_live_status     (latest availability per station)
  weather        -> weather_hourly          (latest observation per region)
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pandas as pd
from feast.data_source import PushMode
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from citibike_fs.config import Settings, get_settings
from citibike_fs.kafka_utils import spark_kafka_options
from citibike_fs.logs import setup_logging
from citibike_fs.spark import transforms
from citibike_fs.spark.session import build_session
from citibike_fs.store import get_store

log = logging.getLogger(__name__)

# How long to wait for late trip events before a window is final.
WATERMARK = "2 minutes"
TRIGGER = "30 seconds"

TRIP_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("station_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts_ms", T.LongType()),
    ]
)
STATUS_FIELDS = ["num_bikes_available", "num_ebikes_available", "num_docks_available", "is_renting"]
STATUS_SCHEMA = T.StructType(
    [T.StructField("station_id", T.StringType()), T.StructField("event_ts_ms", T.LongType())]
    + [T.StructField(f, T.LongType()) for f in STATUS_FIELDS]
)
WEATHER_FIELDS = ["temperature_c", "precipitation_mm", "wind_speed_kmh"]
WEATHER_SCHEMA = T.StructType(
    [T.StructField("region_id", T.StringType()), T.StructField("event_ts_ms", T.LongType())]
    + [T.StructField(f, T.DoubleType()) for f in WEATHER_FIELDS]
)


def read_topic(spark: SparkSession, topic: str, schema: T.StructType) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .options(**spark_kafka_options())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # Topics keep 24 hours; after a longer outage, resume from what is left.
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m"))
        .select("m.*")
        .where(F.col("event_ts_ms").isNotNull())
    )


def feast_sink(push_source: str, key: str, offline_path: str) -> Callable[[DataFrame, int], None]:
    """Write each micro-batch to Parquet, then push the newest row per key to Feast."""

    def sink(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        try:
            # Timestamps cross to pandas as epoch microseconds to stay timezone-exact.
            rows = (
                batch.withColumn("_ts", F.unix_micros("event_timestamp"))
                .drop("event_timestamp")
                .collect()
            )
            if not rows:
                return
            batch.coalesce(1).write.mode("append").parquet(offline_path)
            pdf = pd.DataFrame([row.asDict() for row in rows])
            pdf["event_timestamp"] = pd.to_datetime(pdf.pop("_ts"), unit="us", utc=True)
            latest = pdf.sort_values("event_timestamp").drop_duplicates(key, keep="last")
            get_store().push(push_source, latest, to=PushMode.ONLINE)
            log.info("batch %d: pushed %d rows to %s", batch_id, len(latest), push_source)
        finally:
            batch.unpersist()

    return sink


def start_query(df: DataFrame, name: str, sink: Callable, settings: Settings):
    return (
        df.writeStream.queryName(name)
        .outputMode("append")
        .foreachBatch(sink)
        .option("checkpointLocation", f"{settings.checkpoints}/{name}")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def main() -> None:
    settings = get_settings()
    spark = build_session("citibike-streaming-features")

    trip_events = read_topic(spark, settings.topic_trip_events, TRIP_SCHEMA).select(
        "event_id", "station_id", "event_type", F.timestamp_millis("event_ts_ms").alias("event_ts")
    )
    # Every station reports its status each minute, so the status feed doubles as the
    # heartbeat that gives quiet stations a zero row (the backfill synthesises these).
    heartbeats = read_topic(spark, settings.topic_station_status, STATUS_SCHEMA).select(
        F.concat(F.lit("hb:"), "station_id", F.lit(":"), "event_ts_ms").alias("event_id"),
        "station_id",
        F.lit(transforms.HEARTBEAT).alias("event_type"),
        F.timestamp_millis("event_ts_ms").alias("event_ts"),
    )
    activity_events = (
        trip_events.unionByName(heartbeats)
        .withWatermark("event_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    start_query(
        transforms.station_activity(activity_events),
        "station_recent_activity",
        # The trip stream is a replay, so its offline copy is kept apart from the
        # batch table. With a real feed this would be `settings.recent_activity`.
        feast_sink(
            "station_recent_activity_push",
            "station_id",
            f"{settings.stream_log}/station_recent_activity",
        ),
        settings,
    )

    status = read_topic(spark, settings.topic_station_status, STATUS_SCHEMA).select(
        "station_id", F.timestamp_millis("event_ts_ms").alias("event_timestamp"), *STATUS_FIELDS
    )
    start_query(
        status,
        "station_live_status",
        feast_sink("station_live_status_push", "station_id", settings.live_status),
        settings,
    )

    weather = read_topic(spark, settings.topic_weather, WEATHER_SCHEMA).select(
        "region_id", F.timestamp_millis("event_ts_ms").alias("event_timestamp"), *WEATHER_FIELDS
    )
    start_query(
        weather,
        "weather_hourly",
        feast_sink("weather_hourly_push", "region_id", settings.weather),
        settings,
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    setup_logging()
    main()
