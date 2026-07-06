"""The shared transforms: correct counts, and identical output in batch and streaming."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from citibike_fs import features
from citibike_fs.ingest import replayer
from citibike_fs.spark import transforms

UTC = timezone.utc
T0 = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)  # a Monday, 08:00 in New York

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("station_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.TimestampType()),
    ]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_events(spark, rows):
    """rows: (station, type, minutes after T0)."""
    data = [
        (f"e{i}", station, kind, T0 + timedelta(minutes=minutes))
        for i, (station, kind, minutes) in enumerate(rows)
    ]
    return spark.createDataFrame(data, EVENT_SCHEMA)


def as_dict(df):
    return {
        (r["station_id"], r["event_timestamp"].replace(tzinfo=UTC)): r.asDict()
        for r in df.collect()
    }


def test_station_activity_counts_trailing_windows(spark):
    events = make_events(
        spark,
        [
            ("JC1", "departure", 5),
            ("JC1", "departure", 50),
            ("JC1", "arrival", 55),
            ("JC1", "departure", 60),  # exactly on the boundary: belongs to the next window
        ],
    )
    out = as_dict(transforms.station_activity(events))

    at_1h = out[("JC1", T0 + timedelta(hours=1))]
    assert at_1h["departures_1h"] == 2  # minutes 5 and 50; minute 60 is excluded
    assert at_1h["departures_15m"] == 1  # minute 50
    assert at_1h["arrivals_1h"] == 1
    assert at_1h["arrivals_15m"] == 1

    at_1h15 = out[("JC1", T0 + timedelta(minutes=75))]
    assert at_1h15["departures_1h"] == 2  # minutes 50 and 60; minute 5 has aged out
    assert at_1h15["departures_15m"] == 1  # minute 60


def test_station_activity_is_identical_in_batch_and_streaming(spark, tmp_path):
    rows = [
        ("JC1", "departure", 1), ("JC1", "departure", 14), ("JC1", "arrival", 16),
        ("JC2", "arrival", 29), ("JC1", "departure", 47), ("JC2", "departure", 61),
        ("JC2", "departure", 62), ("JC1", "arrival", 95), ("JC2", "arrival", 130),
        ("JC1", "heartbeat", 170),
    ]  # fmt: skip
    expected = as_dict(transforms.station_activity(make_events(spark, rows)))

    # Streaming reads one file per micro-batch. Two later sentinel events advance the
    # watermark far enough for every real window to be finalised and emitted.
    sentinels = [("ZZ", "departure", 24 * 60), ("ZZ", "departure", 48 * 60)]
    for i, batch in enumerate([rows, sentinels[:1], sentinels[1:]]):
        lines = [
            json.dumps(
                {
                    "event_id": f"{i}-{j}",
                    "station_id": station,
                    "event_type": kind,
                    "event_ts": (T0 + timedelta(minutes=minutes)).isoformat(),
                }
            )
            for j, (station, kind, minutes) in enumerate(batch)
        ]
        (tmp_path / f"in-{i}.json").write_text("\n".join(lines))

    stream = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(tmp_path))
        .withWatermark("event_ts", "2 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    query = (
        transforms.station_activity(stream)
        .writeStream.format("memory")
        .queryName("activity_stream")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(120)
    actual = as_dict(spark.table("activity_stream").where(F.col("station_id") != "ZZ"))

    assert actual == expected


def test_heartbeats_give_quiet_stations_a_zero_row_at_every_boundary(spark):
    events = make_events(spark, [("JC1", "departure", 5), ("JC1", "arrival", 125)])
    with_heartbeats = events.unionByName(transforms.heartbeats(events))
    out = as_dict(transforms.station_activity(with_heartbeats))

    boundaries = sorted(ts for station, ts in out)
    steps = {b - a for a, b in zip(boundaries[:-1], boundaries[1:], strict=True)}
    assert steps == {timedelta(minutes=15)}  # no gaps
    quiet = out[("JC1", T0 + timedelta(hours=2))]  # window [1:00, 2:00) has no trips
    assert quiet["departures_1h"] == quiet["arrivals_1h"] == 0
    # Heartbeats themselves are never counted.
    assert out[("JC1", T0 + timedelta(hours=1))]["departures_1h"] == 1


def test_hour_of_week_matches_between_spark_and_pandas(spark):
    # Includes both sides of the November 2026 DST change in New York.
    stamps = [
        datetime(2026, 8, 3, 4, 0, tzinfo=UTC),  # Monday 00:00 local
        datetime(2026, 8, 9, 3, 59, tzinfo=UTC),  # Saturday 23:59 local
        datetime(2026, 11, 1, 5, 30, tzinfo=UTC),
        datetime(2026, 11, 1, 6, 30, tzinfo=UTC),
        datetime(2026, 11, 2, 15, 0, tzinfo=UTC),
    ]
    df = spark.createDataFrame([(s,) for s in stamps], "ts timestamp")
    from_spark = [r[0] for r in df.select(transforms.hour_of_week(F.col("ts"))).collect()]
    from_pandas = features.hour_of_week(pd.Series(stamps)).tolist()

    assert from_spark == from_pandas
    assert from_pandas[0] == 0
    assert from_pandas[1] == 5 * 24 + 23


def test_labels_count_departures_in_the_following_hour(spark):
    events = make_events(
        spark,
        [
            ("JC1", "departure", 0),
            ("JC1", "departure", 20),
            ("JC1", "arrival", 30),
            ("JC1", "departure", 70),
            ("JC1", "departure", 200),
        ],  # fmt: skip
    )
    labels = {
        r["event_timestamp"].replace(tzinfo=UTC): r["target_departures_next_1h"]
        for r in transforms.demand_labels(events).collect()
    }

    assert labels[T0] == 2  # minutes 0 and 20
    assert labels[T0 + timedelta(minutes=15)] == 2  # minutes 20 and 70
    assert labels[T0 + timedelta(minutes=75)] == 0  # a quiet hour is a real zero
    # The final hour is not fully observed, so it gets no label: the last bucket starts
    # at minute 195, and the last complete hour ahead starts three buckets before it.
    assert max(labels) == T0 + timedelta(minutes=195 - 45)


def test_hourly_profile_averages_the_same_hour_of_previous_weeks(spark):
    rows = []
    for week, departures in enumerate([2, 4, 6]):
        rows += [("JC1", "departure", week * 7 * 24 * 60 + m) for m in range(departures)]
    profile = (
        transforms.station_hourly_profile(make_events(spark, rows))
        .where(F.col("hour_of_week") == 8)  # Monday 08:00 local
        .orderBy("event_timestamp")
        .collect()
    )

    assert [r["avg_departures_4w"] for r in profile] == [2.0, 3.0, 4.0]
    # Stamped with the end of the hour, when the count is known.
    assert profile[0]["event_timestamp"].replace(tzinfo=UTC) == T0 + timedelta(hours=1)


def test_replay_shift_preserves_local_hour_of_week():
    events = pd.DataFrame(
        {"local_ts": pd.to_datetime(["2026-08-03 08:15:00", "2026-08-09 23:30:00"])}
    )
    now_local = pd.Timestamp("2026-11-10 09:00:00")  # after the DST change
    weeks = replayer.weeks_to_shift(now_local, events["local_ts"].max())
    shifted = replayer.shift(events, weeks, "America/New_York")

    assert events["local_ts"].max() + weeks * replayer.WEEK >= now_local
    assert events["local_ts"].max() + (weeks - 1) * replayer.WEEK < now_local
    original = features.hour_of_week(events["local_ts"].dt.tz_localize("America/New_York"))
    assert features.hour_of_week(shifted["event_ts"]).tolist() == original.tolist()
