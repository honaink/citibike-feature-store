"""Replay historical trips as a live event stream.

Citi Bike publishes trips monthly, so there is no public live trip feed. This stands in
for one: it takes the most recent month of history, shifts it forward by a whole number
of weeks so that it lines up with the present, and emits each departure and arrival at
its shifted time. Shifting by whole weeks in local time keeps hour-of-week patterns intact.
"""

from __future__ import annotations

import logging
import math
import re
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

from citibike_fs import storage
from citibike_fs.config import Settings, get_settings
from citibike_fs.kafka_utils import JsonProducer, ensure_topics
from citibike_fs.logs import setup_logging

log = logging.getLogger(__name__)

WEEK = pd.Timedelta(weeks=1)
# On start, emit the last 75 minutes at once so the 1-hour windows are full immediately.
WARMUP = pd.Timedelta(minutes=75)
COLUMNS = ["ride_id", "started_at", "ended_at", "start_station_id", "end_station_id"]


def load_events(settings: Settings) -> pd.DataFrame:
    """Departure and arrival events from the latest month, with naive local timestamps."""
    files = storage.list_files(settings.raw_trips, suffix=".csv")
    months = sorted({m.group(1) for f in files if (m := re.search(r"/(\d{6})/[^/]+$", f))})
    if not months:
        raise SystemExit(f"no trip files under {settings.raw_trips}; run the download step first")
    latest = [f for f in files if f"/{months[-1]}/" in f]
    convert = pacsv.ConvertOptions(
        include_columns=COLUMNS, column_types={c: pa.string() for c in COLUMNS}
    )
    tables = []
    for path in latest:
        with storage.open_input(path) as f:
            tables.append(pacsv.read_csv(f, convert_options=convert))
    trips = pa.concat_tables(tables).to_pandas()

    departures = trips[["ride_id", "start_station_id", "started_at"]].set_axis(
        ["ride_id", "station_id", "local_ts"], axis=1
    )
    departures["event_type"] = "departure"
    arrivals = trips[["ride_id", "end_station_id", "ended_at"]].set_axis(
        ["ride_id", "station_id", "local_ts"], axis=1
    )
    arrivals["event_type"] = "arrival"
    events = pd.concat([departures, arrivals], ignore_index=True)
    events = events[events["station_id"].notna() & (events["station_id"] != "")]
    events = events[events["station_id"].map(settings.in_scope)]
    events["local_ts"] = pd.to_datetime(events["local_ts"], errors="coerce")
    events = events.dropna(subset=["local_ts"])
    log.info(
        "loaded %d events from %s (%s to %s local)",
        len(events),
        months[-1],
        events["local_ts"].min(),
        events["local_ts"].max(),
    )
    return events


def weeks_to_shift(now_local: pd.Timestamp, data_end: pd.Timestamp) -> int:
    """Smallest whole-week shift that puts `now` inside the data rather than past its end."""
    return max(0, math.ceil((now_local - data_end) / WEEK))


def shift(events: pd.DataFrame, weeks: int, tz: str) -> pd.DataFrame:
    """Shift by whole weeks of local wall-clock time and convert to UTC, sorted by time."""
    shifted = (events["local_ts"] + weeks * WEEK).dt.tz_localize(
        tz, ambiguous="NaT", nonexistent="NaT"
    )
    out = events.assign(event_ts=shifted.dt.tz_convert("UTC"), weeks=weeks)
    return out.dropna(subset=["event_ts"]).sort_values("event_ts", ignore_index=True)


def main() -> None:
    settings = get_settings()
    ensure_topics([settings.topic_trip_events], settings)
    producer = JsonProducer(settings)
    events = load_events(settings)
    data_start, data_end = events["local_ts"].min(), events["local_ts"].max()
    if data_end - data_start < WEEK + WARMUP:
        raise SystemExit("need more than a week of trips to replay")

    def now_local() -> pd.Timestamp:
        return pd.Timestamp.now(tz=settings.timezone).tz_localize(None)

    weeks = weeks_to_shift(now_local(), data_end)
    timeline = shift(events, weeks, settings.timezone)
    times = timeline["event_ts"].to_numpy(dtype="datetime64[ns]")
    cursor = pd.Timestamp.now(tz="UTC") - WARMUP
    log.info("replaying with a shift of %d weeks, starting at %s", weeks, cursor)
    emitted, reported_at = 0, float("-inf")

    while True:
        now = pd.Timestamp.now(tz="UTC")
        lo = np.searchsorted(times, cursor.to_datetime64(), side="right")
        hi = np.searchsorted(times, now.to_datetime64(), side="right")
        for row in timeline.iloc[lo:hi].itertuples(index=False):
            producer.send(
                settings.topic_trip_events,
                row.station_id,
                {
                    # Unique per replay pass, so the stream job can drop redelivered events.
                    "event_id": f"{row.ride_id}:{row.event_type[0]}:{row.weeks}",
                    "ride_id": row.ride_id,
                    "event_type": row.event_type,
                    "station_id": row.station_id,
                    "event_ts_ms": int(row.event_ts.timestamp() * 1000),
                },
            )
        if hi > lo:
            producer.flush()
            emitted += hi - lo
        if time.monotonic() - reported_at >= 60:
            log.info("emitted %d events, replay clock at %s", emitted, now.strftime("%H:%M:%S"))
            reported_at, emitted = time.monotonic(), 0
        cursor = now

        # When the shifted data runs out, step back one more week and carry on.
        wanted = weeks_to_shift(now_local(), data_end)
        if wanted != weeks:
            weeks = wanted
            timeline = shift(events, weeks, settings.timezone)
            times = timeline["event_ts"].to_numpy(dtype="datetime64[ns]")
            log.info("reached the end of the data; shift is now %d weeks", weeks)
        time.sleep(1)


if __name__ == "__main__":
    setup_logging()
    main()
