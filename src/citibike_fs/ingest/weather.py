"""Hourly weather from Open-Meteo (no API key).

The backfill writes the offline history; the poller publishes the latest completed hour
to Kafka. Both use hourly values so the definition is the same offline and online.
"""

from __future__ import annotations

import argparse
import logging
import re
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import requests

from citibike_fs import storage
from citibike_fs.config import Settings, get_settings
from citibike_fs.logs import setup_logging

log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,precipitation,wind_speed_10m"

SCHEMA = pa.schema(
    [
        ("region_id", pa.string()),
        ("event_timestamp", pa.timestamp("us", tz="UTC")),
        ("temperature_c", pa.float64()),
        ("precipitation_mm", pa.float64()),
        ("wind_speed_kmh", pa.float64()),
    ]
)


def _to_frame(payload: dict, settings: Settings) -> pd.DataFrame:
    hourly = payload["hourly"]
    df = pd.DataFrame(
        {
            "region_id": settings.region_id,
            "event_timestamp": pd.to_datetime(hourly["time"], utc=True),
            "temperature_c": hourly["temperature_2m"],
            "precipitation_mm": hourly["precipitation"],
            "wind_speed_kmh": hourly["wind_speed_10m"],
        }
    )
    return df.dropna(subset=["temperature_c"])


def _get(url: str, params: dict, settings: Settings) -> pd.DataFrame:
    base = {
        "latitude": settings.weather_latitude,
        "longitude": settings.weather_longitude,
        "hourly": HOURLY_VARS,
        "timezone": "UTC",
    }
    resp = requests.get(url, params={**base, **params}, timeout=60)
    resp.raise_for_status()
    return _to_frame(resp.json(), settings)


def fetch_archive(start: date, end: date, settings: Settings) -> pd.DataFrame:
    return _get(
        ARCHIVE_URL, {"start_date": start.isoformat(), "end_date": end.isoformat()}, settings
    )


def fetch_recent(settings: Settings, past_days: int = 7) -> pd.DataFrame:
    """The last few days up to the latest completed hour, from the forecast model."""
    df = _get(FORECAST_URL, {"past_days": past_days, "forecast_days": 1}, settings)
    return df[df["event_timestamp"] <= pd.Timestamp.now(tz="UTC")]


def latest_hour(settings: Settings) -> dict | None:
    df = fetch_recent(settings, past_days=1)
    if df.empty:
        return None
    row = df.iloc[-1]
    return {
        "region_id": row["region_id"],
        "event_ts_ms": int(row["event_timestamp"].timestamp() * 1000),
        "temperature_c": float(row["temperature_c"]),
        "precipitation_mm": float(row["precipitation_mm"]),
        "wind_speed_kmh": float(row["wind_speed_kmh"]),
    }


def _earliest_trip_month(settings: Settings) -> date | None:
    months = {
        m.group(1)
        for f in storage.list_files(settings.raw_trips, suffix=".csv")
        if (m := re.search(r"/(\d{6})/[^/]+$", f))
    }
    if not months:
        return None
    first = min(months)
    return date(int(first[:4]), int(first[4:]), 1)


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, help="default: first trip month")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    start = args.start or _earliest_trip_month(settings) or today - timedelta(days=120)
    # One day of margin so the first trips of the month have a weather row before them.
    archive = fetch_archive(start - timedelta(days=1), today, settings)
    recent = fetch_recent(settings)
    df = (
        pd.concat([archive, recent])
        .drop_duplicates(subset=["region_id", "event_timestamp"], keep="first")
        .sort_values("event_timestamp")
    )
    # The archive pads the current day with forecast values; keep only observed hours.
    df = df[df["event_timestamp"] <= pd.Timestamp.now(tz="UTC")]
    out = f"{settings.weather}/backfill.parquet"
    storage.write_parquet(pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False), out)
    log.info(
        "wrote %d hourly rows (%s to %s) to %s",
        len(df),
        df["event_timestamp"].min(),
        df["event_timestamp"].max(),
        out,
    )


if __name__ == "__main__":
    setup_logging()
    main()
