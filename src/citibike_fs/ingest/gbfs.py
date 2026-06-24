"""Citi Bike's live GBFS feed: station reference data and per-station availability.

Trip history identifies stations by what GBFS calls `short_name` (e.g. "HB303"), so
that is the `station_id` used everywhere in this project.
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
import pyarrow as pa
import requests

from citibike_fs import storage
from citibike_fs.config import Settings, get_settings
from citibike_fs.logs import setup_logging

log = logging.getLogger(__name__)

GBFS_BASE = "https://gbfs.lyft.com/gbfs/1.1/bkn/en"


def _fetch(feed: str) -> dict:
    resp = requests.get(f"{GBFS_BASE}/{feed}.json", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_stations(settings: Settings) -> pd.DataFrame:
    """Stations in the configured scope, keyed by the trip-history station id."""
    rows = [
        {
            "station_id": s["short_name"],
            "gbfs_station_id": s["station_id"],
            "name": s["name"],
            "lat": float(s["lat"]),
            "lon": float(s["lon"]),
        }
        for s in _fetch("station_information")["data"]["stations"]
        if s.get("short_name") and settings.in_scope(s["short_name"])
    ]
    return pd.DataFrame(rows)


def fetch_status(id_map: dict[str, str]) -> list[dict]:
    """One availability record per known, installed station, stamped with the feed time."""
    payload = _fetch("station_status")
    event_ts_ms = int(payload["last_updated"]) * 1000
    records = []
    for s in payload["data"]["stations"]:
        station_id = id_map.get(s["station_id"])
        if station_id is None or not s.get("is_installed"):
            continue
        records.append(
            {
                "station_id": station_id,
                "event_ts_ms": event_ts_ms,
                "num_bikes_available": int(s.get("num_bikes_available", 0)),
                "num_ebikes_available": int(s.get("num_ebikes_available", 0)),
                "num_docks_available": int(s.get("num_docks_available", 0)),
                "num_bikes_disabled": int(s.get("num_bikes_disabled", 0)),
                "num_docks_disabled": int(s.get("num_docks_disabled", 0)),
                "is_renting": int(s.get("is_renting", 0)),
                "is_returning": int(s.get("is_returning", 0)),
            }
        )
    return records


def main() -> None:
    """Write the station reference table used by the API and dashboard."""
    settings = get_settings()
    argparse.ArgumentParser(description=main.__doc__).parse_args()
    stations = fetch_stations(settings)
    storage.write_parquet(pa.Table.from_pandas(stations, preserve_index=False), settings.stations)
    log.info("wrote %d stations to %s", len(stations), settings.stations)


if __name__ == "__main__":
    setup_logging()
    main()
