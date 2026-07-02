"""Batch backfill: raw trip history to offline feature tables and training labels."""

from __future__ import annotations

import logging

from citibike_fs.config import get_settings
from citibike_fs.logs import setup_logging
from citibike_fs.spark import transforms
from citibike_fs.spark.session import build_session

log = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    spark = build_session("citibike-batch-features")

    raw = spark.read.option("header", True).csv(f"{settings.raw_trips}/*/*.csv")
    trips = transforms.normalize_trips(raw)
    events = transforms.in_scope(transforms.trips_to_events(trips)).cache()

    outputs = {
        settings.recent_activity: transforms.station_activity(
            events.unionByName(transforms.heartbeats(events))
        ),
        settings.hourly_profile: transforms.station_hourly_profile(events),
        settings.labels: transforms.demand_labels(events),
    }
    for path, df in outputs.items():
        df.write.mode("overwrite").parquet(path)
        log.info("wrote %d rows to %s", spark.read.parquet(path).count(), path)

    spark.stop()


if __name__ == "__main__":
    setup_logging()
    main()
