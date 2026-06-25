"""Poll the live feeds and publish them to Kafka.

Station availability is polled every minute (the GBFS feed's own TTL); weather every
ten minutes. A failed poll is logged and retried on the next tick.
"""

from __future__ import annotations

import logging
import time

from citibike_fs.config import get_settings
from citibike_fs.ingest import gbfs, weather
from citibike_fs.kafka_utils import JsonProducer, ensure_topics
from citibike_fs.logs import setup_logging

log = logging.getLogger(__name__)

STATUS_INTERVAL_S = 60
WEATHER_INTERVAL_S = 600
STATIONS_REFRESH_S = 3600


def main() -> None:
    settings = get_settings()
    ensure_topics([settings.topic_station_status, settings.topic_weather], settings)
    producer = JsonProducer(settings)

    id_map: dict[str, str] = {}
    stations_at = weather_at = float("-inf")
    last_weather_ts = None
    while True:
        tick = time.monotonic()
        try:
            if not id_map or tick - stations_at > STATIONS_REFRESH_S:
                stations = gbfs.fetch_stations(settings)
                id_map = dict(zip(stations["gbfs_station_id"], stations["station_id"], strict=True))
                stations_at = tick
            records = gbfs.fetch_status(id_map)
            for record in records:
                producer.send(settings.topic_station_status, record["station_id"], record)
            log.info("published status for %d stations", len(records))
        except Exception:
            log.exception("station status poll failed")

        if tick - weather_at > WEATHER_INTERVAL_S:
            try:
                record = weather.latest_hour(settings)
                # Publish each hourly observation once, not on every poll.
                if record and record["event_ts_ms"] != last_weather_ts:
                    producer.send(settings.topic_weather, record["region_id"], record)
                    last_weather_ts = record["event_ts_ms"]
                    log.info("published weather %s", record)
                weather_at = tick
            except Exception:
                log.exception("weather poll failed")

        producer.flush()
        time.sleep(max(0.0, STATUS_INTERVAL_S - (time.monotonic() - tick)))


if __name__ == "__main__":
    setup_logging()
    main()
