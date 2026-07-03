"""Load the latest batch feature values into the online store.

Only batch-computed views need this. The stream views are written online by the
streaming job as they are computed, and their batch history is too old to serve.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from citibike_fs.logs import setup_logging
from citibike_fs.store import get_store

log = logging.getLogger(__name__)

BATCH_VIEWS = ["station_hourly_profile", "weather_hourly"]


def main() -> None:
    end = datetime.now(timezone.utc)
    get_store().materialize_incremental(end_date=end, feature_views=BATCH_VIEWS)
    log.info("materialized %s up to %s", ", ".join(BATCH_VIEWS), end.isoformat())


if __name__ == "__main__":
    setup_logging()
    main()
