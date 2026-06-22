"""Environment-driven settings shared by every component.

The same code runs locally (Docker Compose) and on AWS; only these values change.
`DATA_ROOT` is a local directory in the first case and an `s3://bucket` URI in the second.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


# Which slice of the Citi Bike system to use, chosen with CITIBIKE_SCOPE.
# Jersey City + Hoboken is ~110 stations and ~100k trips a month, which suits a laptop;
# NYC is ~2,400 stations and several million trips a month.
_JC_STATION_PREFIXES = ("JC", "HB")
SCOPES = {
    "jc": {"trip_file_prefix": "JC-", "latitude": 40.72, "longitude": -74.05},
    "nyc": {"trip_file_prefix": "", "latitude": 40.73, "longitude": -73.99},
}


@dataclass(frozen=True)
class Settings:
    env: str = field(default_factory=lambda: _env("CITIBIKE_ENV", "local"))
    data_root: str = field(default_factory=lambda: _env("DATA_ROOT", "/data").rstrip("/"))
    aws_region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    scope: str = field(default_factory=lambda: _env("CITIBIKE_SCOPE", "jc"))
    trip_months: int = field(default_factory=lambda: int(_env("TRIP_MONTHS", "3")))
    timezone: str = "America/New_York"

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    )
    # "none" for the local broker, "msk_iam" for MSK Serverless.
    kafka_auth: str = field(default_factory=lambda: _env("KAFKA_AUTH", "none"))
    topic_trip_events: str = "citibike.trip_events"
    topic_station_status: str = "citibike.station_status"
    topic_weather: str = "citibike.weather"

    feast_repo_path: str = field(
        default_factory=lambda: _env("FEAST_REPO_PATH", "/app/feature_repo")
    )

    @property
    def feast_yaml(self) -> str:
        default = os.path.join(self.feast_repo_path, f"feature_store.{self.env}.yaml")
        return _env("FEAST_FS_YAML_FILE_PATH", default)

    @property
    def is_aws(self) -> bool:
        return self.env == "aws"

    # --- scope ------------------------------------------------------------
    @property
    def trip_file_prefix(self) -> str:
        return SCOPES[self.scope]["trip_file_prefix"]

    @property
    def region_id(self) -> str:
        """Join key of the weather feature view."""
        return self.scope

    @property
    def weather_latitude(self) -> float:
        return SCOPES[self.scope]["latitude"]

    @property
    def weather_longitude(self) -> float:
        return SCOPES[self.scope]["longitude"]

    def in_scope(self, station_id: str) -> bool:
        is_jc = station_id.startswith(_JC_STATION_PREFIXES)
        return is_jc if self.scope == "jc" else not is_jc

    def path(self, *parts: str) -> str:
        return "/".join([self.data_root, *parts])

    # --- data lake layout -------------------------------------------------
    @property
    def raw_trips(self) -> str:
        return self.path("raw", "trips")

    @property
    def stations(self) -> str:
        return self.path("reference", "stations", "stations.parquet")

    @property
    def recent_activity(self) -> str:
        return self.path("features", "station_recent_activity")

    @property
    def hourly_profile(self) -> str:
        return self.path("features", "station_hourly_profile")

    @property
    def weather(self) -> str:
        return self.path("features", "weather_hourly")

    @property
    def live_status(self) -> str:
        return self.path("features", "station_live_status")

    @property
    def labels(self) -> str:
        return self.path("training", "labels")

    @property
    def stream_log(self) -> str:
        return self.path("stream_log")

    @property
    def checkpoints(self) -> str:
        return self.path("checkpoints")

    @property
    def model_dir(self) -> str:
        return self.path("artifacts", "model")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
