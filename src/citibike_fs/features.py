"""Feature-side logic shared by training and serving.

Anything that turns feature-store output into model input lives here, so the offline
and online paths cannot drift apart.
"""

from __future__ import annotations

import pandas as pd

from citibike_fs.config import get_settings

FEATURE_SERVICE = "demand_forecast_v1"
LIVE_FEATURE_SERVICE = "station_live_v1"
TARGET = "target_departures_next_1h"

# Stream features. Missing only for a station with no history or when the stream has
# stalled past the TTL; counted as zero activity either way.
ACTIVITY_FEATURES = ["departures_15m", "departures_1h", "arrivals_15m", "arrivals_1h"]
PROFILE_FEATURES = ["avg_departures_4w", "avg_arrivals_4w"]
WEATHER_FEATURES = ["temperature_c", "precipitation_mm", "wind_speed_kmh"]
ON_DEMAND_FEATURES = ["typical_net_flow_4w"]
# Derived below from the (TTL-checked) stored features.
DERIVED_FEATURES = ["net_flow_1h", "demand_vs_profile"]
CALENDAR_FEATURES = ["hour_of_day", "day_of_week"]

MODEL_FEATURES = (
    ACTIVITY_FEATURES
    + PROFILE_FEATURES
    + WEATHER_FEATURES
    + ON_DEMAND_FEATURES
    + DERIVED_FEATURES
    + CALENDAR_FEATURES
)


def hour_of_week(ts: pd.Series) -> pd.Series:
    """Monday 00:00 local time = 0 ... Sunday 23:00 = 167. Must match `transforms.hour_of_week`."""
    local = pd.to_datetime(ts, utc=True).dt.tz_convert(get_settings().timezone)
    return (local.dt.dayofweek * 24 + local.dt.hour).astype("int64")


def add_entity_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Add the join keys Feast needs beyond `station_id` and `event_timestamp`."""
    out = df.copy()
    out["hour_of_week"] = hour_of_week(out["event_timestamp"])
    out["region_id"] = get_settings().region_id
    return out


def to_model_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Order and clean the columns exactly as the model expects them."""
    out = pd.DataFrame(index=df.index)
    for col in ACTIVITY_FEATURES + PROFILE_FEATURES + ON_DEMAND_FEATURES:
        out[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    for col in WEATHER_FEATURES:
        # LightGBM handles missing weather natively.
        out[col] = pd.to_numeric(df[col], errors="coerce")
    out["net_flow_1h"] = out["arrivals_1h"] - out["departures_1h"]
    out["demand_vs_profile"] = out["departures_1h"] / (out["avg_departures_4w"] + 1.0)
    how = df["hour_of_week"].astype("int64")
    out["hour_of_day"] = how % 24
    out["day_of_week"] = how // 24
    return out[MODEL_FEATURES].astype("float64")
