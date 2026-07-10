"""Online feature retrieval with the feature-view TTL applied.

Feast applies a feature view's TTL in point-in-time (offline) joins, but an online read
returns the last value written however old it is. Training therefore sees "missing"
where serving would see a stale number. `get_online_features` closes that gap by
blanking any value older than its view's TTL, and reports how old each view's data is.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from feast import FeatureStore

SEPARATOR = "__"


@dataclass
class OnlineFeatures:
    values: pd.DataFrame  # one row per entity row, columns named by feature
    age_seconds: dict[str, float | None]  # per feature view: age of its newest value
    expired: dict[str, int]  # per feature view: entity rows blanked by the TTL


def get_online_features(
    store: FeatureStore, service_name: str, entity_rows: list[dict], now: pd.Timestamp
) -> OnlineFeatures:
    service = store.get_feature_service(service_name)
    raw = store.get_online_features(
        features=service, entity_rows=entity_rows, full_feature_names=True
    ).to_dict(include_event_timestamps=True)

    ttl_seconds = {}
    for projection in service.feature_view_projections:
        view = store.registry.get_any_feature_view(projection.name, store.project)
        ttl = getattr(view, "ttl", None)
        ttl_seconds[projection.name_to_use()] = ttl.total_seconds() if ttl else None

    now_seconds = now.timestamp()
    values: dict[str, list] = {}
    newest: dict[str, float] = {}
    expired: dict[str, set[int]] = {}
    for column, column_values in raw.items():
        if column.endswith("__ts"):
            continue
        view, _, feature = column.partition(SEPARATOR)
        if not feature:  # an entity key, returned without a view prefix
            values[column] = column_values
            continue
        stamps = raw.get(f"{column}__ts") or [0] * len(column_values)
        ttl = ttl_seconds.get(view)
        cleaned = []
        for row, (value, stamp) in enumerate(zip(column_values, stamps, strict=True)):
            if stamp and ttl is not None and now_seconds - stamp > ttl:
                expired.setdefault(view, set()).add(row)
                value = None
            elif stamp and value is not None:
                newest[view] = max(newest.get(view, 0.0), stamp)
            cleaned.append(value)
        values[feature] = cleaned

    # On-demand views have no TTL and no stored timestamps, so they get no age.
    stored_views = [v for v, ttl in ttl_seconds.items() if ttl is not None]
    return OnlineFeatures(
        # object dtype keeps integers as integers when some rows are missing
        values=pd.DataFrame(values, dtype=object),
        age_seconds={v: (now_seconds - newest[v]) if v in newest else None for v in stored_views},
        expired={v: len(rows) for v, rows in expired.items()},
    )
