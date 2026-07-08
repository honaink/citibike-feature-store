"""Build a point-in-time-correct training set from the feature store and train the model.

Each label row is (station, timestamp, departures in the following hour). Feast joins
every feature as it was known at that timestamp, so nothing from the future leaks in.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import lightgbm as lgb
import numpy as np
import pandas as pd

from citibike_fs import storage
from citibike_fs.config import get_settings
from citibike_fs.features import (
    FEATURE_SERVICE,
    MODEL_FEATURES,
    TARGET,
    add_entity_keys,
    to_model_frame,
)
from citibike_fs.logs import setup_logging
from citibike_fs.store import get_store

log = logging.getLogger(__name__)

VALIDATION_FRACTION = 0.2


def build_training_set(max_rows: int) -> pd.DataFrame:
    settings = get_settings()
    labels = storage.read_parquet(settings.labels)
    if len(labels) > max_rows:
        labels = labels.sample(max_rows, random_state=7)
    entity_df = add_entity_keys(labels)
    store = get_store()
    log.info("joining features for %d label rows", len(entity_df))
    return store.get_historical_features(
        entity_df=entity_df, features=store.get_feature_service(FEATURE_SERVICE)
    ).to_df()


def evaluate(y: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(np.mean(np.abs(y - pred))),
        "rmse": float(np.sqrt(np.mean((y - pred) ** 2))),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=400_000)
    args = parser.parse_args()
    settings = get_settings()

    data = build_training_set(args.max_rows).sort_values("event_timestamp", ignore_index=True)
    # Split on time, not at random: validate on the most recent stretch.
    cutoff = data["event_timestamp"].quantile(1 - VALIDATION_FRACTION)
    is_valid = data["event_timestamp"] > cutoff
    x, y = to_model_frame(data), data[TARGET].to_numpy(dtype="float64")
    x_train, y_train, x_valid, y_valid = x[~is_valid], y[~is_valid], x[is_valid], y[is_valid]
    log.info("train rows %d, validation rows %d, cutoff %s", len(x_train), len(x_valid), cutoff)

    booster = lgb.train(
        {
            "objective": "poisson",
            "learning_rate": 0.05,
            "num_leaves": 63,
            "min_data_in_leaf": 50,
            "feature_fraction": 0.9,
            "verbosity": -1,
            "seed": 7,
        },
        lgb.Dataset(x_train, y_train),
        num_boost_round=600,
        valid_sets=[lgb.Dataset(x_valid, y_valid)],
        callbacks=[lgb.early_stopping(30, verbose=False)],
    )

    metrics = {
        "model": evaluate(y_valid, booster.predict(x_valid)),
        # What you get from slow-moving batch features alone.
        "baseline_profile": evaluate(y_valid, x_valid["avg_departures_4w"].to_numpy()),
        # What you get from repeating the last hour.
        "baseline_last_hour": evaluate(y_valid, x_valid["departures_1h"].to_numpy()),
    }
    importance = dict(
        sorted(
            zip(MODEL_FEATURES, booster.feature_importance("gain").round(1).tolist(), strict=True),
            key=lambda kv: -kv[1],
        )
    )
    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_service": FEATURE_SERVICE,
        "features": MODEL_FEATURES,
        "rows": {"train": int(len(x_train)), "validation": int(len(x_valid))},
        "validation_from": cutoff.isoformat(),
        "best_iteration": booster.best_iteration,
        "metrics": metrics,
        "feature_importance_gain": importance,
    }
    storage.write_text(
        booster.model_to_string(num_iteration=booster.best_iteration),
        f"{settings.model_dir}/model.txt",
    )
    storage.write_json(metadata, f"{settings.model_dir}/metadata.json")

    log.info("validation metrics (departures in the next hour):")
    for name, m in metrics.items():
        log.info("  %-20s MAE %.3f  RMSE %.3f", name, m["mae"], m["rmse"])
    log.info("saved model to %s", settings.model_dir)


if __name__ == "__main__":
    setup_logging()
    main()
