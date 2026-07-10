"""Prediction API: online features from Feast in, next-hour departures out.

The model is read from the artifact store and reloaded when a newer one is trained.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from citibike_fs import storage
from citibike_fs.config import get_settings
from citibike_fs.features import (
    FEATURE_SERVICE,
    LIVE_FEATURE_SERVICE,
    MODEL_FEATURES,
    add_entity_keys,
    to_model_frame,
)
from citibike_fs.logs import setup_logging
from citibike_fs.serving.online import get_online_features
from citibike_fs.store import get_store

setup_logging()
log = logging.getLogger(__name__)

MODEL_CHECK_INTERVAL_S = 60


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # The first Feast lookup loads the registry and connects to the online store; do it
    # before taking traffic. Failure is fine here: the store may not be set up yet.
    try:
        store = get_store()
        store.get_online_features(
            features=store.get_feature_service(FEATURE_SERVICE),
            entity_rows=[{"station_id": "warmup", "hour_of_week": 0, "region_id": "warmup"}],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("feature store not ready at startup: %s", exc)
    yield


app = FastAPI(title="Citi Bike demand forecast", version="1.0", lifespan=lifespan)


class ModelHolder:
    """Loads the model lazily and picks up retrained versions."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._checked_at = 0.0
        self.booster: lgb.Booster | None = None
        self.metadata: dict[str, Any] | None = None

    def get(self) -> tuple[lgb.Booster, dict[str, Any]]:
        with self._lock:
            if time.monotonic() - self._checked_at > MODEL_CHECK_INTERVAL_S or self.booster is None:
                self._checked_at = time.monotonic()
                self._refresh()
        if self.booster is None or self.metadata is None:
            raise HTTPException(503, "no trained model yet; run the training step")
        return self.booster, self.metadata

    def _refresh(self) -> None:
        model_dir = get_settings().model_dir
        try:
            metadata = storage.read_json(f"{model_dir}/metadata.json")
        except (FileNotFoundError, OSError):
            return
        if self.metadata and metadata["trained_at"] == self.metadata["trained_at"]:
            return
        if metadata["features"] != MODEL_FEATURES:
            log.error("model features %s do not match the code; retrain", metadata["features"])
            return
        self.booster = lgb.Booster(model_str=storage.read_text(f"{model_dir}/model.txt"))
        self.metadata = metadata
        log.info("loaded model trained at %s", metadata["trained_at"])


model = ModelHolder()


def load_stations() -> pd.DataFrame:
    try:
        return storage.read_parquet(get_settings().stations).set_index("station_id")
    except (FileNotFoundError, OSError):
        return pd.DataFrame(columns=["name", "lat", "lon"]).rename_axis("station_id")


class PredictRequest(BaseModel):
    station_ids: list[str] | None = None  # default: every known station


def _clean(value: Any) -> Any:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    return value


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/model")
def model_info() -> dict[str, Any]:
    return model.get()[1]


@app.get("/stations")
def stations() -> list[dict[str, Any]]:
    return load_stations().reset_index().to_dict("records")


@app.post("/predict")
def predict(request: PredictRequest) -> dict[str, Any]:
    booster, metadata = model.get()
    station_table = load_stations()
    station_ids = request.station_ids or station_table.index.tolist()
    if not station_ids:
        raise HTTPException(404, "no stations known yet; run the stations step")

    now = pd.Timestamp.now(tz="UTC")
    entities = add_entity_keys(pd.DataFrame({"station_id": station_ids, "event_timestamp": now}))
    store = get_store()

    started = time.perf_counter()
    demand = get_online_features(
        store,
        FEATURE_SERVICE,
        entities[["station_id", "hour_of_week", "region_id"]].to_dict("records"),
        now,
    )
    live = get_online_features(
        store, LIVE_FEATURE_SERVICE, [{"station_id": s} for s in station_ids], now
    )
    feature_ms = (time.perf_counter() - started) * 1000

    inputs = demand.values.assign(hour_of_week=entities["hour_of_week"].to_numpy())
    predictions = np.clip(booster.predict(to_model_frame(inputs)), 0, None)
    feature_columns = [c for c in demand.values.columns if c not in entities.columns]

    results = []
    for i, station_id in enumerate(station_ids):
        info = station_table.loc[station_id] if station_id in station_table.index else None
        bikes = _clean(live.values["num_bikes_available"].iloc[i])
        predicted = round(float(predictions[i]), 2)
        results.append(
            {
                "station_id": station_id,
                "name": None if info is None else info["name"],
                "lat": None if info is None else float(info["lat"]),
                "lon": None if info is None else float(info["lon"]),
                "predicted_departures_next_hour": predicted,
                "bikes_available": bikes,
                "docks_available": _clean(live.values["num_docks_available"].iloc[i]),
                "is_renting": _clean(live.values["is_renting"].iloc[i]),
                # More departures expected than bikes on hand.
                "stockout_risk": bikes is not None and predicted > bikes,
                "features": {f: _clean(inputs[f].iloc[i]) for f in feature_columns},
            }
        )

    return {
        "generated_at": now.isoformat(),
        "model_trained_at": metadata["trained_at"],
        "feature_retrieval_ms": round(feature_ms, 1),
        "feature_age_seconds": {**demand.age_seconds, **live.age_seconds},
        "expired_by_ttl": {**demand.expired, **live.expired},
        "stations": results,
    }
