from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from feast import FeatureStore

from citibike_fs.config import get_settings


@lru_cache(maxsize=1)
def get_store() -> FeatureStore:
    settings = get_settings()
    return FeatureStore(repo_path=settings.feast_repo_path, fs_yaml_file=Path(settings.feast_yaml))
