"""Small filesystem helpers that work the same on a local path and on s3://."""

from __future__ import annotations

import json
from typing import Any, BinaryIO

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq


def _resolve(uri: str) -> tuple[pafs.FileSystem, str]:
    if "://" in uri:
        return pafs.FileSystem.from_uri(uri)
    return pafs.LocalFileSystem(), uri


def _ensure_parent(fs: pafs.FileSystem, path: str) -> None:
    parent = path.rsplit("/", 1)[0]
    if isinstance(fs, pafs.LocalFileSystem) and parent:
        fs.create_dir(parent, recursive=True)


def open_output(uri: str) -> BinaryIO:
    fs, path = _resolve(uri)
    _ensure_parent(fs, path)
    return fs.open_output_stream(path)


def open_input(uri: str) -> BinaryIO:
    fs, path = _resolve(uri)
    return fs.open_input_stream(path)


def exists(uri: str) -> bool:
    fs, path = _resolve(uri)
    return fs.get_file_info(path).type != pafs.FileType.NotFound


def list_files(uri: str, suffix: str = "") -> list[str]:
    """Recursively list files under `uri`, returned as paths usable with this module."""
    fs, path = _resolve(uri)
    if fs.get_file_info(path).type == pafs.FileType.NotFound:
        return []
    prefix = uri[: len(uri) - len(path)] if "://" in uri else ""
    infos = fs.get_file_info(pafs.FileSelector(path, recursive=True))
    return sorted(
        prefix + info.path
        for info in infos
        if info.type == pafs.FileType.File and info.path.endswith(suffix)
    )


def read_parquet(uri: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a parquet file or a directory of parquet files (e.g. Spark output)."""
    fs, path = _resolve(uri)
    dataset = ds.dataset(path, filesystem=fs, format="parquet", exclude_invalid_files=True)
    return dataset.to_table(columns=columns).to_pandas()


def write_parquet(table: pa.Table, uri: str) -> None:
    fs, path = _resolve(uri)
    _ensure_parent(fs, path)
    pq.write_table(table, path, filesystem=fs, coerce_timestamps="us")


def write_json(obj: Any, uri: str) -> None:
    with open_output(uri) as f:
        f.write(json.dumps(obj, indent=2, default=str).encode())


def read_json(uri: str) -> Any:
    with open_input(uri) as f:
        return json.loads(f.read())


def write_text(text: str, uri: str) -> None:
    with open_output(uri) as f:
        f.write(text.encode())


def read_text(uri: str) -> str:
    with open_input(uri) as f:
        return f.read().decode()
