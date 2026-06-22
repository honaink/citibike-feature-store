"""Download the most recent months of Citi Bike trip history into the raw zone.

Trip files are published monthly at https://s3.amazonaws.com/tripdata. File names are
not perfectly regular (`.zip` vs `.csv.zip`), so the bucket listing is the source of truth.
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET
import zipfile

import requests

from citibike_fs import storage
from citibike_fs.config import get_settings
from citibike_fs.logs import setup_logging

log = logging.getLogger(__name__)

BUCKET_URL = "https://s3.amazonaws.com/tripdata"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def list_monthly_files(prefix: str) -> dict[str, str]:
    """Return {yyyymm: key} for the monthly trip archives with this file prefix."""
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{6}})-citibike-tripdata(\.csv)?\.zip$")
    # An empty prefix (all of NYC) would list every key, so narrow the listing to "20...".
    params = {"list-type": "2", "prefix": prefix or "20"}
    found: dict[str, str] = {}
    while True:
        resp = requests.get(BUCKET_URL, params=params, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for key in root.findall("s3:Contents/s3:Key", S3_NS):
            match = pattern.match(key.text or "")
            if match:
                found[match.group(1)] = key.text
        token = root.find("s3:NextContinuationToken", S3_NS)
        if token is None:
            return found
        params["continuation-token"] = token.text


def download_month(month: str, key: str, dest_root: str, force: bool = False) -> int:
    dest_dir = f"{dest_root}/{month}"
    if not force and storage.list_files(dest_dir, suffix=".csv"):
        log.info("%s already present, skipping", month)
        return 0
    written = 0
    with tempfile.TemporaryFile() as tmp:
        with requests.get(f"{BUCKET_URL}/{key}", stream=True, timeout=300) as resp:
            resp.raise_for_status()
            shutil.copyfileobj(resp.raw, tmp, length=1 << 20)
        tmp.seek(0)
        with zipfile.ZipFile(tmp) as archive:
            for member in archive.namelist():
                name = member.rsplit("/", 1)[-1]
                if (
                    member.startswith("__MACOSX")
                    or name.startswith(".")
                    or not name.endswith(".csv")
                ):
                    continue
                with archive.open(member) as src, storage.open_output(f"{dest_dir}/{name}") as dst:
                    shutil.copyfileobj(src, dst, length=1 << 20)
                written += 1
    log.info("%s: wrote %d csv file(s) to %s", month, written, dest_dir)
    return written


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", type=int, default=settings.trip_months)
    parser.add_argument("--force", action="store_true", help="re-download months already present")
    args = parser.parse_args()

    available = list_monthly_files(settings.trip_file_prefix)
    if not available:
        raise SystemExit(f"no trip files found for prefix {settings.trip_file_prefix!r}")
    for month in sorted(available)[-args.months :]:
        download_month(month, available[month], settings.raw_trips, force=args.force)


if __name__ == "__main__":
    setup_logging()
    main()
