"""
db.py
Shared read/write for the scraper's local state (rides_database.json).

Centralizing this is the prerequisite for eventually pointing the scraper
at a remote-backed store (e.g. S3) instead of a local file, without having
to touch every call site that reads or writes the database.
"""

import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

# Env-overridable so the scraper can run off-laptop, where the state file
# lives on a mounted volume rather than next to the code.
DB_PATH = Path(os.environ.get("RIDETRACKER_DB_PATH", BASE_DIR / "data" / "rides_database.json"))

# When set (s3://bucket/key), the local file becomes a working copy:
# pulled before reading, pushed after writing. Unset on the laptop, so
# nothing about the local workflow changes.
S3_URI = os.environ.get("RIDETRACKER_DB_S3_URI", "")


def _split_s3_uri(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise ValueError(f"RIDETRACKER_DB_S3_URI is not a valid s3://bucket/key URI: {uri!r}")
    return bucket, key


def _pull_from_s3(path: Path) -> None:
    """
    Fetch the state file from S3 into path.

    A missing object is fine — that is simply the first run. Any other
    failure is fatal on purpose: carrying on with an empty database would
    make every existing ride look new and duplicate the whole Rides table
    in Airtable.
    """
    import boto3
    from botocore.exceptions import ClientError

    bucket, key = _split_s3_uri(S3_URI)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        boto3.client("s3").download_file(bucket, key, str(path))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            print(f"[db] No state at {S3_URI} yet — starting from empty.")
            return
        raise


def _push_to_s3(path: Path) -> None:
    import boto3

    bucket, key = _split_s3_uri(S3_URI)
    boto3.client("s3").upload_file(str(path), bucket, key)


def load_db(path: Path = DB_PATH) -> list[dict]:
    """Load the rides database. Returns [] if it doesn't exist yet."""
    if S3_URI:
        _pull_from_s3(path)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_db(db: list[dict], path: Path = DB_PATH) -> None:
    """
    Write the rides database atomically.

    Writes to a temp file in the same directory, then renames it into
    place with os.replace (atomic on POSIX and Windows) — a crash or kill
    mid-write can never leave rides_database.json truncated or corrupt.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)
    if S3_URI:
        _push_to_s3(path)
