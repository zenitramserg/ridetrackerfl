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
DB_PATH = BASE_DIR / "data" / "rides_database.json"


def load_db(path: Path = DB_PATH) -> list[dict]:
    """Load the rides database. Returns [] if it doesn't exist yet."""
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
