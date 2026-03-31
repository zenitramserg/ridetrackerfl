"""
save_rides.py
Entry point for the data pipeline.

Usage:
    python -m pipeline.save_rides --input /path/to/batch.json [--dry-run]

Reads a batch of vision-extracted ride JSONs, runs validation, dedup,
organizer inference, classification, and saves to rides_database.json + master_rides.csv.
"""

import argparse
import csv
import json
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Allow running as script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.organizer import infer_organizer
from pipeline.classifier import is_valid_group_ride, classify_ride, is_promo_post
from pipeline.deduplicator import deduplicate

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "rides_database.json"
CSV_PATH = BASE_DIR / "data" / "master_rides.csv"

CSV_FIELDS = [
    "ride_id",
    "source_account",
    "source_accounts",
    "organized_by",
    "organized_by_type",
    "organized_by_confidence",
    "item_type",
    "title",
    "weekday",
    "date",
    "start_time",
    "distance",
    "pace",
    "start_location",
    "address_note",
    "confidence",
    "story_id",
    "first_seen",
    "last_updated",
    "raw_visible_text",
]

WEEKDAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def _resolve_date(ride: dict, scan_date: datetime) -> str:
    """Resolve a weekday-only date to the next occurrence from scan_date."""
    if ride.get("date"):
        return ride["date"]

    weekday = (ride.get("weekday") or "").lower().strip()
    if weekday in WEEKDAY_MAP:
        target = WEEKDAY_MAP[weekday]
        current = scan_date.weekday()
        delta = (target - current) % 7
        resolved = scan_date + timedelta(days=delta)
        return resolved.strftime("%A %B %d, %Y")

    return ""


def _enrich(ride: dict, scan_timestamp: str, scan_date: datetime) -> dict:
    """Add pipeline-derived fields to a vision-extracted ride record."""
    # Organizer inference
    org, org_type, org_conf = infer_organizer(
        ride.get("raw_visible_text", ""),
        ride.get("source_account", "")
    )
    ride["organized_by"] = org
    ride["organized_by_type"] = org_type
    ride["organized_by_confidence"] = org_conf

    # Classification
    ride["item_type"] = classify_ride(ride)

    # Date resolution
    ride["date"] = _resolve_date(ride, scan_date)

    # Ensure source_accounts list
    if not ride.get("source_accounts"):
        ride["source_accounts"] = [ride.get("source_account", "")]

    # Timestamps
    ride.setdefault("ride_id", str(uuid.uuid4()))
    ride.setdefault("first_seen", scan_timestamp)
    ride["last_updated"] = scan_timestamp

    return ride


def _write_csv(rides: list):
    """Regenerate master_rides.csv from the full database."""
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for ride in rides:
            row = dict(ride)
            # Flatten list fields for CSV
            if isinstance(row.get("source_accounts"), list):
                row["source_accounts"] = "|".join(row["source_accounts"])
            writer.writerow(row)


def process_batch(batch_path: Path, dry_run: bool = False) -> dict:
    """
    Load a batch of raw vision extractions, validate, enrich, dedup, and save.
    Returns a summary dict.
    """
    with open(batch_path, "r", encoding="utf-8") as f:
        raw_batch = json.load(f)

    scan_timestamp = datetime.now().isoformat(timespec="seconds")
    scan_date = datetime.now()

    # Load existing database
    if DB_PATH.exists():
        with open(DB_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    # Validate and enrich incoming records
    valid_rides = []
    rejected = 0
    for ride in raw_batch:
        if not ride.get("is_ride_post"):
            continue  # Vision flagged as non-ride
        if is_promo_post(ride.get("raw_visible_text", "")):
            rejected += 1
            continue  # Shop promo — excluded per project rules
        if not is_valid_group_ride(ride):
            rejected += 1
            continue
        valid_rides.append(_enrich(ride, scan_timestamp, scan_date))

    # Deduplication + merge
    merged_db, added, updated = deduplicate(existing, valid_rides)

    summary = {
        "scan_timestamp": scan_timestamp,
        "batch_file": str(batch_path),
        "raw_extracted": len(raw_batch),
        "valid_rides": len(valid_rides),
        "rejected": rejected,
        "added_to_db": added,
        "updated_in_db": updated,
        "total_in_db": len(merged_db),
    }

    if not dry_run:
        with open(DB_PATH, "w", encoding="utf-8") as f:
            json.dump(merged_db, f, indent=2, ensure_ascii=False)
        _write_csv(merged_db)
        print(f"[save_rides] Saved {added} new, {updated} updated → {len(merged_db)} total rides in DB")
    else:
        print(f"[save_rides] DRY RUN — would add {added}, update {updated}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="RideTrackerFL pipeline: validate, dedup, and save rides.")
    parser.add_argument("--input", required=True, help="Path to batch JSON file from scraper")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing to DB")
    args = parser.parse_args()

    batch_path = Path(args.input)
    if not batch_path.exists():
        print(f"[save_rides] ERROR: batch file not found: {batch_path}")
        sys.exit(1)

    summary = process_batch(batch_path, dry_run=args.dry_run)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
