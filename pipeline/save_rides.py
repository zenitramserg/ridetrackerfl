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
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Allow running as script from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.organizer import infer_organizer
from pipeline.classifier import rejection_reason, classify_ride
from pipeline.deduplicator import deduplicate
from pipeline.db import load_db, save_db

BASE_DIR = Path(__file__).parent.parent

# Env-overridable like the other pipeline paths: the container has no /app/data
# (the image copies only pipeline/ and config/), so a hardcoded path here crashes
# the run *after* the database has already been saved — the work is done, the
# report never arrives, and the exit code says total failure.
CSV_PATH = Path(os.environ.get("RIDETRACKER_CSV_PATH", BASE_DIR / "data" / "master_rides.csv"))

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
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
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
    existing = load_db()

    # Validate and enrich incoming records
    valid_rides = []
    rejections = []
    skipped_not_ride = 0
    for ride in raw_batch:
        if not ride.get("is_ride_post"):
            skipped_not_ride += 1
            continue  # Vision flagged as non-ride

        reason = rejection_reason(ride)
        if reason:
            # Named and explained. The source story expires within a day, so a
            # rejection that is only counted is a rejection nobody can review.
            label = ride.get("title") or "(untitled)"
            where = ride.get("source_account") or "?"
            shot = Path(ride.get("screenshot_path") or "").name or "?"
            conf = ride.get("confidence")
            print(f"[save_rides] ✗ Rejected: {label!r} (@{where}, {shot}, conf={conf}) — {reason}")
            rejections.append({"title": label, "source_account": where,
                               "screenshot": shot, "confidence": conf, "reason": reason})
            continue

        valid_rides.append(_enrich(ride, scan_timestamp, scan_date))

    # Deduplication + merge
    merged_db, added, updated = deduplicate(existing, valid_rides)

    summary = {
        "scan_timestamp": scan_timestamp,
        "batch_file": str(batch_path),
        "raw_extracted": len(raw_batch),
        "valid_rides": len(valid_rides),
        "rejected": len(rejections),
        "skipped_not_ride": skipped_not_ride,
        "rejections": rejections,
        "added_to_db": added,
        "updated_in_db": updated,
        "total_in_db": len(merged_db),
    }

    if not dry_run:
        save_db(merged_db)
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
