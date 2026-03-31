"""
scripts/cleanup_airtable.py
Delete stale, duplicate, and placeholder records from the RideTrackerFL Rides table.

Run from your ridetrackerfl project directory:
    python3 scripts/cleanup_airtable.py

Requires:  pip install pyairtable  (already installed if you've run run_scan.py)
Requires:  AIRTABLE_API_KEY set in config/secrets.env  (already there)
"""

import json
import os
import sys
from pathlib import Path

# Load secrets from config/secrets.env
BASE_DIR    = Path(__file__).parent.parent
SECRETS_ENV = BASE_DIR / "config" / "secrets.env"

def load_env(path: Path):
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

load_env(SECRETS_ENV)

try:
    from pyairtable import Api
except ImportError:
    print("✗ pyairtable not installed.  Run:  pip install pyairtable")
    sys.exit(1)

API_KEY  = os.environ.get("AIRTABLE_API_KEY")
BASE_ID  = "appp3CTtWpqVcTn6e"
TABLE_ID = "tbl7xURgDo5wU4z5t"

if not API_KEY:
    print("✗ AIRTABLE_API_KEY not found in config/secrets.env")
    sys.exit(1)

# ── Records to delete ─────────────────────────────────────────────────────────
RECORDS_TO_DELETE = {
    # OMG Morning Ride rows with stale specific dates (Mon-Fri, past dates)
    # The Mon/Tue/Thu recurring records will be re-added cleanly without hardcoded dates.
    # Wed & Fri are replaced by "Wednesday Favorite Ride" and "Friday Recovery".
    "reca79S9MD8utrxoU": "OMG Morning Ride - Monday 2026-03-23 (stale date)",
    "recIHBWryQbpAqklR": "OMG Morning Ride - Tuesday 2026-03-24 (stale date)",
    "recxCJrb0YvAJSqCF": "OMG Morning Ride - Wednesday 2026-03-25 (stale date, replaced by Wednesday Favorite Ride)",
    "recPodjdXCLHd9c7y": "OMG Morning Ride - Thursday 2026-03-26 (stale date)",
    "recl0yl3zecl2p00l": "OMG Morning Ride - Friday 2026-03-27 (stale date, replaced by Friday Recovery)",

    # Vague "Local Group" / "Team Recovery" records with no real organizer
    "recEOFxJBOlDdISLY": "Weston Early Ride - Local Group Tue (vague organizer)",
    "recaaPy7YQ60yD80W": "Weston Early Ride - Local Group Thu (vague organizer)",
    "recJBSMm0jyZFdg6x": "Recovery Ride - Local Group Wed PM (vague organizer)",
    "recS1EpX7cJrloSCI": "Team Recovery Ride - TBD time (vague)",
    "recnsl5cxF2cmxkq4": "The Epic Ride - Local Group Sat (vague)",
    "recxYh4EATtJY2yzK": "Charlie's Ride - Local Group Sat (vague)",

    # Duplicate Alex's record (rec5pXok7XiOHyShs is the keeper — High confidence)
    "rec1qFGFm1sAhJSEi": "Alex's Bike Shop Ride - DUPLICATE of Alex's Saturday Group Ride",

    # No-data placeholder records (missing weekday AND time, Low confidence, unknown status)
    "recQNsL6nZtlMzRCH": "Ride 84 Group Ride - placeholder (no day or time)",
    "recmnYLcePmR5lnrD": "FP Bike Shop Group Ride - placeholder (no day or time)",
    "recrevBPEDDBKznzQ": "FP Bike Shop Ride - Saturday with TBD time",
    "recYkGjqgtdroxiRO": "Revolt Cyclery Group Ride - placeholder (no day or time)",
    "recv6E2QNQRfpCwHC": "Unicosta Group Ride - placeholder (no day or time)",
    "recntKYUDJgFcY0fM": "MBO Ten Cycling Ride - placeholder (no day or time)",
    "recod8ptU81PmOzwU": "PD Cycling Club Ride - placeholder (no day or time)",

    # Past one-time event — will be re-added to the new Events table
    "recqHZi0wma2BCyCN": "The Training Ride - Le Tour de Weston 2026 (past event, March 28)",
}

def main(dry_run: bool = False):
    api   = Api(API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    mode = "[DRY RUN] " if dry_run else ""
    print(f"{mode}Preparing to delete {len(RECORDS_TO_DELETE)} records...\n")

    deleted = 0
    failed  = 0

    for record_id, reason in RECORDS_TO_DELETE.items():
        if dry_run:
            print(f"  [dry-run] Would delete {record_id}: {reason}")
            deleted += 1
            continue
        try:
            table.delete(record_id)
            print(f"  ✓ Deleted {record_id}: {reason}")
            deleted += 1
        except Exception as e:
            print(f"  ✗ FAILED  {record_id}: {e}")
            failed += 1

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Done — {deleted} {'would be ' if dry_run else ''}deleted, {failed} failed.")

if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
