"""
sync_to_site.py
Pulls rides and organizers from Airtable and generates public/rides.json.

This replaces the live Airtable API calls that every site visitor was
triggering. The frontend reads the static JSON instead — zero API calls
from visitors, capped at 96 Airtable calls/day from the cron job.

Usage:
    python3.12 -m pipeline.sync_to_site          # generate + git push
    python3.12 -m pipeline.sync_to_site --dry-run # generate only, no push
    python3.12 -m pipeline.sync_to_site --no-push # generate, skip git push

Cron (every 15 min):
    */15 * * * * cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site >> logs/sync.log 2>&1
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

BASE_ID      = "appp3CTtWpqVcTn6e"
RIDES_TABLE  = "tbl7xURgDo5wU4z5t"
ORGS_TABLE   = "tblYXWvRSgqWkvaVE"

# Fields to export for rides — matches what index.html requests
RIDE_FIELDS = [
    "Ride Name", "Organizer", "Day of Week", "Time", "Location",
    "Address Note", "Distance", "Pace", "Status", "Weather Summary",
    "Rain Probability", "Wind Speed", "Ride Date", "Ride Type",
    "Featured", "Display on Site", "Is Primary Listing", "Needs Review",
    "Organizer Instagram Profile", "Rider Count", "Last Checked At",
    "Notes",
]

# Fields to export for organizers
ORG_FIELDS = [
    "Name", "Type", "Instagram Handle", "Instagram URL", "Website URL",
    "WhatsApp Link", "Strava URL", "Contact Email", "Avatar", "Active",
]

OUTPUT_PATH = Path(__file__).parent.parent / "public" / "rides.json"


# ── Airtable fetch ─────────────────────────────────────────────────────────────

def _fetch_table(token: str, table_id: str, fields: list[str],
                 filter_formula: str = None) -> list[dict]:
    """
    Fetch all records from an Airtable table, handling pagination.
    Returns list of { id, fields } dicts matching the Airtable REST format.
    """
    records = []
    offset  = None

    while True:
        params = {}
        for f in fields:
            params.setdefault("fields[]", [])
            if isinstance(params["fields[]"], list):
                params["fields[]"].append(f)

        # urllib.parse.urlencode doesn't handle repeated keys well — build manually
        qs_parts = [f"fields[]={urllib.parse.quote(f)}" for f in fields]
        if filter_formula:
            qs_parts.append(f"filterByFormula={urllib.parse.quote(filter_formula)}")
        if offset:
            qs_parts.append(f"offset={urllib.parse.quote(offset)}")

        url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}?{'&'.join(qs_parts)}"

        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8")
            raise RuntimeError(f"Airtable API error {e.code}: {body}")

        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return records


# ── Generate JSON ──────────────────────────────────────────────────────────────

def generate_rides_json(token: str) -> dict:
    """
    Pull rides (display_on_site only) and all active organizers from Airtable.
    Returns the combined payload that public/rides.json will contain.
    """
    print("[sync] Fetching rides from Airtable...")
    # Only export rides that should show on site
    rides = _fetch_table(
        token, RIDES_TABLE, RIDE_FIELDS,
        filter_formula="{Display on Site}=TRUE()"
    )
    print(f"[sync] {len(rides)} rides fetched.")

    print("[sync] Fetching organizers from Airtable...")
    organizers = _fetch_table(
        token, ORGS_TABLE, ORG_FIELDS,
        filter_formula="{Active}=TRUE()"
    )
    print(f"[sync] {len(organizers)} organizers fetched.")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rides": {"records": rides},
        "organizers": {"records": organizers},
    }
    return payload


# ── Write file ─────────────────────────────────────────────────────────────────

def write_json(payload: dict) -> Path:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"[sync] Written to {OUTPUT_PATH} ({size_kb:.1f} KB)")
    return OUTPUT_PATH


# ── Git push ───────────────────────────────────────────────────────────────────

def git_push(repo_root: Path) -> bool:
    """
    Stage rides.json, commit if changed, push.
    Returns True if a push was made, False if nothing changed.
    """
    try:
        # Check if file actually changed
        result = subprocess.run(
            ["git", "diff", "--quiet", "public/rides.json"],
            cwd=repo_root, capture_output=True
        )
        if result.returncode == 0:
            # Also check if it's untracked (new file)
            status = subprocess.run(
                ["git", "status", "--porcelain", "public/rides.json"],
                cwd=repo_root, capture_output=True, text=True
            )
            if not status.stdout.strip():
                print("[sync] No changes detected — skipping git push.")
                return False

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(["git", "add", "public/rides.json"], cwd=repo_root, check=True)
        subprocess.run(
            ["git", "commit", "-m", f"chore: sync rides.json [{ts}]"],
            cwd=repo_root, check=True, capture_output=True
        )
        subprocess.run(["git", "push"], cwd=repo_root, check=True, capture_output=True)
        print(f"[sync] Pushed to GitHub at {ts}.")
        return True

    except subprocess.CalledProcessError as e:
        print(f"[sync] ⚠ Git error: {e}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Airtable → public/rides.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate JSON locally but do not push to GitHub")
    parser.add_argument("--no-push", action="store_true",
                        help="Write file but skip git push")
    args = parser.parse_args()

    # Load API key from secrets.env or environment
    token = os.environ.get("AIRTABLE_API_KEY")
    if not token:
        secrets_path = Path(__file__).parent.parent / "config" / "secrets.env"
        if secrets_path.exists():
            for line in secrets_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("AIRTABLE_API_KEY="):
                    token = line.split("=", 1)[1].strip()
                    break

    if not token:
        print("[sync] ✗ AIRTABLE_API_KEY not found in environment or config/secrets.env")
        sys.exit(1)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[sync] Starting sync — {ts}")
    print("=" * 56)

    try:
        payload = generate_rides_json(token)
    except RuntimeError as e:
        print(f"[sync] ✗ Failed to fetch from Airtable: {e}")
        print("[sync] ⚠ Keeping existing rides.json — site remains live.")
        sys.exit(1)

    if args.dry_run:
        print("[sync] Dry run — previewing output (not writing file):")
        print(f"  rides:      {len(payload['rides']['records'])}")
        print(f"  organizers: {len(payload['organizers']['records'])}")
        print(f"  generated:  {payload['generated_at']}")
        return

    write_json(payload)

    if args.no_push:
        print("[sync] --no-push set — skipping git push.")
        return

    repo_root = Path(__file__).parent.parent
    git_push(repo_root)

    print("=" * 56)
    print(f"[sync] Done — {len(payload['rides']['records'])} rides, "
          f"{len(payload['organizers']['records'])} organizers.")


if __name__ == "__main__":
    main()
