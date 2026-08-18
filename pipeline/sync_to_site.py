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

OUTPUT_PATH    = Path(__file__).parent.parent / "public" / "rides.json"
AVATARS_PATH   = Path(__file__).parent.parent / "public" / "organizers"


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


# ── Avatar caching ─────────────────────────────────────────────────────────────

def _slug(name: str) -> str:
    """Convert organizer name to a safe filename slug."""
    import re
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _find_cached_avatar(slug: str) -> str | None:
    """
    Look for a previously downloaded avatar for this slug, regardless of
    extension. Used as a fallback when a fresh download fails, so a
    transient network error doesn't leave Airtable's ephemeral attachment
    URL in rides.json (that URL expires in a few hours and breaks the logo).
    """
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if (AVATARS_PATH / f"{slug}{ext}").exists():
            return f"/public/organizers/{slug}{ext}"
    return None


def _sync_avatar(attachment: dict, slug: str) -> str | None:
    """
    Download organizer avatar from Airtable and save to public/organizers/.
    Skips download if local file already exists with the same size.
    Returns the local web path (e.g. /public/organizers/omg_cycling.png)
    or None if download failed.
    """
    AVATARS_PATH.mkdir(parents=True, exist_ok=True)

    url      = attachment.get("url", "")
    filename = attachment.get("filename", "")
    size     = attachment.get("size", 0)

    if not url:
        return None

    # Determine extension from filename or default to jpg
    ext = Path(filename).suffix.lower() if filename else ".jpg"
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        ext = ".jpg"

    local_path = AVATARS_PATH / f"{slug}{ext}"

    # Skip if file exists and size matches
    if local_path.exists() and local_path.stat().st_size == size:
        return f"/public/organizers/{slug}{ext}"

    # Download
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RideTrackerFL/2.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
        local_path.write_bytes(data)
        action = "Updated" if local_path.exists() else "Downloaded"
        print(f"[sync] {action} avatar: {local_path.name}")
        return f"/public/organizers/{slug}{ext}"
    except Exception as e:
        print(f"[sync] ⚠ Could not download avatar for {slug}: {e}")
        return None


# ── Weather refresh ───────────────────────────────────────────────────────────

# Airtable field IDs for weather fields
_WEATHER_FIELD_SUMMARY = "fldIAFoFLCfytdOJV"  # Weather Summary
_WEATHER_FIELD_RAIN    = "fld6lsm9KVE6Ujwcm"  # Rain Probability
_WEATHER_FIELD_WIND    = "fld4nwFJFwrjveMzX"  # Wind Speed


def _refresh_weather(token: str, rides: list[dict]) -> None:
    """
    For any ride within the next 7 days, fetch fresh weather from Open-Meteo
    and write it back to Airtable if the stored value is empty or has changed.
    Updates the ride dicts in-place so rides.json also gets fresh values.
    """
    from datetime import date, timedelta
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from weather import get_ride_weather

    today     = date.today()
    cutoff    = today + timedelta(days=7)
    updated   = 0
    errors    = 0

    for rec in rides:
        fields   = rec.get("fields", {})
        ride_date_str = fields.get("Ride Date") or fields.get("Day of Week")
        ride_time_str = fields.get("Time", "06:00 AM")
        record_id     = rec.get("id")

        if not ride_date_str or not record_id:
            continue

        # Parse ride date
        try:
            from datetime import datetime as _dt
            if len(ride_date_str) == 10 and ride_date_str[4] == "-":
                ride_date = _dt.strptime(ride_date_str, "%Y-%m-%d").date()
            else:
                continue  # weekday-only records handled by frontend
        except Exception:
            continue

        if not (today <= ride_date <= cutoff):
            continue

        # Fetch fresh weather
        try:
            wx = get_ride_weather(ride_date_str, ride_time_str or "06:00 AM")
        except Exception as e:
            errors += 1
            continue

        if not wx or wx.get("rain_probability") is None:
            continue

        new_summary = wx.get("weather_summary", "")
        new_rain    = (wx["rain_probability"] or 0) / 100  # Airtable stores as 0.0–1.0
        new_wind    = wx.get("wind_speed")

        # Check if values differ from stored (skip unnecessary API calls)
        stored_rain = fields.get("Rain Probability")
        if stored_rain is not None and abs(stored_rain - new_rain) < 0.02 and fields.get("Weather Summary"):
            continue  # Close enough — skip update

        # Write back to Airtable
        patch = {
            _WEATHER_FIELD_SUMMARY: new_summary,
            _WEATHER_FIELD_RAIN:    new_rain,
        }
        if new_wind is not None:
            patch[_WEATHER_FIELD_WIND] = new_wind

        try:
            url = f"https://api.airtable.com/v0/{BASE_ID}/{RIDES_TABLE}/{record_id}"
            body = json.dumps({"fields": patch}).encode("utf-8")
            req  = urllib.request.Request(
                url, data=body, method="PATCH",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=15):
                pass
            # Update in-place so rides.json reflects fresh values too
            fields["Weather Summary"]   = new_summary
            fields["Rain Probability"]  = new_rain
            if new_wind is not None:
                fields["Wind Speed"] = new_wind
            updated += 1
        except Exception as e:
            errors += 1

    msg = f"[sync] Weather refreshed: {updated} ride(s) updated."
    if errors:
        msg += f" ({errors} skipped/failed)"
    print(msg)


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

    # Refresh weather for upcoming rides that are missing or have stale data
    _refresh_weather(token, rides)

    # Download avatars and replace Airtable URLs with permanent local paths
    print("[sync] Syncing organizer avatars...")
    downloaded = 0
    for rec in organizers:
        fields    = rec.get("fields", {})
        name      = fields.get("Name", "")
        avatars   = fields.get("Avatar", [])
        if not name or not avatars:
            continue
        slug      = _slug(name)
        local_url = _sync_avatar(avatars[0], slug)
        if not local_url:
            # Fresh download failed — fall back to a previously cached local
            # copy instead of leaving Airtable's ephemeral attachment URL
            # (which expires in a few hours) in rides.json.
            local_url = _find_cached_avatar(slug)
            if local_url:
                print(f"[sync] ⚠ Using cached avatar for {slug} (fresh download failed)")
        if local_url:
            # Replace the Airtable attachment array with a simple local URL string
            # The frontend checks: org['Avatar'][0].url — we keep the same shape
            fields["Avatar"] = [{"url": local_url, "filename": Path(local_url).name}]
            downloaded += 1
    print(f"[sync] Avatars synced: {downloaded}/{len(organizers)}.")

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
    # Clear any stale git lock files left by interrupted processes on mounted filesystems
    for lock_file in ["index.lock", "HEAD.lock", "COMMIT_EDITMSG.lock"]:
        lock_path = repo_root / ".git" / lock_file
        if lock_path.exists():
            try:
                lock_path.unlink()
                print(f"[sync] Cleared stale lock: {lock_file}")
            except OSError:
                pass  # If we can't remove it, the commit will fail and report the real error

    try:
        # Check if file actually changed
        result = subprocess.run(
            ["git", "diff", "--quiet", "public/rides.json", "public/organizers/"],
            cwd=repo_root, capture_output=True
        )
        if result.returncode == 0:
            # Also check for untracked files (new avatars or new rides.json)
            status = subprocess.run(
                ["git", "status", "--porcelain", "public/rides.json", "public/organizers/"],
                cwd=repo_root, capture_output=True, text=True
            )
            if not status.stdout.strip():
                print("[sync] No changes detected — skipping git push.")
                return False

        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(["git", "add", "public/rides.json", "public/organizers/"], cwd=repo_root, check=True)
        result = subprocess.run(
            ["git", "commit", "-m", f"chore: sync rides.json [{ts}]"],
            cwd=repo_root, check=True, capture_output=True, text=True
        )
        subprocess.run(["git", "push"], cwd=repo_root, check=True, capture_output=True)
        print(f"[sync] Pushed to GitHub at {ts}.")
        return True

    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip() if e.stderr else "(no stderr)"
        print(f"[sync] ⚠ Git error: {e} — {stderr}")
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
