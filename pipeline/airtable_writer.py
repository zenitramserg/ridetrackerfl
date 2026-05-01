"""
airtable_writer.py
Write and update ride records in Airtable via MCP.

This module is called by save_rides.py after deduplication.
It maps ride records from rides_database.json to Airtable field IDs.

NOTE: Airtable field IDs are hardcoded here from RideTrackerFL_MAIN > Rides table.
If the table schema changes, update the FIELD_MAP below.
"""

import json
from pathlib import Path

# =============================================================================
# Airtable IDs — RideTrackerFL_MAIN > Rides
# =============================================================================
BASE_ID = "appp3CTtWpqVcTn6e"
TABLE_ID = "tbl7xURgDo5wU4z5t"
HISTORY_TABLE_ID = "tblmjfqmP5swqgKYY"  # Ride History table

# Ride History field IDs
HISTORY_FIELD_MAP = {
    "entry":           "fldT0LLRd0XpKKQGX",  # Entry (primary — auto label)
    "organizer":       "fldhHOxDAATvyzkQ2",  # Organizer
    "ride_date":       "fldHD1dm4sJ8H9MRK",  # Ride Date
    "day_of_week":     "fldp2u6clV5kYwhQR",  # Day of Week (singleSelect)
    "time":            "fldkZTxJdn6GyuQea",  # Time
    "confirmed_via":   "fldqY09nJrLe7p97c",  # Confirmed Via (singleSelect)
    "source_accounts": "fldGWzkssCRlmQUgk",  # Source Accounts
    "confidence":      "fldkbNcNqngYBmfJG",  # Confidence (singleSelect)
    "rides_record_id": "fld5f0fviCFkO1Bhv",  # Rides Record ID (links to Rides table)
    "detected_at":     "fldNh4jdci6KYEd1C",  # Detected At (dateTime)
    "notes":           "fld7IVRnTDQu2md4M",  # Notes
}

# Maps our internal ride field names → Airtable field IDs
FIELD_MAP = {
    "title":                    "fldcGLkoF6JsLndQb",  # Ride Name (primary)
    "organized_by":             "fldorJA4k2hXbXvss",  # Organizer
    "source_accounts":          "fld0uaV5g7uFaC7q4",  # Source Accounts
    "weekday":                  "flddKtPFNDwQ7GcmL",  # Day of Week (singleSelect)
    "start_time":               "fld2iNs1FmYZUjk8Y",  # Time
    "start_location":           "fldBVWgxU0hEDEdBm",  # Location
    "is_recurring":             "fldiMJC4hE7DmX7eo",  # Recurring (checkbox)
    "date":                     "fldgXJ5AXBiYbuMS6",  # Ride Date
    "status":                   "fldfoh4Ljmw8qnE9F",  # Status (singleSelect)
    "last_verified_at":         "fldGC5EMk0nqqYOWJ",  # Last Checked At
    "confidence_label":         "fldyKd4OtwlSSF9V4",  # Confidence (singleSelect)
    "notes":                    "fldw6QZt3cFL2wub2",  # Notes
    "organizer_instagram":      "fldBkXFCLsIzHNE0R",  # Organizer Instagram Profile
    "organized_by_type":        "fld0CgaMzu0e8RNEQ",  # Organizer Type (singleSelect)
    "weather_summary":          "fldIAFoFLCfytdOJV",  # Weather Summary
    "rain_probability":         "fld6lsm9KVE6Ujwcm",  # Rain Probability
    "wind_speed":               "fld4nwFJFwrjveMzX",  # Wind Speed
    "distance":                 "fldUmltgAReTEclC5",  # Distance
    "pace":                     "fldQBVNhysTgg25PB",  # Pace
    "address_note":             "fldhQe3sjuF97T9RX",  # Address Note
    "ride_type":                "fld06v2VSWHtfKuKT",  # Ride Type (singleSelect): weekly, special_event, annual, Regular, Special Event, Featured Event
    # ── Manual control fields (added 2026-04-04) ─────────────────────────────
    # display_on_site:  checkbox — uncheck to hide from website without deleting record
    # is_primary_listing: checkbox — mark the canonical record when duplicates exist
    # source:           singleSelect — "Instagram Scraper" when written by this pipeline
    # needs_review:     checkbox — flag for Sergio to review manually (e.g. new ride, conflict)
    "display_on_site":          "fld127xtSZgzoQZuU",  # Display on Site (checkbox, default unchecked=visible)
    "is_primary_listing":       "fldxlIlSHvv8SZGNx",  # Is Primary Listing (checkbox)
    "source":                   "fldTkJE7ScgZfjLgo",  # Source (singleSelect: Instagram Scraper, Organizer Email, etc.)
    "needs_review":             "fldIQPl9MMrKMlvWL",  # Needs Review (checkbox — flag for manual check)
}

# Instagram Screenshot attachment field — uploaded separately after record creation
SCREENSHOT_FIELD_ID = "fldJH78crXSiHvsR"

# Account handle → Instagram profile URL
INSTAGRAM_URLS = {
    "omg_cycling":       "https://www.instagram.com/omg_cycling/",
    "alexbicycles":      "https://www.instagram.com/alexbicycles/",
    "ride.84":           "https://www.instagram.com/ride.84/",
    "revoltcyclery":     "https://www.instagram.com/revoltcyclery/",
    "fpbikeshop":        "https://www.instagram.com/fpbikeshop/",
    "unicosta_cycling":  "https://www.instagram.com/unicosta_cycling/",
    "mbo_tencycling":    "https://www.instagram.com/mbo_tencycling/",
    "pd.cyclingclub":    "https://www.instagram.com/pd.cyclingclub/",
    "letourdeweston":    "https://www.instagram.com/letourdeweston/",
}

# Confidence float → Airtable label
def _upload_screenshot(api_key: str, record_id: str, screenshot_path: str) -> bool:
    """
    Upload a local screenshot file as an attachment to the Instagram Screenshot field.

    Uses Airtable's upload attachment endpoint:
        POST /v0/{baseId}/{tableId}/{recordId}/{fieldId}/uploadAttachment

    The file is base64-encoded and sent as JSON.
    Returns True on success, False on failure.
    """
    import base64
    import urllib.request
    import urllib.error

    path = Path(screenshot_path)
    if not path.exists():
        print(f"  ⚠ Screenshot not found, skipping upload: {path.name}")
        return False

    try:
        content_b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        payload = json.dumps({
            "contentType": "image/png",
            "filename":    path.name,
            "file":        content_b64,
        }).encode("utf-8")

        url = (
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
            f"/{record_id}/{SCREENSHOT_FIELD_ID}/uploadAttachment"
        )
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization":  f"Bearer {api_key}",
                "Content-Type":   "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
        print(f"  📎 Screenshot uploaded: {path.name}")
        return True

    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  ✗ Screenshot upload failed ({e.code}): {body[:200]}")
        return False
    except Exception as e:
        print(f"  ✗ Screenshot upload error: {e}")
        return False


def _confidence_label(conf: float) -> str:
    if conf is None:
        return "Low"
    c = float(conf)
    if c >= 0.85:
        return "High"
    if c >= 0.60:
        return "Medium"
    return "Low"


def _parse_date_for_airtable(date_str: str) -> str:
    """Convert 'Friday March 28, 2026' → '2026-03-28' for Airtable date fields."""
    from datetime import datetime
    if not date_str:
        return ""
    formats = ["%A %B %d, %Y", "%A %B %d %Y", "%B %d, %Y", "%Y-%m-%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""


def build_airtable_record(ride: dict) -> dict:
    """
    Convert a ride dict (from rides_database.json) to an Airtable fields dict.
    Only includes fields that have a known field ID and non-empty values.
    """
    fields = {}

    # Ride Name (primary field — always set)
    fields[FIELD_MAP["title"]] = ride.get("title") or ride.get("ride_name") or "Untitled Ride"

    # Organizer
    if ride.get("organized_by"):
        fields[FIELD_MAP["organized_by"]] = ride["organized_by"]

    # Organizer Type (singleSelect)
    if ride.get("organized_by_type") in ("group", "shop"):
        fields[FIELD_MAP["organized_by_type"]] = ride["organized_by_type"]

    # Source Accounts (pipe-separated string)
    sources = ride.get("source_accounts") or [ride.get("source_account", "")]
    if sources:
        fields[FIELD_MAP["source_accounts"]] = "|".join(s for s in sources if s)

    # Day of Week (singleSelect — must match option name exactly)
    weekday = ride.get("weekday", "")
    if weekday:
        fields[FIELD_MAP["weekday"]] = weekday.capitalize()

    # Time
    if ride.get("start_time"):
        fields[FIELD_MAP["start_time"]] = ride["start_time"]

    # Location
    if ride.get("start_location"):
        fields[FIELD_MAP["start_location"]] = ride["start_location"]

    # Recurring checkbox — weekly rides are recurring
    ride_type = ride.get("ride_type") or ride.get("item_type", "")
    fields[FIELD_MAP["is_recurring"]] = ride_type in ("weekly", "weekly_ride")

    # Ride Date (ISO format for Airtable)
    date_iso = _parse_date_for_airtable(ride.get("date", ""))
    if date_iso:
        fields[FIELD_MAP["date"]] = date_iso

    # Status (singleSelect)
    # Valid values: planned, confirmed, canceled, unknown, hidden, past
    # Scraper only sets planned/confirmed/canceled/unknown — hidden & past are set manually
    status = ride.get("status", "planned")
    if status in ("planned", "confirmed", "canceled", "unknown", "hidden", "past"):
        fields[FIELD_MAP["status"]] = status

    # Last Checked At (ISO datetime)
    if ride.get("last_verified_at") or ride.get("last_updated"):
        fields[FIELD_MAP["last_verified_at"]] = ride.get("last_verified_at") or ride.get("last_updated")

    # Confidence (singleSelect label)
    fields[FIELD_MAP["confidence_label"]] = _confidence_label(ride.get("confidence"))

    # Notes — combine address_note and image_description if present
    notes_parts = []
    if ride.get("address_note"):
        notes_parts.append(f"Address: {ride['address_note']}")
    if ride.get("pace"):
        notes_parts.append(f"Pace: {ride['pace']}")
    if ride.get("distance"):
        notes_parts.append(f"Distance: {ride['distance']}")
    if ride.get("image_description"):
        notes_parts.append(f"---\n{ride['image_description']}")
    if notes_parts:
        fields[FIELD_MAP["notes"]] = "\n".join(notes_parts)

    # Organizer Instagram URL
    primary_account = ride.get("source_account", "")
    if primary_account in INSTAGRAM_URLS:
        fields[FIELD_MAP["organizer_instagram"]] = INSTAGRAM_URLS[primary_account]

    # ── Manual control fields ──────────────────────────────────────────────────
    # Source: always "Instagram Scraper" when this pipeline creates/updates the record
    fields[FIELD_MAP["source"]] = "Instagram Scraper"

    # Needs Review: set True if confidence is Low, or if this is a new/unknown ride
    confidence = float(ride.get("confidence") or 0)
    if confidence < 0.60 or ride.get("needs_review"):
        fields[FIELD_MAP["needs_review"]] = True
    # Note: display_on_site is set to True only for NEW records (in push_new_rides).
    # is_primary_listing is intentionally never set by the scraper — set manually in Airtable UI.

    # ── Weather fields ─────────────────────────────────────────────────────────
    # Weather fields
    if ride.get("weather_summary"):
        fields[FIELD_MAP["weather_summary"]] = ride["weather_summary"]
    if ride.get("rain_probability") is not None:
        fields[FIELD_MAP["rain_probability"]] = ride["rain_probability"] / 100  # Airtable percent = 0.0–1.0
    if ride.get("wind_speed") is not None:
        fields[FIELD_MAP["wind_speed"]] = ride["wind_speed"]

    # Remove any None-keyed fields (unmapped)
    fields = {k: v for k, v in fields.items() if k is not None}

    return fields


# =============================================================================
# Direct Airtable push via pyairtable (used by run_scan.py for automated runs)
# =============================================================================

# Fields safe to overwrite on an existing (matched) Airtable record.
# Never touch: title, organized_by, start_location, weekday, start_time,
#              display_on_site, is_primary_listing — those are canonical/manual.
_SAFE_UPDATE_KEYS = [
    "date", "status", "source_accounts", "last_verified_at",
    "weather_summary", "rain_probability", "wind_speed",
    "confidence_label", "source", "needs_review",
]


def _ride_match_key(ride: dict) -> tuple:
    """Normalized lookup key: (organizer, weekday, time)."""
    org     = (ride.get("organized_by") or "").lower().strip()
    weekday = (ride.get("weekday") or "").lower().strip()
    time_   = (ride.get("start_time") or "").lower().strip()
    return (org, weekday, time_)


def _build_airtable_index(table) -> dict:
    """
    Fetch all existing Rides records and build a lookup dict:
        (organizer, weekday, time) → airtable_record_id

    One API call total (pyairtable paginates automatically).
    Returns an empty dict on error — safe fallback means new rides are
    created as usual rather than silently dropped.
    """
    index = {}
    try:
        records = table.all(fields=[
            FIELD_MAP["organized_by"],
            FIELD_MAP["weekday"],
            FIELD_MAP["start_time"],
        ])
        for rec in records:
            f = rec.get("fields", {})
            org     = (f.get(FIELD_MAP["organized_by"]) or "").lower().strip()
            weekday = (f.get(FIELD_MAP["weekday"]) or "").lower().strip()
            time_   = (f.get(FIELD_MAP["start_time"]) or "").lower().strip()
            key = (org, weekday, time_)
            if all(key):
                index[key] = rec["id"]
        print(f"[airtable] Indexed {len(index)} existing Airtable record(s).")
    except Exception as e:
        print(f"[airtable] ⚠ Could not build Airtable index ({e}) — will create new records as fallback.")
    return index


def push_new_rides(dry_run: bool = False) -> int:
    """
    Push any rides in rides_database.json that don't yet have an airtable_record_id.

    Before creating, checks whether a matching record already exists in Airtable
    using a (organizer, weekday, time) key — catches recurring rides that would
    otherwise create a duplicate record each week.

    - Match found  → link the local DB ride to the existing record and PATCH
                     only safe week-to-week fields (date, weather, status, etc.).
                     Never overwrites title, location, display_on_site, etc.
    - No match     → create a new record with display_on_site=True.

    Uses pyairtable for direct API access — no MCP needed.

    Requires:
        pip install pyairtable
        AIRTABLE_API_KEY environment variable (Personal Access Token from airtable.com/create/tokens)

    Returns the number of NEW records created (linked records are not counted).
    """
    import os
    import json
    from pathlib import Path

    try:
        from pyairtable import Api
    except ImportError:
        print("[airtable] ✗ pyairtable not installed. Run:  pip install pyairtable")
        return 0

    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        print("[airtable] ✗ AIRTABLE_API_KEY not set. Add it to config/secrets.env")
        return 0

    db_path = Path(__file__).parent.parent / "data" / "rides_database.json"
    if not db_path.exists():
        print("[airtable] ✗ rides_database.json not found")
        return 0

    with open(db_path, encoding="utf-8") as f:
        db = json.load(f)

    # Only process rides that haven't been synced yet
    pending = [r for r in db if not r.get("airtable_record_id")]
    if not pending:
        print("[airtable] ✓ All rides already synced — nothing to push.")
        return 0

    print(f"[airtable] {len(pending)} ride(s) pending sync...")

    if dry_run:
        for ride in pending:
            print(f"  [dry-run] Would process: {ride.get('title', '?')}")
        return 0

    api   = Api(api_key)
    table = api.table(BASE_ID, TABLE_ID)

    # Build index of all existing Airtable records — 1 API call
    airtable_index = _build_airtable_index(table)

    created = 0
    linked  = 0

    for ride in pending:
        match_key = _ride_match_key(ride)
        existing_record_id = airtable_index.get(match_key) if all(match_key) else None

        if existing_record_id:
            # ── Match found: link + patch safe fields ─────────────────────────
            ride["airtable_record_id"] = existing_record_id

            all_fields = build_airtable_record(ride)
            patch_fields = {
                FIELD_MAP[key]: all_fields[FIELD_MAP[key]]
                for key in _SAFE_UPDATE_KEYS
                if key in FIELD_MAP
                and FIELD_MAP[key] in all_fields
                and all_fields[FIELD_MAP[key]] is not None
            }

            try:
                if patch_fields:
                    table.update(existing_record_id, patch_fields)
                print(f"  ↻ Linked {existing_record_id}: {ride.get('title', '?')} (existing record updated)")
                linked += 1
            except Exception as e:
                print(f"  ✗ Failed to patch '{ride.get('title', '?')}' ({existing_record_id}): {e}")
                ride.pop("airtable_record_id", None)  # Don't save a bad link

        else:
            # ── No match: create new record ───────────────────────────────────
            fields = build_airtable_record(ride)
            # New records are visible on the website by default.
            # Uncheck manually in Airtable to hide a ride without deleting it.
            fields[FIELD_MAP["display_on_site"]] = True
            try:
                record = table.create(fields)
                record_id = record.get("id")
                if not record_id:
                    raise ValueError(f"Airtable response missing 'id' field: {record}")
                ride["airtable_record_id"] = record_id
                # Add to index so later rides in the same batch don't create a second duplicate
                if all(match_key):
                    airtable_index[match_key] = record_id
                print(f"  ✓ Created {record_id}: {ride.get('title', '?')}")
                created += 1

                # Upload screenshot attachment if available
                screenshot_path = ride.get("screenshot_path", "")
                if screenshot_path:
                    _upload_screenshot(api_key, record_id, screenshot_path)

            except Exception as e:
                print(f"  ✗ Failed to create '{ride.get('title', '?')}': {e}")

    # Save record IDs back to the local database (both new + linked)
    resolved = created + linked
    if resolved:
        with open(db_path, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        print(f"[airtable] ✓ Done — {created} created, {linked} linked to existing records.")

    return created


def push_updated_rides(updated_ids: list[str] | None = None, dry_run: bool = False) -> int:
    """
    Sync updated ride fields back to existing Airtable records.

    Called after deduplication when existing rides have new data (e.g. a recurring
    ride was re-detected this week with a new date/status/source_accounts).

    Only updates fields that change week-to-week:
        date, status, source_accounts, last_verified_at, weather, confidence.
    Never touches display_on_site or is_primary_listing (manual-only fields).

    Args:
        updated_ids: list of local DB ride IDs (ride["id"]) to sync.
                     If None, syncs all rides that have an airtable_record_id
                     and a date updated within the last 24 hours.
        dry_run: if True, print what would be updated without calling Airtable.
    """
    import os
    from datetime import datetime, timezone, timedelta

    try:
        from pyairtable import Api
    except ImportError:
        print("[airtable] ✗ pyairtable not installed.")
        return 0

    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        print("[airtable] ✗ AIRTABLE_API_KEY not set.")
        return 0

    db_path = Path(__file__).parent.parent / "data" / "rides_database.json"
    if not db_path.exists():
        return 0

    with open(db_path, encoding="utf-8") as f:
        db = json.load(f)

    # Fields that are safe to overwrite on existing records
    UPDATABLE_FIELDS = [
        "date", "status", "source_accounts", "last_verified_at",
        "weather_summary", "rain_probability", "wind_speed", "confidence_label",
        "source", "needs_review",
    ]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    candidates = [
        r for r in db
        if r.get("airtable_record_id")
        and (
            updated_ids is None
            or r.get("id") in updated_ids
        )
    ]

    # When no explicit list, filter to recently-updated rides only
    if updated_ids is None:
        def _recently_updated(r):
            ts = r.get("last_updated") or r.get("last_verified_at", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                # Handle both naive and timezone-aware datetimes
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt >= cutoff
            except Exception:
                return False
        candidates = [r for r in candidates if _recently_updated(r)]

    if not candidates:
        print("[airtable] ✓ No rides need updating.")
        return 0

    print(f"[airtable] Updating {len(candidates)} existing ride(s)...")

    api   = Api(api_key)
    table = api.table(BASE_ID, TABLE_ID)
    updated = 0

    for ride in candidates:
        record_id = ride["airtable_record_id"]
        all_fields = build_airtable_record(ride)

        # Only send the fields that are safe to overwrite
        update_fields = {
            FIELD_MAP[key]: all_fields[FIELD_MAP[key]]
            for key in UPDATABLE_FIELDS
            if key in FIELD_MAP
            and FIELD_MAP[key] in all_fields
            and all_fields[FIELD_MAP[key]] is not None
        }

        if not update_fields:
            continue

        if dry_run:
            date_val = ride.get("date", "?")
            print(f"  [dry-run] Would update {record_id}: {ride.get('title', '?')} → date={date_val}")
            continue

        try:
            table.update(record_id, update_fields)
            print(f"  ✓ Updated {record_id}: {ride.get('title', '?')} (date={ride.get('date', '?')})")
            updated += 1
        except Exception as e:
            print(f"  ✗ Failed to update {record_id} '{ride.get('title', '?')}': {e}")

    return updated


def push_ride_history(rides: list[dict] | None = None, dry_run: bool = False) -> int:
    """
    Log each detected ride occurrence to the Ride History table.

    One history record per ride per scan run. Captures:
    - Which ride it was (linked via Rides Record ID)
    - The specific date of this occurrence
    - How it was confirmed (Instagram for scraper-detected rides)
    - Which accounts posted it
    - Confidence level

    Args:
        rides: list of ride dicts from the current scan. If None, reads
               rides_database.json and logs all rides updated in the last 24h.
        dry_run: if True, print without writing to Airtable.
    """
    import os
    from datetime import datetime, timezone, timedelta

    try:
        from pyairtable import Api
    except ImportError:
        print("[airtable] ✗ pyairtable not installed.")
        return 0

    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        return 0

    if rides is None:
        db_path = Path(__file__).parent.parent / "data" / "rides_database.json"
        if not db_path.exists():
            return 0
        with open(db_path, encoding="utf-8") as f:
            db = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        def _recent(r):
            ts = r.get("last_updated") or r.get("last_verified_at", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                # Handle both naive and timezone-aware datetimes
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt >= cutoff
            except Exception:
                return False
        rides = [r for r in db if _recent(r)]

    if not rides:
        print("[airtable] ✓ No history entries to log.")
        return 0

    print(f"[airtable] Logging {len(rides)} ride history entry(s)...")

    api   = Api(api_key)
    htable = api.table(BASE_ID, HISTORY_TABLE_ID)
    logged = 0

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for ride in rides:
        # Build entry label: "OMG Cycling — Thursday 2026-04-15"
        organizer  = ride.get("organized_by", "Unknown")
        weekday    = (ride.get("weekday") or "").capitalize()
        date_iso   = _parse_date_for_airtable(ride.get("date", ""))
        entry_label = f"{organizer} — {weekday} {date_iso}" if date_iso else f"{organizer} — {weekday}"

        sources = ride.get("source_accounts") or [ride.get("source_account", "")]
        source_str = "|".join(s for s in sources if s)

        conf_label = _confidence_label(ride.get("confidence"))

        fields = {
            HISTORY_FIELD_MAP["entry"]:           entry_label,
            HISTORY_FIELD_MAP["organizer"]:       organizer,
            HISTORY_FIELD_MAP["time"]:            ride.get("start_time", ""),
            HISTORY_FIELD_MAP["confirmed_via"]:   "Instagram",
            HISTORY_FIELD_MAP["source_accounts"]: source_str,
            HISTORY_FIELD_MAP["confidence"]:      conf_label,
            HISTORY_FIELD_MAP["detected_at"]:     now_iso,
        }

        if date_iso:
            fields[HISTORY_FIELD_MAP["ride_date"]] = date_iso

        if weekday:
            fields[HISTORY_FIELD_MAP["day_of_week"]] = weekday

        if ride.get("airtable_record_id"):
            fields[HISTORY_FIELD_MAP["rides_record_id"]] = ride["airtable_record_id"]

        # Remove empty strings
        fields = {k: v for k, v in fields.items() if v}

        if dry_run:
            print(f"  [dry-run] Would log: {entry_label}")
            continue

        try:
            htable.create(fields)
            print(f"  ✓ History: {entry_label}")
            logged += 1
        except Exception as e:
            print(f"  ✗ History log failed for '{entry_label}': {e}")

    return logged
