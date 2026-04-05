"""
airtable_writer.py
Write and update ride records in Airtable via MCP.

This module is called by save_rides.py after deduplication.
It maps ride records from rides_database.json to Airtable field IDs.

NOTE: Airtable field IDs are hardcoded here from RideTrackerFL_MAIN > Rides table.
If the table schema changes, update the FIELD_MAP below.
"""

# =============================================================================
# Airtable IDs — RideTrackerFL_MAIN > Rides
# =============================================================================
BASE_ID = "appp3CTtWpqVcTn6e"
TABLE_ID = "tbl7xURgDo5wU4z5t"

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
    # instagram_screenshot: attachment — scraper does NOT write this; add manually in Airtable
    "display_on_site":          "fld127xtSZgzoQZuU",  # Display on Site (checkbox, default unchecked=visible)
    "is_primary_listing":       "fldxlIlSHvv8SZGNx",  # Is Primary Listing (checkbox)
    "source":                   "fldTkJE7ScgZfjLgo",  # Source (singleSelect: Instagram Scraper, Organizer Email, etc.)
    "needs_review":             "fldIQPl9MMrKMlvWL",  # Needs Review (checkbox — flag for manual check)
    # Note: Instagram Screenshot (fldJH78crXSiHvsR) is attachment-only — scraper cannot write attachments via API
}

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
    # Note: display_on_site and is_primary_listing are intentionally NOT set by the scraper.
    # Leave them as unchecked (Airtable default) — set manually in Airtable UI.

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

def push_new_rides(dry_run: bool = False) -> int:
    """
    Push any rides in rides_database.json that don't yet have an airtable_record_id.

    Uses pyairtable for direct API access — no MCP needed.

    Requires:
        pip install pyairtable
        AIRTABLE_API_KEY environment variable (Personal Access Token from airtable.com/create/tokens)

    Returns the number of records pushed.
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

    # Only push rides that haven't been synced yet
    pending = [r for r in db if not r.get("airtable_record_id")]
    if not pending:
        print("[airtable] ✓ All rides already synced — nothing to push.")
        return 0

    print(f"[airtable] Pushing {len(pending)} new ride(s)...")

    if dry_run:
        for ride in pending:
            print(f"  [dry-run] Would push: {ride.get('title', '?')}")
        return 0

    api   = Api(api_key)
    table = api.table(BASE_ID, TABLE_ID)
    pushed = 0

    for ride in pending:
        fields = build_airtable_record(ride)
        try:
            record = table.create(fields)
            record_id = record.get("id")
            if not record_id:
                raise ValueError(f"Airtable response missing 'id' field: {record}")
            ride["airtable_record_id"] = record_id
            print(f"  ✓ Created {record_id}: {ride.get('title', '?')}")
            pushed += 1
        except Exception as e:
            print(f"  ✗ Failed to push '{ride.get('title', '?')}': {e}")

    # Save record IDs back to the local database
    if pushed:
        with open(db_path, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        print(f"[airtable] ✓ Pushed {pushed} record(s) and saved record IDs to DB.")

    return pushed
