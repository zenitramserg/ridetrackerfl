"""
deduplicator.py
Detect and merge duplicate ride records across accounts and story IDs.
Ported and adapted from weston-rides-monitor legacy project.
"""

import re
from typing import List, Dict, Tuple, Optional
from pipeline.organizer import pick_best_organizer


def _normalize_key(text: str) -> str:
    """Lowercase, strip extra spaces, remove punctuation for fuzzy matching."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_date(text: str) -> str:
    """
    Strip year suffixes so 'saturday march 28 2026' == 'saturday march 28'.
    Also strips ordinal suffixes: 'march 28th' → 'march 28'.
    """
    t = _normalize_key(text)
    t = re.sub(r"\b(st|nd|rd|th)\b", "", t)   # 28th → 28
    t = re.sub(r"\b20\d\d\b", "", t)           # strip 4-digit year
    return re.sub(r"\s+", " ", t).strip()


def _title_similarity(a: str, b: str) -> float:
    """
    Simple word-overlap ratio between two normalized titles.
    Returns 0.0–1.0. Used as a fallback match signal.
    """
    wa = set(_normalize_key(a).split())
    wb = set(_normalize_key(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _normalize_location(ride: dict) -> str:
    loc = _normalize_key(ride.get("start_location", ""))
    # Collapse known aliases
    aliases = {
        "alex": "alexs bicycle pro shop",
        "revolt": "revolt cyclery",
        "weston town center": "weston town center",
        "markham": "markham park",
    }
    for key, canonical in aliases.items():
        if key in loc:
            return canonical
    return loc


def ride_identity_key(ride: dict) -> Tuple[str, str, str]:
    """
    Unique identity: (normalized date OR weekday) + start_time + start_location.
    Date is year-stripped so 'Saturday March 28 2026' == 'Saturday March 28'.
    Used for both same-account and cross-account dedup.
    """
    date = _normalize_date(ride.get("date", "") or ride.get("weekday", ""))
    time = _normalize_key(ride.get("start_time", ""))
    location = _normalize_location(ride)
    return (date, time, location)


def _merge_rides(existing: dict, incoming: dict) -> dict:
    """
    Merge two ride records. Prefer non-empty, longer, or higher-confidence values.
    Combine source_accounts lists.
    """
    merged = dict(existing)

    # Merge scalar fields: prefer longer / non-empty value
    for field in ["title", "weekday", "date", "distance", "pace", "address_note",
                  "image_description", "raw_visible_text"]:
        old = (existing.get(field) or "").strip()
        new = (incoming.get(field) or "").strip()
        if len(new) > len(old):
            merged[field] = new

    # Prefer higher confidence float
    if float(incoming.get("confidence", 0)) > float(existing.get("confidence", 0)):
        merged["confidence"] = incoming["confidence"]

    # Keep earliest first_seen, latest last_updated
    merged["last_updated"] = incoming.get("last_updated", existing.get("last_updated", ""))

    # Merge source_accounts list
    existing_sources = existing.get("source_accounts", [existing.get("source_account", "")])
    incoming_sources = incoming.get("source_accounts", [incoming.get("source_account", "")])
    merged["source_accounts"] = sorted(set(existing_sources) | set(incoming_sources))

    # Pick best organizer
    org, org_type, org_conf = pick_best_organizer(existing, incoming)
    merged["organized_by"] = org
    merged["organized_by_type"] = org_type
    merged["organized_by_confidence"] = org_conf

    # Carry screenshot_path from incoming if existing doesn't have one
    if not merged.get("screenshot_path") and incoming.get("screenshot_path"):
        merged["screenshot_path"] = incoming["screenshot_path"]

    return merged


def _is_recurring(ride: dict) -> bool:
    """
    Returns True if this ride is a recurring weekly ride (not a special/one-time event).
    Used to decide whether week-over-week dedup matching applies.
    """
    ride_type = (ride.get("ride_type") or "").lower()
    if ride_type in ("special_event", "annual", "special event", "featured event"):
        return False
    return bool(ride.get("weekday"))


def _recurring_key(ride: dict) -> Tuple[str, str, str]:
    """
    Dedup key for recurring rides: (weekday, time, location) — no date.
    Matches the same weekly ride across different weeks and different posting accounts.
    """
    weekday  = _normalize_key(ride.get("weekday", ""))
    time     = _normalize_key(ride.get("start_time", ""))
    location = _normalize_location(ride)
    return (weekday, time, location)


def deduplicate(existing_rides: List[dict], new_rides: List[dict]) -> Tuple[List[dict], int, int]:
    """
    Merge new_rides into existing_rides.
    Returns (merged_database, added_count, updated_count).

    Matching tiers (in order):
      1. Exact story_id   — same Instagram story re-scraped
      2. Identity key     — (date, time, location) — same event, same week, cross-account
      3. Recurring key    — (weekday, time, location) — same weekly ride, different week
                            Updates the existing record's date to the new occurrence.
                            Never applied to special/one-time events.
      4. Fuzzy title+date — catches minor text extraction differences
      5. New ride         — no match, append to database
    """
    database = list(existing_rides)
    added = 0
    updated = 0

    # Build index of existing rides by identity key (date+time+location)
    index: Dict[Tuple, int] = {}
    for i, ride in enumerate(database):
        key = ride_identity_key(ride)
        if all(k for k in key):
            index[key] = i

    # Build index by recurring key (weekday+time+location) — recurring rides only
    recurring_index: Dict[Tuple, int] = {}
    for i, ride in enumerate(database):
        if _is_recurring(ride):
            rkey = _recurring_key(ride)
            if all(k for k in rkey):
                recurring_index[rkey] = i

    # Also index by story_id for exact match
    story_id_index: Dict[str, int] = {
        r.get("story_id", ""): i
        for i, r in enumerate(database)
        if r.get("story_id")
    }

    for incoming in new_rides:
        story_id = incoming.get("story_id", "")
        identity = ride_identity_key(incoming)
        matched  = False

        # ── Tier 1: Exact story_id (same story re-scraped) ───────────────────
        if story_id and story_id in story_id_index:
            idx = story_id_index[story_id]
            database[idx] = _merge_rides(database[idx], incoming)
            updated += 1
            matched = True

        # ── Tier 2: Identity key (same date+time+location, cross-account) ────
        if not matched and all(k for k in identity) and identity in index:
            idx = index[identity]
            database[idx] = _merge_rides(database[idx], incoming)
            if story_id:
                story_id_index[story_id] = idx
            updated += 1
            matched = True

        # ── Tier 3: Recurring key (same weekday+time+location, new week) ─────
        # Matches the same weekly ride across different weekly occurrences.
        # Updates the existing record's date to this week's occurrence so
        # push_updated_rides() can sync the new date to Airtable.
        # Never applied to special events.
        if not matched and _is_recurring(incoming):
            rkey = _recurring_key(incoming)
            if all(k for k in rkey) and rkey in recurring_index:
                idx = recurring_index[rkey]
                database[idx] = _merge_rides(database[idx], incoming)
                # Always advance to the newest occurrence date
                if incoming.get("date"):
                    database[idx]["date"] = incoming["date"]
                if story_id:
                    story_id_index[story_id] = idx
                if all(k for k in identity):
                    index[identity] = idx
                updated += 1
                matched = True

        # ── Tier 4: Fuzzy title + date ────────────────────────────────────────
        if not matched:
            incoming_title = incoming.get("title", "")
            incoming_date  = _normalize_date(incoming.get("date", "") or incoming.get("weekday", ""))
            for idx, existing in enumerate(database):
                sim = _title_similarity(incoming_title, existing.get("title", ""))
                existing_date = _normalize_date(existing.get("date", "") or existing.get("weekday", ""))
                if sim >= 0.6 and incoming_date and existing_date and incoming_date == existing_date:
                    database[idx] = _merge_rides(database[idx], incoming)
                    if story_id:
                        story_id_index[story_id] = idx
                    if all(k for k in identity):
                        index[identity] = idx
                    updated += 1
                    matched = True
                    break

        # ── Tier 5: New ride ──────────────────────────────────────────────────
        if not matched:
            database.append(incoming)
            new_idx = len(database) - 1
            if all(k for k in identity):
                index[identity] = new_idx
            if _is_recurring(incoming):
                rkey = _recurring_key(incoming)
                if all(k for k in rkey):
                    recurring_index[rkey] = new_idx
            if story_id:
                story_id_index[story_id] = new_idx
            added += 1

    return database, added, updated
