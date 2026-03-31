"""
classifier.py
Classify each extracted ride record by type and validate it is a true group ride.
Ported and adapted from weston-rides-monitor legacy project.
"""

import json
import re
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config" / "known_locations.json"


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

_CONFIG = _load_config()
NOISE_PATTERNS = _CONFIG["noise_patterns"]
PERFORMANCE_PATTERNS = _CONFIG["performance_patterns"]
PROMO_PATTERNS = _CONFIG["promo_patterns"]


def is_noise(text: str) -> bool:
    t = (text or "").lower()
    return any(p in t for p in NOISE_PATTERNS)


def is_performance_post(text: str) -> bool:
    t = (text or "").lower()
    return any(p in t for p in PERFORMANCE_PATTERNS)


def is_promo_post(text: str) -> bool:
    """Detect shop promotions, demos, and sales — excluded per project rules."""
    t = (text or "").lower()
    return any(p in t for p in PROMO_PATTERNS)


def classify_ride(ride: dict) -> str:
    """
    Returns one of:
      - weekly_ride   : recurring weekday group ride
      - ride_event    : one-off or special event ride
      - review        : uncertain, needs manual check
    """
    raw = (ride.get("raw_visible_text", "") or "").lower()
    title = (ride.get("title", "") or "").lower()
    weekday = (ride.get("weekday", "") or "").lower()

    # Special / one-off events
    event_keywords = ["tour", "event", "register", "sign up", "race", "century", "gran fondo"]
    if any(k in raw or k in title for k in event_keywords):
        return "ride_event"

    # Recurring weekday rides
    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if weekday in weekdays or any(w in title for w in weekdays):
        return "weekly_ride"

    # Has time and location — likely a structured recurring ride
    if ride.get("start_time") and ride.get("start_location"):
        return "weekly_ride"

    return "review"


def is_valid_group_ride(ride: dict) -> bool:
    """
    Gate check: returns True only for genuine group ride flyers.
    Rejects: promos, performance stats, noise, and incomplete records.
    Requires at minimum: start_time + (distance OR pace OR start_location).
    """
    raw = ride.get("raw_visible_text", "") or ""

    if is_noise(raw):
        return False
    if is_performance_post(raw):
        return False
    if is_promo_post(raw):
        return False

    # Must pass the confidence threshold set during vision extraction
    if float(ride.get("confidence", 0)) < 0.5:
        return False

    has_time     = bool(ride.get("start_time"))
    has_distance = bool(ride.get("distance"))
    has_pace     = bool(ride.get("pace"))
    has_location = bool(ride.get("start_location"))
    confidence   = float(ride.get("confidence", 0))

    # Standard path: time + at least one other detail
    if has_time and (has_distance or has_pace or has_location):
        return True

    # Relaxed path: high-confidence detection with a clear location but no explicit time
    # Covers event announcements and recurring rides where time is community-known
    if has_location and confidence >= 0.70:
        return True

    return False
