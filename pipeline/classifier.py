"""
classifier.py
Classify each extracted ride record by type and validate it is a true group ride.
Ported and adapted from weston-rides-monitor legacy project.
"""

import functools
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
SUPPRESSED_TITLES = _CONFIG.get("suppressed_titles", [])
TRAVEL_PACKAGE_PATTERNS = _CONFIG.get("travel_package_patterns", [])


@functools.lru_cache(maxsize=None)
def _compile(pattern: str) -> re.Pattern:
    r"""
    Word-bounded when the pattern is only letters and spaces, a plain substring
    otherwise.

    Bare substrings misfire: 38% of the scraped posts are Spanish or
    Portuguese, and "sale" — Spanish for "it departs", as in "sale a las
    6:00 AM" — sits inside the promo list, as it does inside "wholesale".

    The split matters because blanket boundaries would break the patterns that
    start with a symbol. "% off" is preceded by a digit in "20% off", and a
    (?<!\w) lookbehind fails against a digit, so the pattern would stop
    matching the exact thing it was written for.
    """
    if pattern.replace(" ", "").isalpha():
        return re.compile(rf"(?<!\w){re.escape(pattern)}(?!\w)", re.I)
    return re.compile(re.escape(pattern), re.I)


def _matched(patterns, text: str) -> str | None:
    """The first pattern that matches, so a rejection can name its evidence."""
    t = text or ""
    return next((p for p in patterns if _compile(p).search(t)), None)


def is_noise(text: str) -> bool:
    return _matched(NOISE_PATTERNS, text) is not None


def is_suppressed_title(title: str) -> bool:
    """Returns True if this ride title is on the suppression list (e.g. route maps, generic posts)."""
    t = (title or "").lower().strip()
    return any(p in t for p in SUPPRESSED_TITLES)


def is_performance_post(text: str) -> bool:
    return _matched(PERFORMANCE_PATTERNS, text) is not None


def is_promo_post(text: str) -> bool:
    """Detect shop promotions, demos, and sales — excluded per project rules."""
    return _matched(PROMO_PATTERNS, text) is not None


def is_travel_package(text: str) -> bool:
    """
    A trip to a ride is not a ride. These posts sell transport and lodging to
    get to someone else's event, and the "start time" the vision model pulls
    out of them is a coach departure — so they publish as a ride nobody can
    turn up to. The Horrible Hundred post was extracted with the *return*
    coach (4:00 PM Sunday) as its start.

    Keyed on an overnight stay, because a single-day group ride has no reason
    to mention one. Deliberately NOT keyed on price, "per person", "limited
    spots" or a shuttle: the genuine Miami→Key West and Coast to Coast rides
    carry all four. Nor on "register separately", which a local ride uses to
    mean "individually, not as a team". Nor on "hotel", which a local ride can
    legitimately meet at.

    Measured over the 119 stored rides: flags the two travel packages
    (Horrible Hundred, Clermont Weekend Experience), nothing else.
    """
    return _matched(TRAVEL_PACKAGE_PATTERNS, text) is not None


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


# Below this, the vision extraction is not trusted enough to publish.
CONFIDENCE_FLOOR = 0.5


def rejection_reason(ride: dict) -> str | None:
    """
    Return a short, printable reason this ride fails the gate, or None if it
    passes. is_valid_group_ride() is this function read as a boolean.

    It exists because the gate used to reject silently: a run would report
    "Valid rides: 1, Rejected: 3" with nothing to say which three or why, and
    the stories they came from expire within 24 hours, so there was no way to
    go back and look. Three high-confidence candidates were lost that way on
    2026-10-18 before anyone could see what the gate disliked.

    Minimum required fields:
      - start_time   (when the ride leaves)
      - start_location (where it meets)
      - weekday OR explicit date (when in the week / calendar)

    All three must be present. Rides missing any one are rejected — they
    would show on the website with missing info and confuse users.
    """
    raw = ride.get("raw_visible_text", "") or ""

    # Each of these names the pattern it matched. A rejection that only states
    # a category cannot be checked: "shop promo" on a 0.93-confidence century
    # ride is either a correct call or a substring misfire, and the flyer it
    # came from has expired by the time anyone asks.
    if m := _matched(NOISE_PATTERNS, raw):
        return f"noise text (matched {m!r})"
    if m := _matched(PERFORMANCE_PATTERNS, raw):
        return f"performance post, ride stats rather than an invitation (matched {m!r})"
    if m := _matched(PROMO_PATTERNS, raw):
        return f"shop promo (matched {m!r})"
    if m := _matched(TRAVEL_PACKAGE_PATTERNS, raw):
        return f"travel package, a trip to an event rather than a ride (matched {m!r})"
    if is_suppressed_title(ride.get("title", "")):
        return f"suppressed title: {ride.get('title', '')!r}"

    # Must pass the confidence threshold set during vision extraction.
    # Use `or 0` instead of default=0 — the vision model can return an
    # explicit `null` confidence, and default= only covers a missing key,
    # not a present-but-null value. float(None) would raise TypeError and
    # crash the whole batch (same bug class fixed in deduplicator.py).
    confidence = float(ride.get("confidence") or 0)
    if confidence < CONFIDENCE_FLOOR:
        return f"confidence {confidence:.2f} below {CONFIDENCE_FLOOR:.2f}"

    # Treat the literal sentinel "empty string" (returned by the vision model
    # for missing fields) as falsy — same as None or "".
    def _present(val) -> bool:
        return bool(val) and str(val).strip().lower() != "empty string"

    missing = []
    if not _present(ride.get("start_time")):
        missing.append(f"start_time={ride.get('start_time')!r}")
    if not _present(ride.get("start_location")):
        missing.append(f"start_location={ride.get('start_location')!r}")
    if not (_present(ride.get("weekday")) or _present(ride.get("date"))):
        missing.append(f"weekday={ride.get('weekday')!r} and date={ride.get('date')!r}")
    if missing:
        # The value is printed, not just the field name: "empty string" and a
        # plausible-but-unusable value fail here for different reasons, and
        # only the value tells you which.
        return "missing required field(s) — " + "; ".join(missing)

    return None


def is_valid_group_ride(ride: dict) -> bool:
    """Gate check: True only for genuine, publishable group ride flyers."""
    return rejection_reason(ride) is None
