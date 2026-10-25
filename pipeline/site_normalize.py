"""
pipeline/site_normalize.py — display-ready fields for the website.

Called by sync_to_site.py for every ride before rides.json is written.
Adds a `site` object next to `fields` on each ride record. Purely additive:
the Airtable `fields` are left as they are (except Notes, see below), so the
current index.html keeps working unchanged.

    rec["site"] = {
        "time_min": 360,             # minutes after midnight, for sorting (None if unparseable)
        "time_display": "6:00 AM",
        "pace_label": "22–24 mph",   # short pill text
        "pace_groups": ["fast"],     # any of fast | mod | soc  (filter chips)
        "pace_top": "fast",          # pill colour: fast | mod | soc | unk
        "pace_nodrop": False,
        "dist_display": "42/52/62 mi",
        "venue": "Weston Town Center",
        "venue_detail": "Behind Bank of America",
        "area": "Weston",            # Weston | Sunrise | Pembroke Pines | Miami | Fort Lauderdale
        "display_name": "Tuesday New Season",
    }

Notes: the scraper writes "Key: value" lines, then "---", then Claude's
description of the Instagram image. That description is internal and must
not be published, so Notes is replaced with only the free-text lines, and
Address/Pace/Distance lines fill the real fields when those are empty.
"""
from __future__ import annotations

import re

AREA_RULES = [
    (re.compile(r"sunrise", re.I), "Sunrise"),
    (re.compile(r"pembroke|taft st", re.I), "Pembroke Pines"),
    (re.compile(r"miami|coconut grove|key biscayne", re.I), "Miami"),
    (re.compile(r"fort lauderdale|ft\.? lauderdale", re.I), "Fort Lauderdale"),
]

VENUE_ALIASES = [
    (re.compile(r"alex'?s?\s+(bicycle|bike)\s*(pro\s*)?shop", re.I), "Alex's Bicycle Pro Shop"),
    (re.compile(r"sunrise\s+sports\s*(plex|complex)", re.I), "Sunrise Sportsplex"),
    (re.compile(r"grit\.?\s*bike\s*lab", re.I), "GRIT Bike Lab"),
    (re.compile(r"weston\s+town\s+cent(er|re)", re.I), "Weston Town Center"),
]

_NOTE_KV = re.compile(r"^\s*(Address|Pace|Distance)\s*:\s*(.+)$", re.I)
_NODROP = re.compile(r"no.?drop|recovery|chill|social|controlled", re.I)
_SEP = r"\s*[-–—:|]\s*"


def parse_time(raw) -> int | None:
    if not raw:
        return None
    s = str(raw).strip()
    m = re.search(r"(\d{1,2})(?::(\d{2}))?\s*([AaPp])\.?\s*[Mm]?\b", s)
    if m:
        h = int(m.group(1)) % 12
        if m.group(3).lower() == "p":
            h += 12
        return h * 60 + int(m.group(2) or 0)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def fmt_time(minutes: int | None) -> str | None:
    if minutes is None:
        return None
    h, mm = divmod(minutes, 60)
    return f"{h % 12 or 12}:{mm:02d} {'PM' if h >= 12 else 'AM'}"


def _group(n: float) -> str:
    return "fast" if n >= 21 else "mod" if n >= 17 else "soc"


def pace_info(raw) -> dict:
    empty = {"pace_label": "", "pace_groups": [], "pace_top": "unk", "pace_nodrop": False}
    if not raw:
        return empty
    s = str(raw)
    plus = "+" in s
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", s) if 10 <= float(n) <= 35]
    nodrop = bool(_NODROP.search(s))
    groups: set[str] = set()
    label = ""
    if nums:
        lo, hi = min(nums), max(nums)
        fmt = lambda n: str(int(n)) if n == int(n) else str(n)
        label = (fmt(lo) if lo == hi else f"{fmt(lo)}–{fmt(hi)}") + ("+" if plus else "") + " mph"
        for n in range(int(lo), int(hi) + 1):
            groups.add(_group(n))
    else:
        letters = sorted(set(re.findall(r"\b([A-D])\b", s.upper())))
        if letters:
            label = "/".join(letters) + (" groups" if len(letters) > 1 else " group")
            for l in letters:
                groups.add("fast" if l == "A" else "mod" if l == "B" else "soc")
    if nodrop:
        groups.add("soc")
    if not label and nodrop:
        label = "No-drop"
    top = next((g for g in ("fast", "mod", "soc") if g in groups), "unk")
    return {
        "pace_label": label or s,
        "pace_groups": [g for g in ("fast", "mod", "soc") if g in groups],
        "pace_top": top,
        "pace_nodrop": nodrop and bool(nums),
    }


def fmt_dist(raw) -> str:
    if not raw:
        return ""
    s = re.sub(r"\s*(mi|miles?)\.?\s*$", "", str(raw).strip(), flags=re.I)
    s = re.sub(r"\s*/\s*", "/", s)
    return f"{s} mi" if re.search(r"\d", s) else s


def split_location(loc) -> tuple[str, str]:
    full = str(loc or "").strip()
    parts = re.split(r"\s+[·—–]\s+|\s+-\s+", full)
    venue, detail = parts[0], ", ".join(parts[1:])
    if "," in venue and not re.match(r"\d", venue):
        v, rest = venue.split(",", 1)
        venue = v.strip()
        detail = " · ".join(x for x in (rest.strip(), detail) if x)
    elif "," in venue:
        venue = venue.split(",", 1)[0].strip()
    for rx, name in VENUE_ALIASES:
        if rx.search(venue):
            venue = name
            break
    return venue, detail


def area_of(loc) -> str:
    for rx, name in AREA_RULES:
        if rx.search(str(loc or "")):
            return name
    return "Weston"


def display_name(name: str, org: str) -> str:
    """Drop the organizer from the ride name when it is just a prefix/suffix
    joined by a separator: 'OMG Cycling - Tuesday New Season' -> 'Tuesday New Season'."""
    name = (name or "").strip()
    if not org or not name:
        return name or org
    o = re.escape(org.strip())
    short = re.sub(rf"^{o}{_SEP}", "", name, flags=re.I)
    short = re.sub(rf"{_SEP}{o}$", "", short, flags=re.I)
    short = short.strip()
    if not short:
        return name
    return short[0].upper() + short[1:]


def clean_notes(fields: dict) -> None:
    """Strip the image description from Notes; use Address/Pace/Distance lines as fallbacks."""
    notes = fields.get("Notes")
    if not notes:
        return
    # Anchored per line, not "newline then dashes": the scraper omits the
    # Address/Pace/Distance block when the flyer has none, and Notes then
    # *begins* with the separator — no preceding newline to match, so the
    # old pattern cut nothing and published the image description.
    head = re.split(r"(?m)^\s*-{3,}\s*$", str(notes))[0]
    kv, free = {}, []
    for line in head.splitlines():
        m = _NOTE_KV.match(line)
        if m:
            kv[m.group(1).lower()] = m.group(2).strip()
        elif line.strip() and not line.strip().startswith("---"):
            free.append(line.strip())
    if kv.get("pace") and not fields.get("Pace"):
        fields["Pace"] = kv["pace"]
    if kv.get("distance") and not fields.get("Distance"):
        fields["Distance"] = kv["distance"]
    if kv.get("address") and not fields.get("Address Note"):
        fields["Address Note"] = kv["address"]
    if free:
        fields["Notes"] = "\n".join(free)
    else:
        fields.pop("Notes", None)


def normalize_ride(rec: dict) -> dict:
    """Mutates rec in place (cleans Notes, adds rec['site']) and returns it."""
    f = rec.setdefault("fields", {})
    clean_notes(f)
    venue, detail = split_location(f.get("Location"))
    addr = f.get("Address Note") or ""
    if addr and addr.lower() not in (detail or "").lower():
        detail = " · ".join(x for x in (detail, addr) if x)
    t = parse_time(f.get("Time"))
    rec["site"] = {
        "time_min": t,
        "time_display": fmt_time(t),
        **pace_info(f.get("Pace")),
        "dist_display": fmt_dist(f.get("Distance")),
        "venue": venue,
        "venue_detail": detail,
        "area": area_of(f.get("Location")),
        "display_name": display_name(f.get("Ride Name") or "", f.get("Organizer") or ""),
    }
    return rec


if __name__ == "__main__":  # quick check: python3 -m pipeline.site_normalize public/rides.json
    import json, sys
    data = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "public/rides.json"))
    for r in data["rides"]["records"]:
        normalize_ride(r)
        s = r["site"]
        print(f'{s["time_display"] or "TBA":>8} | {s["display_name"][:38]:38} | {s["pace_label"][:16]:16} {s["pace_top"]:4} | {s["dist_display"][:12]:12} | {s["venue"][:24]:24} | {s["area"]}')
