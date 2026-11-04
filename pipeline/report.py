"""
report.py
Builds the per-run email: which rides went live, which were seen again, which
were rejected, and anything that looks off — each with a link to its Airtable
record, so a review is a click per ride rather than a search.

Plain text on purpose: the report goes out through SNS email, which cannot
send HTML. Most mail clients still turn the bare URLs into links.
"""

from datetime import datetime

from pipeline.airtable_writer import BASE_ID, TABLE_ID, _normalize_time
from pipeline.deduplicator import _normalize_location, _title_similarity

# Below this word overlap, two titles on one record are probably two different
# rides merged together, not two wordings of the same flyer.
_DIFFERENT_TITLE = 0.3


def _record_url(record_id: str | None) -> str:
    if not record_id:
        return "(not in Airtable yet)"
    return f"https://airtable.com/{BASE_ID}/{TABLE_ID}/{record_id}"


def _short_date(text: str | None) -> str:
    """'Thursday November 05, 2026' -> 'Thu Nov 5'. Unparseable text is kept as-is."""
    text = (text or "").strip()
    if not text:
        return "no date"
    for fmt in ("%A %B %d, %Y", "%Y-%m-%d", "%A %B %d %Y", "%B %d, %Y",
                "%A %B %d", "%A %b %d", "%B %d"):
        try:
            d = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if "%Y" in fmt:
            return f"{d:%a %b} {d.day}"
        # No year means strptime assumed 1900, so its weekday is wrong; keep
        # the weekday the post itself gave, if it gave one.
        day = text.split()[0][:3] if fmt.startswith("%A") else ""
        return f"{day} {d:%b} {d.day}".strip()
    return text


def _differences(before: dict, incoming: dict) -> list[str]:
    """What the scraper saw that differs from the stored record.

    Only start time, location and a clearly different title are compared;
    those are what a moved, retimed or wrongly merged ride looks like.
    """
    out = []
    bt, it = before.get("start_time"), incoming.get("start_time")
    if bt and it and _normalize_time(bt) != _normalize_time(it):
        out.append(f"time: on file {bt}, post says {it}")
    bl, il = before.get("start_location"), incoming.get("start_location")
    if bl and il and _normalize_location(before) != _normalize_location(incoming):
        out.append(f"location: on file {bl}, post says {il}")
    bn, inn = before.get("title"), incoming.get("title")
    if bn and inn and _title_similarity(bn, inn) < _DIFFERENT_TITLE:
        out.append(f"title: on file '{bn}', post says '{inn}'")
    return out


def _same_slot(ride: dict, db: list[dict]) -> list[dict]:
    """Other rides on file for the same organizer and weekday.

    A ride that changes time or start location does not match its old record
    (those are part of the match key), so it arrives as new. This surfaces the
    record it most likely replaces.
    """
    org = (ride.get("organized_by") or "").lower().strip()
    day = (ride.get("weekday") or "").lower().strip()
    if not org or not day:
        return []
    return [r for r in db
            if r.get("ride_id") != ride.get("ride_id")
            and (r.get("organized_by") or "").lower().strip() == org
            and (r.get("weekday") or "").lower().strip() == day]


def build_report(summary: dict, outcomes: list[dict], db: list[dict],
                 posts_found: int, site_sync: str) -> tuple[str, str]:
    """Return (subject, body) for this run's email."""
    by_id = {r.get("ride_id"): r for r in db}
    action = {o["ride_id"]: o for o in outcomes}

    # One entry per stored ride, in the order the scraper met them; a ride
    # posted twice in one run collects both sightings.
    grouped: dict[str, list[dict]] = {}
    for e in summary.get("match_events", []):
        grouped.setdefault(e["ride_id"], []).append(e)

    new, seen, check = [], [], []
    for ride_id, events in grouped.items():
        ride = by_id.get(ride_id) or events[-1]["incoming"]
        name = ride.get("title") or "(untitled)"
        record_id = ride.get("airtable_record_id") or action.get(ride_id, {}).get("record_id")
        added = any(e["kind"] == "added" for e in events)

        if added and action.get(ride_id, {}).get("action") != "linked":
            lines = [f"  • {name} — {ride.get('organized_by') or '?'}",
                     f"    {_short_date(ride.get('date'))} · {ride.get('start_time') or 'no time'}"
                     f" · {ride.get('start_location') or 'no location'}",
                     f"    {_record_url(record_id)}"]
            for other in _same_slot(ride, db)[:2]:
                lines.append(f"    ⚠ also on file for {ride.get('weekday')}: '{other.get('title')}'"
                             f" {other.get('start_time') or ''} at {other.get('start_location') or '?'}"
                             f" — moved or retimed? {_record_url(other.get('airtable_record_id'))}")
            new.append("\n".join(lines))
            continue

        # Seen again. Compare against the record as it stood before this run:
        # the local copy keeps whichever title is longest, but Airtable never
        # receives that, so the pre-run title is the one you will see there.
        baseline = next((e["before"] for e in events if e["before"]), {})
        name = baseline.get("title") or name
        old_date = _short_date(baseline.get("date")) if baseline.get("date") else None
        cur_date = _short_date(ride.get("date"))
        when = f"{old_date} → {cur_date}" if old_date and old_date != cur_date else cur_date
        seen.append(f"{name} ({when})")

        diffs = [d for e in events for d in _differences(baseline, e["incoming"])]
        if diffs:
            check.append("\n".join([f"  • {name}",
                                    *[f"    {d}" for d in dict.fromkeys(diffs)],
                                    "    Not applied — only dates sync to existing records.",
                                    f"    {_record_url(record_id)}"]))

    rejected = [f"  • {r['title']} (@{r['source_account']}, conf {r['confidence']})\n"
                f"    {r['reason']}"
                for r in summary.get("rejections", [])]

    parts = [f"{len(new)} new", f"{len(seen)} re-confirmed", f"{len(rejected)} rejected"]
    if check:
        parts.append(f"{len(check)} to check")
    subject = "RideTrackerFL: " + ", ".join(parts)

    body = []
    if new:
        body += ["NEW — already live on the site", *new, ""]
    if check:
        body += ["CHECK — the post differs from what is on file", *check, ""]
    if rejected:
        body += ["REJECTED — not saved; the story expires within 24h", *rejected, ""]
    if seen:
        body += [f"RE-CONFIRMED ({len(seen)})", *[f"  {s}" for s in seen], ""]
    if not (new or check or rejected or seen):
        body += ["No rides added, changed or rejected.", ""]
    body += [f"Run: {posts_found} ride post(s) found · {summary.get('total_in_db', '?')} in DB"
             f" · site sync {site_sync}"]
    return subject, "\n".join(body)
