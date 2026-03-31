"""
run_scan.py — RideTrackerFL full pipeline orchestrator

Runs all five phases in sequence without any Claude chat context:

  Phase 1  Playwright scrapes Instagram stories → screenshots
  Phase 2  Vision client extracts ride data from each screenshot
  Phase 3  save_rides pipeline validates, deduplicates, writes DB
  Phase 4  Weather enrichment for any new rides (via Open-Meteo)
  Phase 5  Airtable sync — push new/updated rides via pyairtable

Usage:
    python -m pipeline.run_scan                     # full scan
    python -m pipeline.run_scan --use-ollama        # with Ollama pre-filter
    python -m pipeline.run_scan --dry-run           # validate only, no writes
    python -m pipeline.run_scan --visible           # show browser window
    python -m pipeline.run_scan --skip-scrape data/screenshots/2026-03-27_1130

Scheduled task invocation (from cron / Cowork scheduler):
    cd /path/to/ridetrackerfl && python -m pipeline.run_scan
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

# Load .env / secrets.env if present (for ANTHROPIC_API_KEY, AIRTABLE_API_KEY)
_secrets_path = BASE_DIR / "config" / "secrets.env"
if _secrets_path.exists():
    for line in _secrets_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())


# ── Banner ────────────────────────────────────────────────────────────────────

def _banner(label: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'─'*60}")
    print(f"  {label}  [{ts}]")
    print(f"{'─'*60}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RideTrackerFL full pipeline scan")
    parser.add_argument(
        "--use-ollama", action="store_true",
        help="Enable Ollama pre-filter to reduce Claude API calls by ~65%%"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and extract without writing to DB or Airtable"
    )
    parser.add_argument(
        "--visible", action="store_true",
        help="Show browser window during scraping (useful for debugging)"
    )
    parser.add_argument(
        "--skip-scrape", metavar="SCAN_DIR",
        help="Skip Phase 1 and use an existing screenshot directory"
    )
    parser.add_argument(
        "--account", metavar="HANDLE",
        help="Scrape a single account only (for testing)"
    )
    args = parser.parse_args()

    print(f"\n{'═'*60}")
    print(f"  RideTrackerFL — {datetime.now().strftime('%A %B %d, %Y  %H:%M')}")
    print(f"{'═'*60}")

    # ── Phase 1: Playwright scrape ────────────────────────────────────────────
    _banner("Phase 1 · Instagram Scrape (Playwright)")

    if args.skip_scrape:
        scan_dir = Path(args.skip_scrape)
        if not scan_dir.exists():
            print(f"✗ Directory not found: {scan_dir}")
            sys.exit(1)
        print(f"Skipping scrape — using existing directory: {scan_dir.name}")
    else:
        from pipeline.story_scraper import scrape_stories
        scan_dir = asyncio.run(scrape_stories(
            headless=not args.visible,
            handle_filter=args.account,
        ))

    # ── Phase 2: Vision extraction ────────────────────────────────────────────
    _banner("Phase 2 · Vision Extraction (Claude API)")

    from pipeline.vision_client import analyze_scan_directory
    ride_candidates = analyze_scan_directory(scan_dir, use_ollama=args.use_ollama)

    # ── Screenshot cleanup: keep only ride-post images ───────────────────────
    if not args.dry_run:
        _cleanup_non_ride_screenshots(scan_dir, ride_candidates)

    if not ride_candidates:
        print("\nNo ride posts detected in this scan. All done.")
        return

    # Write batch file (preserves raw extractions for debugging)
    batch_path = BASE_DIR / "data" / "scan_batch_latest.json"
    if not args.dry_run:
        with open(batch_path, "w", encoding="utf-8") as f:
            json.dump(ride_candidates, f, indent=2, ensure_ascii=False)
        print(f"\nWrote {len(ride_candidates)} candidates → {batch_path.name}")

    # ── Phase 3: Pipeline (validate, dedup, save) ─────────────────────────────
    _banner("Phase 3 · Pipeline (validate → dedup → save)")

    from pipeline.save_rides import process_batch
    summary = process_batch(batch_path, dry_run=args.dry_run)

    added   = summary.get("added_to_db", 0)
    updated = summary.get("updated_in_db", 0)
    total   = summary.get("total_in_db", 0)
    print(f"Added: {added}  |  Updated: {updated}  |  Total in DB: {total}")

    if args.dry_run:
        print("\n[dry-run] Skipping Phases 4 & 5.")
        _print_summary(summary, scan_dir)
        return

    # ── Phase 4: Weather enrichment ───────────────────────────────────────────
    _banner("Phase 4 · Weather Enrichment")

    if added > 0:
        _enrich_weather()
    else:
        print("No new rides added — skipping weather enrichment.")

    # ── Phase 5: Airtable sync ────────────────────────────────────────────────
    _banner("Phase 5 · Airtable Sync")

    if added > 0 or updated > 0:
        from pipeline.airtable_writer import push_new_rides
        pushed = push_new_rides()
        print(f"Pushed {pushed} record(s) to Airtable.")
    else:
        print("No changes to sync.")

    # ── Summary ───────────────────────────────────────────────────────────────
    _print_summary(summary, scan_dir)


def _cleanup_non_ride_screenshots(scan_dir: Path, ride_candidates: list[dict]):
    """
    Delete every screenshot in scan_dir that was NOT identified as a ride post.
    Keeps only the images a human might want to review, saving disk space.
    scan_metadata.json and any other non-image files are always preserved.
    """
    meta_path = scan_dir / "scan_metadata.json"
    if not meta_path.exists():
        return

    with open(meta_path, encoding="utf-8") as f:
        metadata = json.load(f)

    all_paths  = {Path(s["path"]) for s in metadata.get("screenshots", [])}
    keep_paths = {Path(r["screenshot_path"]) for r in ride_candidates
                  if r.get("screenshot_path")}

    deleted = 0
    for img_path in all_paths:
        if img_path not in keep_paths:
            try:
                img_path.unlink(missing_ok=True)
                deleted += 1
            except Exception as e:
                print(f"[cleanup] ⚠ Could not delete {img_path.name}: {e}")

    kept = len(keep_paths)
    total = len(all_paths)
    print(f"[cleanup] Kept {kept} ride-post screenshot(s), deleted {deleted} of {total} total.")


def _enrich_weather():
    """Fetch weather for any rides in rides_database.json that are missing it."""
    import urllib.request

    DB_PATH = BASE_DIR / "data" / "rides_database.json"
    with open(DB_PATH, encoding="utf-8") as f:
        db = json.load(f)

    # Weston FL coordinates
    LAT, LON = 26.1004, -80.3997
    updated_count = 0

    for ride in db:
        if ride.get("weather_summary"):
            continue   # already enriched
        date_str = ride.get("date", "")
        time_str = ride.get("start_time", "")
        if not date_str:
            continue

        try:
            # Parse date to ISO format for Open-Meteo
            from datetime import datetime as dt
            for fmt in ["%A %B %d, %Y", "%A %B %d %Y", "%Y-%m-%d"]:
                try:
                    d = dt.strptime(date_str.strip(), fmt)
                    break
                except ValueError:
                    continue
            else:
                continue

            iso_date = d.strftime("%Y-%m-%d")
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={LAT}&longitude={LON}"
                f"&hourly=precipitation_probability,windspeed_10m"
                f"&wind_speed_unit=mph&timezone=America%2FNew_York"
                f"&start_date={iso_date}&end_date={iso_date}"
            )
            with urllib.request.urlopen(url, timeout=10) as r:
                data = json.loads(r.read())

            # Find the hour closest to start_time
            hour = 7  # default to 7 AM if no time
            if time_str:
                try:
                    h = dt.strptime(time_str.strip(), "%I:%M %p").hour
                    hour = h
                except ValueError:
                    print(f"  ⚠ Could not parse time '{time_str}', using default 7 AM")

            hourly = data.get("hourly", {})
            rain   = hourly.get("precipitation_probability", [None] * 24)
            wind   = hourly.get("windspeed_10m", [None] * 24)

            rain_pct = rain[hour] if hour < len(rain) else None
            wind_mph = wind[hour] if hour < len(wind) else None

            if rain_pct is not None and wind_mph is not None:
                ride["rain_probability"] = rain_pct
                ride["wind_speed"]       = round(wind_mph, 1)
                ride["weather_summary"]  = (
                    f"{'Dry' if rain_pct < 30 else 'Possible rain'} "
                    f"({rain_pct}% rain), winds {wind_mph:.0f} mph"
                )
                print(f"  ✓ Weather for '{ride['title']}': {ride['weather_summary']}")
                updated_count += 1

        except Exception as e:
            print(f"  ✗ Weather fetch failed for '{ride.get('title', '?')}': {e}")

    if updated_count:
        with open(DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, ensure_ascii=False)
        print(f"  Weather updated for {updated_count} ride(s).")
    else:
        print("  No rides needed weather enrichment.")


def _print_summary(summary: dict, scan_dir: Path):
    print(f"\n{'═'*60}")
    print("  Scan Complete")
    print(f"{'═'*60}")
    print(f"  Scan directory:  {scan_dir.name}")
    print(f"  Raw extracted:   {summary.get('raw_extracted', '—')}")
    print(f"  Valid rides:     {summary.get('valid_rides', '—')}")
    print(f"  Rejected:        {summary.get('rejected', '—')}")
    print(f"  Added to DB:     {summary.get('added_to_db', '—')}")
    print(f"  Updated in DB:   {summary.get('updated_in_db', '—')}")
    print(f"  Total in DB:     {summary.get('total_in_db', '—')}")
    print(f"  Timestamp:       {summary.get('scan_timestamp', '—')}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
