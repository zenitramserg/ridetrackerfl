"""
restore_thanksgiving_rides.py — undo the Thanksgiving 2026 pause.

Four weekly Thursday rides were hidden for 2026-11-26 so only the holiday
events showed. This puts them back exactly as they were and strips the
notes that were added, including the NO AUTO-RESTORE marker on the Team
Recovery ride (the sync's housekeeping restored that one on its own once,
matching an expired Team Recovery Thursday special, so it needed blocking).

Safe to run more than once: a ride already restored is reported and skipped.

    python3 scripts/restore_thanksgiving_rides.py --dry-run
    python3 scripts/restore_thanksgiving_rides.py
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_ID = "appp3CTtWpqVcTn6e"
RIDES_TABLE = "tbl7xURgDo5wU4z5t"

# Captured before hiding — all four were confirmed and visible.
TARGETS = {
    "recHtSoEzNTxyHQBY": "Thursday Tempo Ride (OMG Cycling)",
    "recDfjjtCpA5xYVKH": "Thirsty Thursday Group Ride (MBO Ten Cycling)",
    "recJgnwpRsAuba1yA": "Unicosta Thursday Ride",
    "recK1EPkBZUO59H02": "Team Recovery Weston x GRIT Bike Lab Social Ride",
}

# Any "---"-delimited notes block containing one of these is one this
# pause added, and comes back out on restore.
_PAUSE_MARKERS = (
    "PAUSED for Thanksgiving (2026-11-26)",
    "NO AUTO-RESTORE until 2026-11-27",
)


def _token() -> str:
    for line in (Path(__file__).parent.parent / "config" / "secrets.env").read_text().splitlines():
        if line.strip().startswith("AIRTABLE_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("AIRTABLE_API_KEY not found in config/secrets.env")


def _request(req: urllib.request.Request, attempts: int = 4) -> dict:
    """
    Do one Airtable call, retrying transient network failures.

    The connection to Airtable times out occasionally from this machine. An
    unattended run has nobody to notice and re-run it, so a timeout must not
    be fatal. HTTP errors (404, 403, 422) are real answers, not blips, and
    are raised immediately rather than retried.
    """
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt == attempts:
                raise
            backoff = 2 ** attempt
            print(f"      (network issue: {e}; retry {attempt}/{attempts - 1} in {backoff}s)")
            time.sleep(backoff)
    raise RuntimeError("unreachable")


def _strip_pause_notes(notes: str) -> str:
    kept = [b for b in (notes or "").split("\n---\n")
            if not any(m in b for m in _PAUSE_MARKERS)]
    return "\n---\n".join(kept).strip()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Show what would change, write nothing.")
    args = ap.parse_args()

    hdr = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}
    restored = skipped = failed = 0

    for rid, label in TARGETS.items():
        url = f"https://api.airtable.com/v0/{BASE_ID}/{RIDES_TABLE}/{rid}"
        try:
            fields = _request(urllib.request.Request(url, headers=hdr))["fields"]
        except urllib.error.HTTPError as e:
            print(f"  x could not read {label}: HTTP {e.code}")
            failed += 1
            continue
        except Exception as e:
            print(f"  x could not read {label} after retries: {e}")
            failed += 1
            continue

        if fields.get("Status") == "confirmed" and fields.get("Display on Site"):
            print(f"  - already live, skipping: {label}")
            skipped += 1
            continue

        patch = {
            "Status": "confirmed",
            "Display on Site": True,
            "Notes": _strip_pause_notes(fields.get("Notes", "")),
        }
        if args.dry_run:
            print(f"  [dry-run] would restore: {label}")
            restored += 1
            continue

        try:
            req = urllib.request.Request(url, data=json.dumps({"fields": patch}).encode(),
                                         method="PATCH", headers=hdr)
            out = _request(req)["fields"]
            print(f"  ok restored: {label}  (status={out.get('Status')}, "
                  f"visible={out.get('Display on Site', False)})")
            restored += 1
        except urllib.error.HTTPError as e:
            print(f"  x failed to restore {label}: HTTP {e.code} {e.read().decode()[:200]}")
            failed += 1
        except Exception as e:
            print(f"  x failed to restore {label} after retries: {e}")
            failed += 1

    print(f"\nrestored={restored}  already-live={skipped}  failed={failed}")
    if failed:
        raise SystemExit(1)
    if not args.dry_run and restored:
        print("Now run:  python3.12 -m pipeline.sync_to_site")


if __name__ == "__main__":
    main()
