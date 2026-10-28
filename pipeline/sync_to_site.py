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
import base64
import fcntl
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

try:
    from pipeline.site_normalize import normalize_ride
except ImportError:  # run as a plain script from inside pipeline/
    from site_normalize import normalize_ride

# ── Config ────────────────────────────────────────────────────────────────────

BASE_ID      = "appp3CTtWpqVcTn6e"
RIDES_TABLE  = "tbl7xURgDo5wU4z5t"
ORGS_TABLE   = "tblYXWvRSgqWkvaVE"
GITHUB_REPO  = "zenitramserg/ridetrackerfl"

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
THUMBS_PATH    = AVATARS_PATH / "thumbs"
THUMB_PX       = 96   # 2x the largest on-page logo (48px)


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


def _make_thumb(local_url: str, slug: str) -> str | None:
    """
    Small square WebP of an organizer avatar for the site (the originals can be
    600 KB+ for a 34px circle). Regenerated only when the source is newer.
    Returns the web path, or None if Pillow is missing or conversion fails —
    the frontend falls back to the full avatar in that case.
    """
    try:
        from PIL import Image, ImageOps
    except ImportError:
        return None
    src = AVATARS_PATH / Path(local_url).name
    dst = THUMBS_PATH / f"{slug}.webp"
    try:
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            return f"/public/organizers/thumbs/{slug}.webp"
        THUMBS_PATH.mkdir(parents=True, exist_ok=True)
        with Image.open(src) as im:
            im = ImageOps.exif_transpose(im)
            if im.mode in ("RGBA", "LA", "P"):
                im = im.convert("RGBA")
                bg = Image.new("RGBA", im.size, (255, 255, 255, 255))
                bg.alpha_composite(im)
                im = bg
            im = ImageOps.fit(im.convert("RGB"), (THUMB_PX, THUMB_PX), Image.LANCZOS)
            im.save(dst, "WEBP", quality=82, method=6)
        return f"/public/organizers/thumbs/{slug}.webp"
    except Exception as e:
        print(f"[sync] ⚠ Could not build thumbnail for {slug}: {e}")
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


# ── Expire specials / restore displaced recurring rides ───────────────────────

# Notes markers that mean "this record is hidden on purpose — never auto-restore".
# Used for scraper false positives and out-of-scope rides that happen to sit on
# the same weekday as a real special event.
_NO_RESTORE_MARKERS = (
    "NO AUTO-RESTORE",
    "FALSE POSITIVE",
    "NOT LISTED",
    "NEEDS REVIEW",
)

_SPECIAL_TYPES = ("special_event", "special event", "featured event")


def _patch_ride(token: str, record_id: str, fields: dict) -> None:
    """PATCH a single Rides record by field name."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{RIDES_TABLE}/{record_id}"
    body = json.dumps({"fields": fields}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="PATCH",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15):
        pass


def _expire_and_restore(token: str, dry_run: bool = False) -> None:
    """
    Housekeeping pass, run before the display set is fetched.

    With dry_run=True nothing is written back to Airtable; the actions that
    would be taken are printed instead.

    1. Any special_event whose Ride Date has passed is set to 'past' and
       un-featured, so stale events stop showing on the site.
    2. Any weekly ride that was hidden to make room for such a special is
       restored (Status -> confirmed AND Display on Site -> true; both are
       independent gates and a ride needs both to appear).
       Matching is deliberately narrow: same Organizer AND same Day of Week.
       Conservative by design — it would rather miss a restore than wrongly
       un-hide something hidden for another reason.
    3. Anything still hidden that rule 2 did not cover is reported, so a
       deliberately-hidden ride is never silently resurrected, and an
       accidentally-hidden one is never silently forgotten.
    """
    from datetime import date as _date

    today = _date.today()

    try:
        all_rides = _fetch_table(
            token, RIDES_TABLE,
            ["Ride Name", "Organizer", "Day of Week", "Ride Date",
             "Ride Type", "Status", "Display on Site", "Featured", "Notes"],
        )
    except RuntimeError as e:
        print(f"[sync] ⚠ Housekeeping skipped — could not fetch rides: {e}")
        return

    def _parse(d):
        try:
            y, m, dd = map(int, d.split("-"))
            return _date(y, m, dd)
        except Exception:
            return None

    expired_keys = set()   # (organizer, weekday) freed up by a past special
    upcoming_keys = set()  # (organizer, weekday) still claimed by a future special
    to_expire = []

    for rec in all_rides:
        f = rec.get("fields", {})
        if str(f.get("Ride Type", "")).lower() not in _SPECIAL_TYPES:
            continue
        rd = _parse(f.get("Ride Date") or "")
        if not rd:
            continue
        org, dow = f.get("Organizer"), f.get("Day of Week")
        if not org or not dow:
            continue  # can't match reliably; a blank field would collide with
                      # every other blank-field record on the same weekday
        key = (org, dow)
        if rd < today:
            expired_keys.add(key)
            if f.get("Status") in ("confirmed", "planned"):
                to_expire.append((rec["id"], f.get("Ride Name"), f.get("Ride Date")))
        else:
            upcoming_keys.add(key)

    # 1. Expire past specials
    for rid, name, rdate in to_expire:
        if dry_run:
            print(f"[sync] [dry-run] would expire special: {name} ({rdate}) -> past")
            continue
        try:
            _patch_ride(token, rid, {"Status": "past", "Featured": False})
            print(f"[sync] Expired special: {name} ({rdate}) -> past")
        except Exception as e:
            print(f"[sync] ⚠ Could not expire {name}: {e}")

    # 2. Restore weekly rides displaced by a special that has now passed
    restored, orphans = 0, []
    for rec in all_rides:
        f = rec.get("fields", {})
        if f.get("Status") != "hidden":
            continue
        if str(f.get("Ride Type", "")).lower() != "weekly":
            continue

        notes = str(f.get("Notes") or "").upper()
        if any(marker in notes for marker in _NO_RESTORE_MARKERS):
            continue  # hidden on purpose

        org, dow = f.get("Organizer"), f.get("Day of Week")
        if not org or not dow:
            orphans.append(f"{f.get('Ride Name')} (missing Organizer/Day — cannot match)")
            continue
        key = (org, dow)
        if key in upcoming_keys:
            continue  # another special still occupies this slot — leave hidden
        if key not in expired_keys:
            orphans.append(f.get("Ride Name"))
            continue

        if dry_run:
            print(f"[sync] [dry-run] would restore recurring ride: {f.get('Ride Name')}")
            restored += 1
            continue
        try:
            _patch_ride(token, rec["id"],
                        {"Status": "confirmed", "Display on Site": True})
            print(f"[sync] Restored recurring ride: {f.get('Ride Name')}")
            restored += 1
        except Exception as e:
            print(f"[sync] ⚠ Could not restore {f.get('Ride Name')}: {e}")

    # 3. Report anything still hidden that we did not touch
    if orphans:
        print(f"[sync] ⚠ {len(orphans)} hidden weekly ride(s) with no expired "
              f"special to explain them — review manually:")
        for name in orphans:
            print(f"[sync]     · {name}")

    if not to_expire and not restored and not orphans:
        print("[sync] Housekeeping: nothing to expire or restore.")


# ── Generate JSON ──────────────────────────────────────────────────────────────

def generate_rides_json(token: str, dry_run: bool = False) -> dict:
    """
    Pull rides (display_on_site only) and all active organizers from Airtable.
    Returns the combined payload that public/rides.json will contain.
    """
    print("[sync] Housekeeping — expiring past specials, restoring recurring rides...")
    _expire_and_restore(token, dry_run=dry_run)

    print("[sync] Fetching rides from Airtable...")
    # Only export rides that should show on site AND are not hidden/past
    rides = _fetch_table(
        token, RIDES_TABLE, RIDE_FIELDS,
        filter_formula='AND({Display on Site}=TRUE(), NOT(OR({Status}="hidden", {Status}="past")))'
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
            avatar = {"url": local_url, "filename": Path(local_url).name}
            thumb = _make_thumb(local_url, slug)
            if thumb:
                avatar["thumb"] = thumb
            fields["Avatar"] = [avatar]
            downloaded += 1
    print(f"[sync] Avatars synced: {downloaded}/{len(organizers)}.")

    # Display-ready fields for the site (additive `site` object per ride) and
    # removal of the scraper's internal image description from public Notes.
    for rec in rides:
        normalize_ride(rec)

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

def _github_api_push_file(repo_path: str, local_path: Path, token: str, commit_message: str) -> bool:
    """
    Create or update a single file in the GitHub repo via the Contents API.

    Returns True if a commit was made, False if the file content is
    already identical (no-op, not an error).
    """
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/vnd.github+json",
        "User-Agent":    "RideTrackerFL/2.0",
    }

    new_content = local_path.read_bytes()
    new_b64     = base64.b64encode(new_content).decode("ascii")

    # Look up the file's current sha — required by the API to update an
    # existing file, and doubles as our "did anything change" check.
    sha = None
    req = urllib.request.Request(f"{api_url}?ref=main", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            current = json.loads(resp.read().decode("utf-8"))
        sha = current.get("sha")
        if current.get("content", "").replace("\n", "") == new_b64:
            return False  # identical — nothing to push
    except urllib.error.HTTPError as e:
        if e.code != 404:  # 404 = file doesn't exist yet, which is fine
            raise

    body = json.dumps({
        "message": commit_message,
        "content": new_b64,
        "branch":  "main",
        **({"sha": sha} if sha else {}),
    }).encode("utf-8")

    req = urllib.request.Request(
        api_url, data=body, method="PUT",
        headers={**headers, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()
    return True


def _git_push_via_api(repo_root: Path) -> bool:
    """
    Fallback for environments with no local git working copy (e.g. the
    ECS container, which deliberately excludes .git to keep the image
    small). Pushes public/rides.json straight to GitHub via the REST API
    instead of shelling out to git — same end result (a commit on main
    that Netlify's connected auto-deploy picks up), no local repo needed.

    Organizer avatar syncing (public/organizers/) is NOT handled here —
    those change rarely and this keeps the fix scoped to the actual pain
    point (ride data not reaching the live site). Still handled normally
    by the laptop/local git path.
    """
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("[sync] ⚠ No local .git and GITHUB_TOKEN not set — cannot push rides.json.")
        return False

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        pushed = _github_api_push_file(
            "public/rides.json", repo_root / "public" / "rides.json",
            token, f"chore: sync rides.json [{ts}]",
        )
        if pushed:
            print(f"[sync] Pushed to GitHub via API at {ts}.")
        else:
            print("[sync] No changes detected — skipping GitHub push.")
        return pushed
    except Exception as e:
        print(f"[sync] ⚠ GitHub API push failed: {e}")
        return False


# Paths this job owns. A local commit that touches anything else is real
# work, not sync output, and must not be rewritten to clear a collision.
SYNC_OWNED_PATHS = ("public/rides.json", "public/organizers/")


def _reconcile_with_remote(repo_root: Path) -> bool:
    """
    Put the local branch back on top of origin/main before committing.

    Two writers push this branch: this job on the laptop, and the Fargate
    container after its daily scan. Nothing here ever pulled, so whoever
    pushed second was rejected as non-fast-forward and stayed rejected —
    one collision on 2026-10-19 wedged the laptop for two days and left the
    site refreshing only once a day, when the container ran.

    `reset --soft` moves HEAD to origin/main and leaves the index and working
    tree alone, so the rides.json written moments ago survives and the stack
    of superseded sync commits collapses into the one commit that follows.
    Discarding commits is only safe while they hold nothing but sync output,
    hence the guard.
    """
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=repo_root,
                       check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip() or "(no stderr)"
        print(f"[sync] ⚠ Could not fetch origin — skipping push this run: {stderr}")
        return False

    def _count(rev_range: str) -> int:
        out = subprocess.run(["git", "rev-list", "--count", rev_range],
                             cwd=repo_root, check=True, capture_output=True, text=True)
        return int(out.stdout.strip() or 0)

    behind = _count("HEAD..origin/main")
    if behind == 0:
        return True

    changed = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        cwd=repo_root, check=True, capture_output=True, text=True
    ).stdout.split()
    foreign = sorted({f for f in changed if not f.startswith(SYNC_OWNED_PATHS)})
    if foreign:
        print(f"[sync] ⚠ Behind origin/main by {behind} commit(s), and the local commits "
              f"touch files this job does not own: {', '.join(foreign)}. "
              f"Not reconciling automatically — merge or rebase by hand, then rerun.")
        return False

    ahead = _count("origin/main..HEAD")
    subprocess.run(["git", "reset", "--soft", "origin/main"],
                   cwd=repo_root, check=True, capture_output=True, text=True)
    print(f"[sync] Reconciled with origin/main (was {ahead} ahead, {behind} behind) — "
          f"dropped {ahead} superseded sync commit(s), kept the files just written.")
    return True


def git_push(repo_root: Path) -> bool:
    """
    Stage rides.json, commit if changed, push.
    Returns True if a push was made, False if nothing changed.
    """
    if not (repo_root / ".git").exists():
        return _git_push_via_api(repo_root)

    # Clear any stale git lock files left by interrupted processes on mounted filesystems
    for lock_file in ["index.lock", "HEAD.lock", "COMMIT_EDITMSG.lock"]:
        lock_path = repo_root / ".git" / lock_file
        if lock_path.exists():
            try:
                lock_path.unlink()
                print(f"[sync] Cleared stale lock: {lock_file}")
            except OSError:
                pass  # If we can't remove it, the commit will fail and report the real error

    if not _reconcile_with_remote(repo_root):
        return False

    try:
        # Against HEAD, not the index: a soft reset in _reconcile_with_remote
        # leaves the index holding the tree of a commit that no longer exists.
        result = subprocess.run(
            ["git", "diff", "--quiet", "HEAD", "--", "public/rides.json", "public/organizers/"],
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

LOCK_PATH = Path(os.environ.get("RIDETRACKER_LOCK_PATH", "/tmp/ridetrackerfl-sync.lock"))


def _acquire_single_run_lock():
    """
    Hold an exclusive lock for the length of the run, or return None.

    Cron fires this every 15 minutes; a slow Airtable fetch or a stalled
    avatar download can outlast that window, and two overlapping runs racing
    on the same branch produced a ref-lock failure on 2026-10-18. macOS ships
    no flock(1), so the lock lives here instead of in the crontab — which has
    the side benefit of covering manual runs too.
    """
    handle = open(LOCK_PATH, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def main():
    parser = argparse.ArgumentParser(description="Sync Airtable → public/rides.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate JSON locally but do not push to GitHub")
    parser.add_argument("--no-push", action="store_true",
                        help="Write file but skip git push")
    args = parser.parse_args()

    lock = _acquire_single_run_lock()  # released when the process exits
    if lock is None:
        print(f"[sync] Another sync is already running ({LOCK_PATH}) — exiting.")
        return

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
        payload = generate_rides_json(token, dry_run=args.dry_run)
    except (RuntimeError, urllib.error.URLError, OSError) as e:
        # _fetch_table turns an HTTPError into RuntimeError, but a laptop that
        # is asleep, waking, or on flaky wifi fails below that: URLError for
        # DNS, TimeoutError and ConnectionResetError from the socket. Those are
        # OSError subclasses and used to escape this handler, so the common
        # failure printed a traceback instead of the line written for it — 13
        # times in the log so far. Nothing is written either way, so the
        # previous rides.json stands and the site stays live.
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
