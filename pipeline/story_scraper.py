"""
story_scraper.py
Playwright-based Instagram story scraper.

Navigates to each account's story page, clicks through slides,
and saves screenshots + metadata for the vision pipeline.

This replaces the Claude in Chrome MCP browser-control leg of the scan,
eliminating the largest source of token consumption.

Requires:
    pip install playwright
    playwright install chromium
    config/instagram_cookies.json  (run scripts/save_instagram_session.py once to generate)

Usage:
    python -m pipeline.story_scraper              # headless, all active accounts
    python -m pipeline.story_scraper --visible    # show browser window (debug)
    python -m pipeline.story_scraper --account revoltcyclery  # single account test
"""

import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

# Paths are env-overridable so the scraper can run somewhere other than
# the laptop (a container has its own filesystem layout, and credentials
# arrive from a secret store rather than config/).
ACCOUNTS_PATH    = Path(os.environ.get("RIDETRACKER_ACCOUNTS_PATH",    BASE_DIR / "config" / "accounts.json"))
COOKIES_PATH     = Path(os.environ.get("RIDETRACKER_COOKIES_PATH",     BASE_DIR / "config" / "instagram_cookies.json"))
SCREENSHOTS_BASE = Path(os.environ.get("RIDETRACKER_SCREENSHOTS_DIR",  BASE_DIR / "data" / "screenshots"))

# A redirect to any of these means Instagram rejected the session itself —
# not that the account has no stories. Without this distinction a dead
# session reads as "no active stories" on every account and the scan
# exits 0, silently serving stale data until someone notices.
_AUTH_FAIL_MARKERS = ("/accounts/login", "/challenge", "/checkpoint")

# ── Story viewer ──────────────────────────────────────────────────────────────
# Slides are advanced with the keyboard rather than a click at fixed coordinates.
# The panel's width depends on what the slide holds and on how the browser renders
# it, so a point that lands inside the panel on the laptop lands on the backdrop in
# the container, where clicking does nothing at all.
STALL_RECOVERY_ATTEMPTS = 2   # advance retries before giving up on a frozen viewer
MAX_SLIDES_PER_ACCOUNT = 24   # safety cap; most accounts have 1-5 active stories
PAGE_LOAD_MS   = 3500         # wait after initial navigation (story must fully load)
SLIDE_LOAD_MS  = 2500         # wait between slides (video stories need a moment; extra 500ms for animated flyers)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_accounts(handle_filter: str | None = None) -> list[dict]:
    with open(ACCOUNTS_PATH, encoding="utf-8") as f:
        data = json.load(f)
    accounts = [a for a in data["accounts"] if a.get("active")]
    if handle_filter:
        accounts = [a for a in accounts if a["handle"] == handle_filter]
    return accounts


def _load_cookies() -> list[dict]:
    if not COOKIES_PATH.exists():
        raise FileNotFoundError(
            f"\n✗ Instagram cookies not found at:\n  {COOKIES_PATH}\n\n"
            "Run this once to generate them:\n"
            "  python scripts/save_instagram_session.py\n"
        )
    with open(COOKIES_PATH, encoding="utf-8") as f:
        return json.load(f)


async def _advance_slide(page, recovery: bool = False) -> bool:
    """
    Move the story viewer to the next slide.

    Returns False only when the viewer could not be driven at all; a True return
    says the attempt was made, not that the slide actually moved — the caller
    decides that by comparing frames.
    """
    try:
        await page.keyboard.press("ArrowRight")
        if recovery:
            # A frozen panel may have taken focus away from the viewer, leaving the
            # keyboard shortcut with nothing to act on. The viewer's own Next control
            # does not depend on focus, so it is worth a try before giving up.
            try:
                next_btn = page.get_by_role("button", name="Next")
                if await next_btn.is_visible(timeout=1000):
                    await next_btn.click()
            except Exception:
                pass
        await page.wait_for_timeout(SLIDE_LOAD_MS)
        return True
    except Exception as e:
        print(f"[scraper]   ✗ Advance failed: {e}")
        return False


def _story_id_from_url(url: str) -> str:
    """Extract story ID from URL like /stories/handle/3862367330567642767/"""
    parts = [p for p in url.rstrip("/").split("/") if p]
    if parts and parts[-1].isdigit():
        return parts[-1]
    return ""


# ── Main scraper ──────────────────────────────────────────────────────────────

async def scrape_stories(
    headless: bool = True,
    handle_filter: str | None = None,
) -> Path:
    """
    Scrape stories from all active accounts (or one specific account).

    Returns the path to the scan session directory, which contains:
      - {handle}_{slide:02d}.png    one screenshot per slide
      - scan_metadata.json          index of all screenshots + account info
    """
    from playwright.async_api import async_playwright

    accounts = _load_accounts(handle_filter)
    cookies  = _load_cookies()

    scan_ts  = datetime.now().strftime("%Y-%m-%d_%H%M")
    scan_dir = SCREENSHOTS_BASE / scan_ts
    scan_dir.mkdir(parents=True, exist_ok=True)

    metadata: dict = {
        "scan_timestamp":    datetime.now().isoformat(timespec="seconds"),
        "scan_directory":    str(scan_dir),
        "accounts_checked":  [],
        "screenshots":       [],
    }
    auth_failures = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        await context.add_cookies(cookies)
        page = await context.new_page()

        for account in accounts:
            handle     = account["handle"]
            max_slides = account.get("max_slides", MAX_SLIDES_PER_ACCOUNT)
            print(f"[scraper] ── @{handle} (max {max_slides} slides) ──────────────────────────────")

            story_url = f"https://www.instagram.com/stories/{handle}/"
            try:
                await page.goto(story_url, wait_until="domcontentloaded", timeout=20_000)
                await page.wait_for_timeout(PAGE_LOAD_MS)
            except Exception as e:
                print(f"[scraper]   ✗ Navigation failed: {e}")
                metadata["accounts_checked"].append({
                    "handle": handle,
                    "slides_captured": 0,
                    "error": str(e),
                })
                continue

            # Instagram redirected away — either the session is dead, or
            # this account simply has no active stories right now.
            if f"/stories/{handle}/" not in page.url:
                if any(m in page.url for m in _AUTH_FAIL_MARKERS):
                    print(f"[scraper]   ✗ SESSION REJECTED — sent to {page.url[:70]}")
                    auth_failures += 1
                    metadata["accounts_checked"].append({
                        "handle": handle,
                        "slides_captured": 0,
                        "error": "session_rejected",
                    })
                    continue

                print(f"[scraper]   ○ No active stories (redirected to {page.url[:60]})")
                metadata["accounts_checked"].append({
                    "handle": handle,
                    "slides_captured": 0,
                })
                continue

            # ── Dismiss "View story" confirmation screen ──────────────────────
            # Instagram shows a privacy confirmation ("View as X? omg_cycling will
            # be able to see that you viewed their story.") before showing the story.
            # We need to click "View story" to proceed.
            try:
                view_btn = page.get_by_role("button", name="View story")
                if await view_btn.is_visible(timeout=3000):
                    await view_btn.click()
                    print(f"[scraper]   ✓ Dismissed 'View story' confirmation")
                    await page.wait_for_timeout(SLIDE_LOAD_MS)
            except Exception:
                pass  # No confirmation screen — already on the story, continue

            # ── Step through slides ───────────────────────────────────────────
            slide_idx    = 0     # slides actually captured
            last_digest  = None  # fingerprint of the previous capture
            stall_tries  = 0     # advances that produced no new frame

            while slide_idx < max_slides:

                # Bail if Instagram has moved us off this account's stories
                if f"/stories/{handle}/" not in page.url:
                    print(f"[scraper]   → Story ended after {slide_idx} slides")
                    break

                try:
                    shot = await page.screenshot(full_page=False)
                except Exception as e:
                    print(f"[scraper]   ✗ Screenshot error: {e}")
                    break

                digest = hashlib.sha256(shot).hexdigest()

                # An identical frame means the viewer never moved. A story whose video
                # this browser cannot play freezes the panel on an error card: the
                # advance lands, nothing changes, and the URL keeps the handle, so the
                # loop above never exits. Left alone it re-shoots that one frame up to
                # max_slides — a vision call per copy, every later slide unseen, and
                # nothing in the log to say the account was never actually read.
                if digest == last_digest:
                    if stall_tries < STALL_RECOVERY_ATTEMPTS:
                        stall_tries += 1
                        print(f"[scraper]   ⟳ Slide {slide_idx:02d} did not advance — "
                              f"retry {stall_tries}/{STALL_RECOVERY_ATTEMPTS}")
                        if not await _advance_slide(page, recovery=True):
                            break
                        continue
                    print(f"[scraper]   ✗ Stuck at slide {slide_idx:02d} after "
                          f"{stall_tries} retries — abandoning @{handle} with "
                          f"{slide_idx} slide(s) captured. A story this browser "
                          f"cannot render (usually video) is the usual cause.")
                    break

                last_digest = digest
                stall_tries = 0

                filename = f"{handle}_{slide_idx:02d}.png"
                out_path = scan_dir / filename

                try:
                    out_path.write_bytes(shot)
                except Exception as e:
                    print(f"[scraper]   ✗ Could not write {filename}: {e}")
                    break

                story_id = _story_id_from_url(page.url)
                print(f"[scraper]   ✓ slide {slide_idx:02d} saved  ({filename})")
                metadata["screenshots"].append({
                    "path":        str(out_path),
                    "filename":    filename,
                    "account":     handle,
                    "slide_index": slide_idx,
                    "story_id":    story_id,
                    "url":         page.url,
                    "timestamp":   datetime.now().isoformat(timespec="seconds"),
                })

                slide_idx += 1

                if not await _advance_slide(page):
                    break
            else:
                # Ran out of budget rather than out of stories. Saying so turns a
                # silently truncated account into something visible in the log.
                print(f"[scraper]   ⚠ Reached the {max_slides}-slide cap for "
                      f"@{handle} — it may have more stories than were captured.")

            metadata["accounts_checked"].append({
                "handle":          handle,
                "slides_captured": slide_idx,
            })

        await browser.close()

    # ── Write metadata index ──────────────────────────────────────────────────
    metadata["auth_failures"] = auth_failures
    meta_path = scan_dir / "scan_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    total_slides = len(metadata["screenshots"])
    if auth_failures:
        print(f"\n[scraper] ✗ Instagram rejected the session on {auth_failures} "
              f"of {len(accounts)} account(s) — the saved cookies are dead.")
        print("[scraper]   Refresh with:  python3 scripts/save_instagram_session.py")
    print(f"\n[scraper] ✓ Done — {total_slides} screenshots across "
          f"{len(accounts)} accounts → {scan_dir.name}/")
    return scan_dir


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Instagram stories via Playwright")
    parser.add_argument("--visible",  action="store_true", help="Show browser window")
    parser.add_argument("--account",  metavar="HANDLE",    help="Scrape a single account only")
    args = parser.parse_args()

    scan_dir = asyncio.run(scrape_stories(
        headless=not args.visible,
        handle_filter=args.account,
    ))
    print(f"\nScan directory: {scan_dir}")
