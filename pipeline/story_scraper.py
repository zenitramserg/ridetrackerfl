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
import json
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ACCOUNTS_PATH = BASE_DIR / "config" / "accounts.json"
COOKIES_PATH  = BASE_DIR / "config" / "instagram_cookies.json"
SCREENSHOTS_BASE = BASE_DIR / "data" / "screenshots"

# ── Story viewer layout (1280 × 900 viewport) ─────────────────────────────────
# Instagram's story panel sits roughly centered. These coordinates advance slides.
STORY_ADVANCE_X = 950   # right portion of story panel → next slide
STORY_ADVANCE_Y = 350
MAX_SLIDES_PER_ACCOUNT = 12   # safety cap; most accounts have 1-5 active stories
PAGE_LOAD_MS   = 3500         # wait after initial navigation (story must fully load)
SLIDE_LOAD_MS  = 2000         # wait between slides (video stories need a moment)


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


def _story_id_from_url(url: str) -> str:
    """Extract story ID from URL like /stories/handle/3862367330567642767/
    Strips query params and fragments before parsing so URLs like
    /stories/handle/123/?source=story_viewer still parse correctly.
    """
    url_path = url.split("?")[0].split("#")[0]
    parts = [p for p in url_path.rstrip("/").split("/") if p]
    if parts and parts[-1].isdigit():
        return parts[-1]
    return ""


def _url_path(url: str) -> str:
    """Return the path portion of a URL, stripping query string and fragment."""
    return url.split("?")[0].split("#")[0]


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

            # If Instagram redirected away, this account has no active stories
            if f"/stories/{handle}/" not in page.url:
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

            # ── Click through slides ──────────────────────────────────────────
            # Strategy: 2 screenshots per story card (start + mid), then advance.
            # 2 shots gives Vision two chances to read any text overlay (important
            # for video cards where text appears after the video starts playing).
            # We track story IDs to detect when we've truly moved to a new card.
            # If we can't advance after STUCK_LIMIT retries, the story has ended.

            SHOTS_PER_CARD  = 2      # screenshots per unique story card
            INTRA_CARD_MS   = 2500   # wait between shot 1 and shot 2 of same card
            STUCK_LIMIT     = 3      # max retries when advance click doesn't change card

            slide_idx   = 0
            stuck_count = 0
            seen_ids: set[str] = set()

            while slide_idx < max_slides:

                # Bail if Instagram has moved us off this account's stories
                if f"/stories/{handle}/" not in page.url:
                    print(f"[scraper]   → Story ended after {slide_idx} slides")
                    break

                current_story_id = _story_id_from_url(page.url) or _url_path(page.url)

                # If we're still on a card we've already shot, advance and retry.
                # After STUCK_LIMIT retries with no new card, the story has ended.
                if current_story_id in seen_ids:
                    stuck_count += 1
                    if stuck_count >= STUCK_LIMIT:
                        print(f"[scraper]   → Story ended after {slide_idx} slides")
                        break
                    try:
                        await page.mouse.click(STORY_ADVANCE_X, STORY_ADVANCE_Y)
                        await page.wait_for_timeout(SLIDE_LOAD_MS)
                    except Exception:
                        break
                    continue

                # New card — reset stuck counter and take SHOTS_PER_CARD screenshots
                stuck_count = 0
                seen_ids.add(current_story_id)

                for shot_num in range(SHOTS_PER_CARD):
                    if slide_idx >= max_slides:
                        break

                    # Wait between shots within the same card (lets video play a bit)
                    if shot_num > 0:
                        await page.wait_for_timeout(INTRA_CARD_MS)
                        # Stop if Instagram auto-advanced during our wait
                        if f"/stories/{handle}/" not in page.url:
                            break
                        new_id = _story_id_from_url(page.url) or _url_path(page.url)
                        if new_id != current_story_id:
                            break  # already moved on, don't screenshot old card again

                    filename = f"{handle}_{slide_idx:02d}.png"
                    out_path = scan_dir / filename
                    try:
                        await page.screenshot(path=str(out_path), full_page=False)
                        print(f"[scraper]   ✓ slide {slide_idx:02d} saved  ({filename})")
                        metadata["screenshots"].append({
                            "path":        str(out_path),
                            "filename":    filename,
                            "account":     handle,
                            "slide_index": slide_idx,
                            "story_id":    current_story_id,
                            "url":         page.url,
                            "timestamp":   datetime.now().isoformat(timespec="seconds"),
                        })
                        slide_idx += 1
                    except Exception as e:
                        print(f"[scraper]   ✗ Screenshot error: {e}")
                        break

                # Advance to next card
                try:
                    await page.mouse.click(STORY_ADVANCE_X, STORY_ADVANCE_Y)
                    await page.wait_for_timeout(SLIDE_LOAD_MS)
                except Exception as e:
                    print(f"[scraper]   ✗ Advance click failed: {e}")
                    break

            metadata["accounts_checked"].append({
                "handle":          handle,
                "slides_captured": slide_idx,
            })

        await browser.close()

    # ── Write metadata index ──────────────────────────────────────────────────
    meta_path = scan_dir / "scan_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    total_slides = len(metadata["screenshots"])
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
