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
            slide_idx = 0
            seen_story_ids: set[str] = set()   # deduplicate: 1 screenshot per card

            while slide_idx < max_slides:

                # Bail if Instagram has moved us off this account's stories
                if f"/stories/{handle}/" not in page.url:
                    print(f"[scraper]   → Story ended after {slide_idx} slides")
                    break

                current_story_id = _story_id_from_url(page.url)

                # Skip cards we've already captured (e.g. video-pause made us loop back)
                if current_story_id and current_story_id in seen_story_ids:
                    try:
                        await page.mouse.click(STORY_ADVANCE_X, STORY_ADVANCE_Y)
                        await page.wait_for_timeout(SLIDE_LOAD_MS)
                    except Exception:
                        break
                    continue

                if current_story_id:
                    seen_story_ids.add(current_story_id)

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
                except Exception as e:
                    print(f"[scraper]   ✗ Screenshot error: {e}")
                    break

                slide_idx += 1

                # Advance to next slide by clicking right portion of story panel.
                # For VIDEO cards, the first click sometimes only pauses the video
                # rather than advancing — so we verify the URL changed and retry once.
                try:
                    await page.mouse.click(STORY_ADVANCE_X, STORY_ADVANCE_Y)
                    await page.wait_for_timeout(SLIDE_LOAD_MS)

                    # If the story ID didn't change, we likely just paused a video.
                    # Click once more to actually advance to the next card.
                    new_story_id = _story_id_from_url(page.url)
                    if (current_story_id
                            and new_story_id
                            and new_story_id == current_story_id
                            and f"/stories/{handle}/" in page.url):
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
