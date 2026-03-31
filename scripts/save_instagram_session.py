"""
save_instagram_session.py — One-time Instagram authentication setup.

Opens a real browser window, lets you log in to Instagram manually,
then saves the session cookies to config/instagram_cookies.json.

The automated scraper (pipeline/story_scraper.py) will use these cookies
on every run so you never need to log in again — until Instagram expires the
session (typically 3–6 months).

Usage:
    python scripts/save_instagram_session.py

Run this:
  - Once on initial setup
  - If the scraper starts seeing login pages instead of stories
"""

import asyncio
import json
from pathlib import Path

COOKIES_PATH = Path(__file__).parent.parent / "config" / "instagram_cookies.json"


async def save_session():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("✗ Playwright not installed. Run:  pip install playwright && playwright install chromium")
        return

    print("\n─────────────────────────────────────────────────")
    print("  RideTrackerFL — Instagram Session Setup")
    print("─────────────────────────────────────────────────")
    print("A browser window will open at instagram.com.")
    print("Log in to your Instagram account, then come back")
    print("here and press ENTER to save the session.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        await page.goto("https://www.instagram.com/")

        input("✓ Log in, then press ENTER here to save your session cookies... ")

        # Verify we're actually logged in
        current_url = page.url
        if "accounts/login" in current_url:
            print("\n✗ Still on login page — please log in first, then press ENTER.")
            input("Press ENTER when logged in... ")

        cookies = await context.cookies()
        ig_cookies = [c for c in cookies if "instagram.com" in c.get("domain", "")]

        if not ig_cookies:
            print("✗ No Instagram cookies found. Make sure you're logged in.")
            await browser.close()
            return

        COOKIES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(COOKIES_PATH, "w", encoding="utf-8") as f:
            json.dump(ig_cookies, f, indent=2)

        print(f"\n✓ Saved {len(ig_cookies)} cookies → {COOKIES_PATH}")
        print("  The scraper is now ready to run.\n")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(save_session())
