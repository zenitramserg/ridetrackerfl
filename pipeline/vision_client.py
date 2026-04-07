"""
vision_client.py
Direct Claude API vision calls for ride extraction.

Replaces the in-context Claude vision analysis (which consumed chat tokens).
Each screenshot is sent as a one-shot API call — no conversation context needed.

Requires:
    pip install anthropic
    ANTHROPIC_API_KEY set in environment (or config/secrets.env)

Optional Ollama pre-filter (cuts ~60-70% of API calls for non-ride slides):
    pip install ollama
    ollama pull llama3.2-vision     # or: ollama pull llava
    Ollama running at localhost:11434
"""

import base64
import json
import os
import re
import time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

# Sonnet balances quality vs cost well. Switch to haiku to go faster/cheaper,
# opus for difficult or low-quality flyer images.
CLAUDE_MODEL  = "claude-sonnet-4-6"
OLLAMA_MODEL  = "llama3.2-vision"   # fallback: "llava"

# ── Extraction prompt ─────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are analyzing a screenshot of an Instagram story from a cycling account in the Weston, FL area.

Your job: determine if this story slide is announcing an upcoming group cycling ride, then extract the key details.

Output ONLY valid JSON with this exact structure — no markdown, no commentary:
{
  "is_ride_post": true or false,
  "title": "descriptive ride name or empty string",
  "ride_type": "weekly_ride | ride_event | unknown",
  "weekday": "Monday/Tuesday/Wednesday/Thursday/Friday/Saturday/Sunday or empty string",
  "date": "full date string like 'Saturday March 28, 2026' or empty string",
  "start_time": "HH:MM AM/PM format or empty string",
  "start_location": "venue or place name or empty string",
  "address_note": "street address if visible or empty string",
  "distance": "e.g. '30 mi' or '50 km' or empty string",
  "pace": "e.g. '20-22 mph' or '24+ mph' or empty string",
  "status": "confirmed | planned | canceled | unknown",
  "confidence": a number from 0.0 to 1.0,
  "raw_visible_text": "all text visible in the image, transcribed exactly",
  "image_description": "one sentence describing what the image shows"
}

Classification rules:
- is_ride_post = false for: product promotions, gear sales, performance activity stats,
  race results, general lifestyle content, team photos without ride details
- is_ride_post = true only when the slide announces a specific upcoming GROUP ride
  with at least a date/weekday OR a time
- A countdown timer (e.g. "PEDALS UP 16h 27m") counts as is_ride_post = true
  but set confidence below 0.65 if no location or distance is shown
- Extract EXACTLY what is visible — never infer or fabricate details not shown
- confidence = how certain you are this is a genuine upcoming group ride announcement
- Some accounts post in Spanish — translate day names to English (lunes=Monday, martes=Tuesday,
  miércoles=Wednesday, jueves=Thursday, viernes=Friday, sábado=Saturday, domingo=Sunday)
  and treat "rodada", "salida", "millas" as ride indicators"""


# ── Image encoding ────────────────────────────────────────────────────────────

def _b64(image_path: Path) -> str:
    with open(image_path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode("utf-8")


# ── Tesseract text pre-filter ─────────────────────────────────────────────────

# Minimum characters of readable text required to bother sending to Claude.
# Video frames and action shots typically return 0-5 chars.
# Ride flyers / story cards with text return 20-200+ chars.
MIN_TEXT_CHARS = 15

def has_readable_text(image_path: Path) -> bool:
    """
    Run fast local OCR (pytesseract) to check if the story content area
    contains meaningful text. Returns False for pure video frames / action
    shots, True for story cards, flyers, and text overlays.

    We crop out the Instagram UI chrome (top ~90px has account name,
    timestamp, buttons; bottom ~60px has progress bars) before OCR so that
    the UI itself doesn't make every frame look text-rich.

    This runs in ~50ms locally and costs nothing — skipping saves one
    Claude API call (~$0.003) per blank slide.
    """
    try:
        import pytesseract
        from PIL import Image
        img = Image.open(image_path)
        w, h = img.size

        # Crop to story content area only (strip top UI bar and bottom bar)
        TOP_CROP    = int(h * 0.10)   # ~90px on 900px viewport
        BOTTOM_CROP = int(h * 0.07)   # ~63px
        content = img.crop((0, TOP_CROP, w, h - BOTTOM_CROP))

        text = pytesseract.image_to_string(content, config="--psm 11")
        char_count = len(text.strip())
        return char_count >= MIN_TEXT_CHARS
    except Exception as e:
        # If OCR fails for any reason, fail open so Claude still sees it
        print(f"[vision]   ⚠ OCR check failed ({image_path.name}): {e}")
        return True


# ── Ollama pre-filter ─────────────────────────────────────────────────────────

def ollama_prefilter(image_path: Path) -> bool:
    """
    Quick local YES/NO: is this possibly a ride announcement?
    Returns True (pass to Claude) or False (skip).
    Fails open — if Ollama is unavailable, returns True so Claude still sees it.
    """
    try:
        import ollama
        with open(image_path, "rb") as f:
            image_bytes = f.read()

        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{
                "role": "user",
                "content": (
                    "Does this image show a cycling group ride announcement, event flyer, "
                    "or ride invitation with a specific upcoming date or scheduled time? "
                    "Reply with only YES or NO."
                ),
                "images": [image_bytes],
            }],
        )
        answer = response["message"]["content"].strip().upper()
        return answer.startswith("YES")

    except Exception as e:
        print(f"[vision] Ollama unavailable ({type(e).__name__}), skipping pre-filter")
        return True   # fail open


# ── Single image analysis ─────────────────────────────────────────────────────

def analyze_screenshot(
    image_path: Path,
    source_account: str,
    use_ollama: bool = False,
) -> dict | None:
    """
    Analyze a single story screenshot via the Claude API.

    Returns a ride extraction dict, or None if:
      - Ollama pre-filter rejected it
      - The API call failed
      - The response was unparseable JSON
    """
    import anthropic

    # Layer 1: OCR text check — free, ~50ms, no API cost
    # Skip slides with no readable text (video frames, action shots)
    if not has_readable_text(image_path):
        print(f"[vision]   ○ No text detected, skipping")
        return None

    # Layer 2: Optional Ollama pre-filter — free, local, ~1-2s
    if use_ollama:
        passed = ollama_prefilter(image_path)
        tag = "✓ Ollama pass" if passed else "○ Ollama filtered"
        print(f"[vision]   {tag}: {image_path.name}")
        if not passed:
            return None

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not set. Add it to your environment or to config/secrets.env"
        )

    client = anthropic.Anthropic(api_key=api_key)

    # Retry on 529 overloaded errors with exponential backoff
    MAX_RETRIES = 4
    RETRY_DELAYS = [5, 15, 30, 60]   # seconds between attempts

    message = None
    for attempt in range(MAX_RETRIES):
        try:
            message = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type":       "base64",
                                "media_type": "image/png",
                                "data":       _b64(image_path),
                            },
                        },
                        {
                            "type": "text",
                            "text": EXTRACTION_PROMPT,
                        },
                    ],
                }],
            )
            break   # success — exit retry loop
        except Exception as e:
            is_overloaded = "529" in str(e) or "overloaded" in str(e).lower()
            if is_overloaded and attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"[vision]   ⚠ API overloaded, retrying in {wait}s "
                      f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(wait)
            else:
                print(f"[vision]   ✗ API error ({image_path.name}): {e}")
                return None

    if message is None:
        return None

    if not message.content:
        print(f"[vision]   ✗ Empty response content ({image_path.name})")
        return None
    raw = message.content[0].text.strip()

    # Strip markdown code fences if model included them
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "",    raw)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[vision]   ✗ JSON parse error ({image_path.name}): {e}")
        print(f"[vision]     Raw: {raw[:200]}")
        return None

    result["source_account"]    = source_account
    result["screenshot_path"]   = str(image_path)
    return result


# ── Batch: full scan directory ────────────────────────────────────────────────

def analyze_scan_directory(
    scan_dir: Path,
    use_ollama: bool = False,
) -> list[dict]:
    """
    Process every screenshot in a scan directory (produced by story_scraper).

    Reads scan_metadata.json to know which screenshots belong to which account,
    then calls analyze_screenshot() on each one.

    Returns a list of ride extraction dicts where is_ride_post=True.
    Non-ride slides are silently dropped.
    """
    meta_path = scan_dir / "scan_metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"No scan_metadata.json found in {scan_dir}")

    with open(meta_path, encoding="utf-8") as f:
        metadata = json.load(f)

    screenshots = metadata.get("screenshots", [])
    print(f"[vision] Analyzing {len(screenshots)} screenshots"
          f" (Ollama pre-filter: {'on' if use_ollama else 'off'})...")

    results = []
    ride_count = 0

    for shot in screenshots:
        img_path = Path(shot["path"])
        account  = shot["account"]

        if not img_path.exists():
            print(f"[vision] ✗ Missing file: {img_path.name}")
            continue

        print(f"[vision] → {img_path.name}  (@{account})")
        result = analyze_screenshot(img_path, account, use_ollama=use_ollama)

        if result is None:
            continue

        # Attach story metadata from scraper
        result.setdefault("story_id",    shot.get("story_id", ""))
        result.setdefault("slide_index", shot.get("slide_index", 0))

        if result.get("is_ride_post"):
            ride_count += 1
            conf  = result.get("confidence", "?")
            title = result.get("title", "(no title)")
            print(f"[vision]   ✓ RIDE: {title}  (conf={conf})")
            results.append(result)
        else:
            print(f"[vision]   ○ Not a ride post")

    print(f"\n[vision] Done — {ride_count} ride posts found in {len(screenshots)} screenshots.")
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="Run vision extraction on a scan directory")
    parser.add_argument("scan_dir", help="Path to scan directory (output of story_scraper)")
    parser.add_argument("--use-ollama", action="store_true", help="Enable Ollama pre-filter")
    args = parser.parse_args()

    scan_dir = Path(args.scan_dir)
    if not scan_dir.exists():
        print(f"✗ Directory not found: {scan_dir}")
        sys.exit(1)

    rides = analyze_scan_directory(scan_dir, use_ollama=args.use_ollama)
    print(json.dumps(rides, indent=2, ensure_ascii=False))
