# Playwright's own image: Ubuntu 24.04 with Chromium and every system
# library it needs already present. Pinned to match the playwright version
# in requirements.txt — the Python package and the browser build have to
# agree, and this sidesteps the dependency problem that blocks Chromium on
# Amazon Linux.
FROM mcr.microsoft.com/playwright/python:v1.60.0-noble

# Tesseract is the OCR pre-filter deciding which slides are worth sending
# to Claude Vision. pytesseract is only a wrapper around this binary — if
# it is missing, has_readable_text() fails open and every single slide
# becomes a paid API call, with no error to tell you.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tesseract-ocr \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Explicit allow-list. Credentials (secrets.env, instagram_cookies.json)
# are deliberately absent — they arrive at runtime from a secret store, so
# the image itself stays safe to push to a registry.
COPY pipeline/ ./pipeline/
COPY config/accounts.json config/known_locations.json ./config/
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# A container filesystem is not the laptop's: these point at writable,
# ephemeral locations. The state file is mounted or synced in; screenshots
# are scratch space discarded when the task exits.
ENV RIDETRACKER_SCREENSHOTS_DIR=/tmp/screenshots \
    RIDETRACKER_COOKIES_PATH=/tmp/instagram_cookies.json \
    RIDETRACKER_DB_PATH=/data/rides_database.json \
    RIDETRACKER_SCAN_BATCH_PATH=/tmp/scan_batch_latest.json \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python3", "-m", "pipeline.run_scan"]
