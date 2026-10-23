# Playwright's own image: Ubuntu 24.04 with Chromium and every system
# library it needs already present. Pinned to match the playwright version
# in requirements.txt — the Python package and the browser build have to
# agree, and this sidesteps the dependency problem that blocks Chromium on
# Amazon Linux.
FROM mcr.microsoft.com/playwright/python:v1.64.1-noble

# Tesseract is the OCR pre-filter deciding which slides are worth sending
# to Claude Vision. pytesseract is only a wrapper around this binary — if
# it is missing, has_readable_text() fails open and every single slide
# becomes a paid API call, with no error to tell you.
#
# The openssl upgrade is deliberate and separate from the base image: the
# Playwright image is current, but it ships 3.0.13-0ubuntu3.9 while
# noble-security carries 3.0.13-0ubuntu3.16, which closes two high-severity
# advisories. Only these packages are upgraded — a blanket `apt-get upgrade`
# would move the browser's own pinned dependencies out from under Playwright.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tesseract-ocr \
 && apt-get install -y --only-upgrade openssl libssl3t64 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# virtualenv arrives with the base image, nothing depends on it (pip reports
# an empty Required-by) and this container only ever runs one module, so it
# is dead weight carrying eight high-severity advisories. Dropping it is the
# difference between a scan that lists real work and one that lists noise.
RUN pip uninstall -y virtualenv

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
    RIDETRACKER_CSV_PATH=/tmp/master_rides.csv \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python3", "-m", "pipeline.run_scan"]
