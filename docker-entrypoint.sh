#!/usr/bin/env bash
# Bridges injected secrets to the shapes the pipeline expects.
#
# ECS delivers secrets as environment variables, but story_scraper.py reads
# the Instagram session from a file (that is also how it works on the
# laptop). Rather than fork the code for one deployment target, write the
# variable out to the path the code already looks at.
set -euo pipefail

if [ -n "${INSTAGRAM_COOKIES:-}" ]; then
    cookies_path="${RIDETRACKER_COOKIES_PATH:-/tmp/instagram_cookies.json}"
    # Written, then locked down: other processes in the container have no
    # business reading a live session.
    install -m 600 /dev/null "$cookies_path"
    printf '%s' "$INSTAGRAM_COOKIES" > "$cookies_path"

    # Fail now with a clear message rather than deep inside Playwright with
    # an opaque one. A truncated or mis-pasted SSM value is the likely cause.
    python3 -c "
import json, sys
p = '$cookies_path'
try:
    c = json.load(open(p))
except Exception as e:
    sys.exit(f'INSTAGRAM_COOKIES is not valid JSON: {e}')
if not any(x.get('name') == 'sessionid' for x in c):
    sys.exit('INSTAGRAM_COOKIES parsed, but contains no sessionid cookie.')
print(f'[entrypoint] Session loaded ({len(c)} cookies).')
"
    # Keep the secret out of the environment of everything downstream.
    unset INSTAGRAM_COOKIES
fi

exec "$@"
