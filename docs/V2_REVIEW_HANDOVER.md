# Site v2 redesign: review handover

**Status:** built and tested locally. **NOT deployed. Do not commit, push or rename files.** This is a review only.
**Review done 2026-10-24 — see §8. S1/S3/S4/S8 were APPLIED on 2026-10-25 (see §11); the deploy blocker is cleared.**
**S5–S7 remain open questions for Sergio, and the S2 deploy-order change still has to be honoured.**
**Date:** 2026-10-24
**Reviewer ask:** check the changes below for bugs, regressions against the live site, and anything that could break production. Report findings; do not fix without asking Sergio.

---

## 1. Why

The live `index.html` uses one big card per ride, which gets messy as rides per day grow, and some rides appear several times on the page. v2 is a redesign focused on looking professional and scaling to more rides **without adding new data points**. Sergio approved 16 changes (listed in section 3).

## 2. Files changed (working tree, uncommitted)

| File | Change |
|---|---|
| `index-v2.html` (new) | Full redesigned page. Will replace `index.html` at deploy. Self-contained (inline CSS/JS). |
| `pipeline/site_normalize.py` (new) | Builds display-ready fields per ride; cleans Notes. |
| `pipeline/sync_to_site.py` | Imports `normalize_ride` and runs it on every ride before writing `rides.json`; adds `_make_thumb()` (96px WebP organizer logos) and an `Avatar[0].thumb` key. |
| `service-worker.js` | Cache bumped v7 → v8; new **Network First** branch for HTML navigations (previously Cache First, which pinned users to old page versions). |
| `.gitignore` | Allows `public/organizers/thumbs/*.webp`. |
| `icons/og-image.png` (new) | 1200×630 social/link preview image. |
| `public/organizers/thumbs/*.webp` | Generated thumbnails (~38 KB total vs ~2.6 MB originals). |

Run `git diff` and `git status` to see exact changes.

### ⚠ Already partly live
A local sync job on Sergio's Mac (it commits `chore: sync rides.json [...]` roughly every 15 min) ran the **modified** `sync_to_site.py` at 19:15 and pushed `rides.json` (commit `fa35fec`). So production `rides.json` already has:
- a new `site` object on every ride record (additive)
- Notes stripped of the scraper's image description
- `Avatar[0].thumb` paths on organizers
- the thumbnail files themselves (`public/organizers/thumbs/*.webp`, pushed in commit `c136438` at 19:30 once `.gitignore` allowed them)

The current live `index.html` ignores `site` and `thumb`. **Please confirm nothing in the live `index.html` breaks** with this data (e.g. it reads `Notes`, `Pace`, `Distance`, `Address Note`; Notes may now be absent or shorter, and Pace/Distance/Address Note may now be filled from Notes).

Also note: the ECS Fargate scraper image does NOT have `site_normalize.py` yet. Until the image is rebuilt, a cloud-run sync will produce `rides.json` without `site`. v2 handles both (see 4.1).

## 3. The 16 changes

1. Display cleanup moved into the sync (`site` object): time, pace label/groups, distance, venue, venue detail, area, short display name
2. Scraper image description removed from public Notes; `Address:/Pace:/Distance:` lines in Notes used as fallbacks for empty fields
3. GA4 + email signup carried over (GA skipped on localhost/LAN)
4. Service worker: Network First for pages + cache bump
5. Share button per ride (Web Share API → clipboard fallback), link `/?ride=<recordId>`
6. Day links: `?day=today|tomorrow|mon..sun|week|events`
7. Same-day cancellation banner (canceled ride starting within 24h replaces the next-ride banner)
8. Stale-data warning when `generated_at` is older than 24h
9. Small WebP logos (with `onerror` fallback to the full avatar)
10. Short ride names (organizer prefix/suffix removed only when joined by a separator: `-`, `–`, `—`, `:`, `|`)
11. Today's past rides collapsed into "N earlier rides today" (a ride is "past" 90 min after start)
12. Link preview image + og/twitter meta
13. Consistent logo circles
14. Dark mode (`prefers-color-scheme`)
15. Swipe left/right on the ride list to change day (horizontal-dominant, ≥70px, <600ms)
16. Pace filter remembered in localStorage (`rt_pace`, wrapped in try/catch)

Plus the earlier v2 base: compact rows, sticky day strip (Week / 7 days / Later), time blocks (Early < 6:30 / Morning / Afternoon & evening, shown only when a day has ≥4 rides), filter chips (pace groups, Weston only, organizer), each ride listed once, "Coming up" strip for events beyond 7 days, next-ride countdown banner.

## 4. How it works (what to check)

### 4.1 Data path
- Page fetches `/public/rides.json` (`cache: 'no-store'`).
- `siteFields(r)` uses `r.site` if present, otherwise computes the same values in JS. **The JS fallback must mirror `site_normalize.py`.** Please diff the logic: `parseTime`, `paceInfo`, `fmtDist`, `splitLocation`, `areaOf`, `displayName`, `cleanNotes`.
- Pace groups: fast ≥21 mph, mod 17–21, soc <17; letters A→fast, B→mod, C/D→soc; "no drop / recovery / chill / social / controlled" adds soc. Pill colour = fastest group.

### 4.2 Date logic (`buildItems`) — compare with live `index.html`
Live rules: `shouldShowRide`, `hasPrimaryDuplicate`, `hasPassedMidnight`, `rideDateFor`, `weekDates` (around lines 2040–2110 and 1259–1294).
v2 rules:
- Skip `Display on Site === false` and status `hidden`/`past`.
- Weekly: use `Ride Date` if it falls within today..today+6, otherwise the next occurrence of `Day of Week`.
- Non-weekly: `Ride Date` (or next weekday if missing); dropped if before today.
- 7-day window; non-weekly rides beyond it go to "Later".
- Dedup key: organizer + date + start minutes. Keep `Is Primary Listing`, else latest `Ride Date`, else lowest record id. **This differs from live** (live keys on Day of Week + raw Time string and falls back to the lowest id). Check whether this can hide a legitimate ride or let a duplicate through.

### 4.3 Things I'm least sure about
- `hasPrimaryDuplicate` parity (above).
- Timezone: all date math uses the browser's local time, same as live. Visitors outside ET would see shifted "today". Same as before, but worth a note.
- Swipe handler on `#main` with `touch-action: pan-y`: make sure it can't fight vertical scroll on iOS Safari.
- Service worker HTML Network First: confirm offline still works and that `/` vs `/index.html` caching behaves.
- `sync_to_site.py` `_make_thumb`: Pillow is in `requirements.txt`; confirm the Docker image installs it and that `THUMBS_PATH` writes are fine in the ECS container (no `.git` there; it pushes via the GitHub API and only sends `rides.json`, so thumbs won't reach the repo from ECS; the page falls back to the full avatar).
- `display_name` regex escaping for organizer names with special characters (apostrophes, e.g. "Alex's Bicycle Pro Shop").

## 5. How to test locally

```bash
cd ~/ridetrackerfl
python3 -m http.server 8765          # if "address already in use", one is already running
open "http://localhost:8765/index-v2.html"
open "http://localhost:8765/index-v2.html?day=sun"
open "http://localhost:8765/index-v2.html?day=events"

# Python normalizer against the current data
python3 -m pipeline.site_normalize public/rides.json

# syntax checks
python3 -c "import ast; ast.parse(open('pipeline/sync_to_site.py').read())"
node -e "new Function(require('fs').readFileSync('service-worker.js','utf8'))"
```

**Do not** run `python3 -m pipeline.sync_to_site` without `--no-push`: it writes to Airtable (expiry/weather housekeeping) and pushes to GitHub, which triggers a Netlify deploy.

Already verified in headless Chromium (390px and 1280px, light and dark): swipe next/prev, pace persistence after reload, `?ride=` opens and expands the ride, share copies text + URL, cancellation banner, stale banner, no console errors.

## 6. Planned deploy (only after review + Sergio's go)

1. `cp index.html index-backup-2026-10-24.html` (local only; `*-backup-*.html` is gitignored)
2. `mv index-v2.html index.html`
3. One commit: `index.html`, `service-worker.js`, `.gitignore`, `pipeline/site_normalize.py`, `pipeline/sync_to_site.py`, `icons/og-image.png`, `public/organizers/thumbs/`
4. Push → a single Netlify deploy (credits are limited, so avoid extra pushes or preview branches)
5. Later: rebuild and push the ECS scraper image so cloud syncs include `site_normalize.py`

Rollback: restore `index.html` from the backup (or `git revert`) and push.

## 7. Constraints (from project notes)
- `rides.json` is the site's only data source; the browser never calls Airtable.
- Keep the scraper untouched.
- Sergio does not want more data points on the site; the review should be about correctness and robustness, not new features.

---

# 8. Review findings (2026-10-24)

Reviewed against the working tree plus the **live** `public/rides.json`. Parity was checked by
running the page's own JS verbatim through JavaScriptCore (`osascript -l JavaScript`; no `node`
on this machine) over **raw, pre-normalizer** data recovered from commit `320d728` — i.e. exactly
what an ECS run produces. Both visibility algorithms were reimplemented from the two sources and
diffed on live data and on synthetic edge cases. Nothing was changed, committed or pushed, and
`sync_to_site` was never run.

**Verdict: S1 blocks the deploy.** S2 is a sequencing change. S3/S4/S8 are one-liners worth taking
in the same commit. S5–S7 are behaviour changes that need Sergio's intent, not fixes.

## S1 · HIGH — the image-description stripper fails, and v2 publishes the description

`pipeline/site_normalize.py:163` · `index-v2.html:417`

The separator pattern `\n-{3,}\s*(?:\n|$)` requires a newline **before** the dashes. When the
vision model extracts no `Address:`/`Pace:`/`Distance:` lines, Notes *begins* with `---`, nothing
matches, and the whole field survives. The follow-up loop only skips the dashes line itself; the
description is on the next line and passes through into published Notes.

Live in production now — `Horrible Hundred 2026 - MBO Cycling Trip` (`rec3niCJ1vxSMKFDx`),
committed in `fa35fec`:

    raw:        '---\nA promotional flyer for the Horrible Hundred 2026 cycling event in
                 Clermont, Florida, ... $200 USD per person, covering transportation,
                 accommodation, and breakfasts...'
    normalized: 'A promotional flyer for the Horrible Hundred 2026 cycling event in ...'

Severity comes from the live/v2 difference: **the live page never renders Notes** (`notes:` is
declared at `index.html:1217`, but nothing reads it and it is absent from the fetch list at
`index.html:2168`), while **v2 renders it** at `index-v2.html:613` in `.d-notes`. Deploying v2
therefore publishes internal text, including the trip's pricing, for the first time. The JS
fallback carries the identical regex, so it does not save you.

    # pipeline/site_normalize.py:163
    - head = re.split(r"\n-{3,}\s*(?:\n|$)", str(notes))[0]
    + head = re.split(r"(?m)^\s*-{3,}\s*$", str(notes))[0]

    // index-v2.html:417
    - const head = String(f['Notes'] || '').split(/\n-{3,}\s*(?:\n|$)/)[0];
    + const head = String(f['Notes'] || '').split(/^\s*-{3,}\s*$/m)[0];

Behaviour on normal records is unchanged (only a trailing newline in `head`, which the line loop
discards). Note this is **prevention, not cleanup**: `rides.json` keeps the leaked text until the
next sync, and git history keeps it permanently.

## S2 · MEDIUM — the planned deploy order republishes raw Notes daily

§6 defers the ECS image rebuild to step 5. Between the site deploy and that rebuild, the 10:00 ET
cloud run uses the **old** image and therefore the old `sync_to_site.py`, pushing a `rides.json`
with no `site`, uncleaned Notes and no `thumb`. Three consequences: every image description returns
to the public file and to git for a day; v2 falls back to the JS normalizer and so exposes S3; and
avatars revert to ~2.6 MB instead of 38 KB.

**Fix:** rebuild and push the ECS image in the same change window. No Dockerfile change is needed —
`Dockerfile:24` already does `COPY pipeline/ ./pipeline/`, so `site_normalize.py` ships
automatically.

## S3 · MEDIUM — JS fallback duplicates the address in `venue_detail`

`index-v2.html:433` vs `pipeline/site_normalize.py:189`. Python checks containment before
appending the address; the JS joins unconditionally. Across 25 records x 11 fields this was the
**only** mismatch:

    OMG Sunday Chill Ride   py='Behind Bank of America'
                            js='Behind Bank of America · Behind Bank of America'

Latent while `site` is present — and triggered precisely by the S2 window.

    // index-v2.html:433
    - venue_detail: [detail, c.addr].filter(Boolean).join(' · '),
    + venue_detail: (c.addr && !detail.toLowerCase().includes(c.addr.toLowerCase()))
    +   ? [detail, c.addr].filter(Boolean).join(' · ') : detail,

## S4 · MEDIUM — a weekly ride with a malformed `Ride Date` disappears in v2

`index-v2.html:467-470`. `new Date('2026-10-27T00:00:00.000Z' + 'T00:00:00')` is an Invalid Date.
Both `NaN < today` and `NaN >= winEnd` are false, so the weekly re-anchor on line 468 never fires,
and line 470's `isNaN(date)` drops the ride. Live's `rideDateFor` (`index.html:1259-1278`) falls
through to the weekday slot and still shows it. Verified: live shows 10-27, v2 hides.

Latent today (live data is 23 x `YYYY-MM-DD` + 2 missing); it fires if the Airtable field ever
becomes a dateTime.

    // index-v2.html:467
      let date = f['Ride Date'] ? new Date(f['Ride Date'] + 'T00:00:00') : null;
    + if (date && isNaN(date)) date = null;   // unparseable == absent, so the fallbacks below run

## S5–S7 · LOW — behaviour changes vs live, all verified, none active on today's data

Live and v2 show the **same 25 rides** on current production data. These are latent differences:

| | where | live | v2 |
|---|---|---|---|
| **S5** duplicate pair, neither primary | `index-v2.html:487` vs `index.html:2084` | keeps lowest record id | keeps latest `Ride Date` |
| **S5b** same org/date, `"6:00 AM"` vs `"6 AM"` | `index-v2.html:486` | shows **both** | collapses to one |
| **S6** one-off, future date, no `Day of Week` | `index.html:2097`, `:2194` | hidden | shown |
| **S7** weekly, `Ride Date` 3 weeks out | `index-v2.html:468` | shown on the far date | re-anchored to this week |

S5b and S6 look like v2 fixing live bugs. S5 changes *which* record's name/notes/pace a visitor
sees in the same slot. S7 will show a weekly ride this week even when a distant date was set
deliberately — if that is unwanted, re-anchor only when the stored date is in the past.

## S8–S10 · LOW / INFO

- **S8** `service-worker.js:73` — if both `caches.match(request)` and `caches.match('/index.html')`
  miss, this resolves `undefined`, and `respondWith(undefined)` throws, replacing the offline page
  with the browser's error page. Normally safe: `STATIC_ASSETS` (`:5-11`) holds both `/` and
  `/index.html` and `addAll` is atomic, so it only bites after storage-pressure eviction.
  Fix: append `|| new Response('<h1>Offline</h1>', {headers:{'Content-Type':'text/html'}})`.
- **S9** the new HTML branch caches navigations keyed by full URL, so `/?ride=recX` and `/?day=sun`
  each become separate RUNTIME_CACHE entries. Unbounded though tiny; could normalise to
  `/index.html`.
- **S10** `_make_thumb` (`sync_to_site.py:170-199`) regenerates all 15 thumbs on every cloud sync —
  avatars are re-downloaded so the mtime guard never hits — and the API push only sends
  `rides.json`, so they can never reach the repo. Harmless; `avatarHtml` (`index-v2.html:578`) has
  the `onerror` fallback. A new organizer added by a cloud-only run will 404 once before falling
  back.

## Checked and clean

- **The live page is safe with the new data.** `site` and `thumb` are additive and ignored; Notes is
  never rendered. The normalizer newly filled 7 fields across 6 rides (6 x `Address Note`,
  1 x `Pace` -> `'22/24 mph'`), all sane, all replacing a `—`. Live has no pace filter, so no
  filter behaviour changes.
- **Parity:** 25 records x 11 fields -> one mismatch (S3). The `Notes`/`Pace`/`Distance`/
  `Address Note` fallbacks matched exactly, 0 mismatches.
- **Visible sets identical** on live data: 25 live, 25 v2, no difference either way.
- **Swipe is safe on iOS** — `touch-action: pan-y` (`:145`), both listeners `passive: true`, never
  calls `preventDefault`, requires horizontal dominance >=1.8x. It cannot block vertical scroll.
- **Thumbnails:** 15 on disk, every `thumb` reference resolves. `Image.LANCZOS` is still valid in
  Pillow 13.0.0 (the laptop was upgraded to pillow 13 / certifi 2026.10.19 on 2026-10-23).
- **Service worker:** the HTML branch does not shadow `rides.json` (fetch sends `accept: */*` and
  mode is not `navigate`); activate deletes the v7 caches.

---

# 9. New information from Sergio (2026-10-24)

**The Horrible Hundred record is not a ride.** It is a post promoting a group trip to Orlando/
Clermont to *attend* an event — "Departure from Miami, 4:00 PM" is the coach departure, not a ride
start. It should not be listed at all.

This gives a faster fix than code for the S1 instance. `sync_to_site.py:461` already fetches with
`AND({Display on Site}=TRUE(), NOT(OR({Status}="hidden", {Status}="past")))`, so **unchecking
Display on Site on `rec3niCJ1vxSMKFDx`** drops the record out of `rides.json` entirely — removing
both the wrong listing and the leaked description from the published file on the next 15-minute
sync, with no deploy.

It is durable: `airtable_writer.py:480` sets `display_on_site=True` **only when creating** a record,
and `push_updated_rides` excludes it from `UPDATABLE_FIELDS` and says so at `:519` ("Never touches
display_on_site or is_primary_listing"). The scraper has matched this record in place on both later
runs. Residual: it stays in `rides_database.json` (118 entries) and is re-synced each run, which is
harmless while Airtable hides it; the only way it returns is if dedup fails to match and creates a
new record, which would arrive with `Display on Site = True`.

**This does not retire S1.** Hiding one record fixes today. The regex will leak the next flyer whose
Notes start with `---`, and that is the common shape, not the rare one.

## Follow-up (not part of v2): the gate cannot tell a ride from a trip to a ride

This record passed validation **legitimately** — it had every required field: Time `4:00 PM`,
Location `Departure from Miami`, Ride Date `2026-11-15`, confidence `0.82`. So this is a
classification gap, not a validation one. `is_promo_post` only matches retail words (`discount`,
`sale`, `% off`, `shop now`…); nothing covers a travel package.

A targeted rule would key on travel-package signals rather than the word "trip": a per-person price
**combined with** lodging/transport words (`accommodation`, `hotel`, `breakfast`, `transportation`,
`bus`, `van`, `package`), requiring two or more signals before rejecting. The conjunction matters —
a real gran fondo or century with a registration fee must still list. Writing it precisely needs the
raw flyer text from `state/rides_database.json` in S3, which needs an AWS login.

# 10. Open decisions for Sergio

1. ~~Hide `rec3niCJ1vxSMKFDx`~~ — **done 2026-10-25**, see §11.
2. ~~Apply S1, S3, S4, S8~~ — **done 2026-10-25**, see §11.
3. S5–S7: confirm intent, or leave as-is. **Still open.**
4. Rebuild the ECS image in the same window as the site deploy (S2) rather than "later".
5. ~~Draft the travel-package classifier rule~~ — **applied 2026-10-25**, see §12.

---

# 11. Applied 2026-10-25

Sergio approved the fixes. All four code changes are in the **working tree, uncommitted** — they
still belong in the single deploy commit described in §6.

| | change | files |
|---|---|---|
| **S1** | separator pattern anchored per line (`(?m)^\s*-{3,}\s*$` / `/^\s*-{3,}\s*$/m`) | `pipeline/site_normalize.py:163`, `index-v2.html:417` |
| **S3** | JS mirrors the Python containment check before appending the address | `index-v2.html:433` |
| **S4** | `if (date && isNaN(date)) date = null;` — unparseable date now behaves like a missing one | `index-v2.html:467` |
| **S8** | offline navigation falls back to a real `Response` instead of `undefined` | `service-worker.js:73` |

Each change carries a comment explaining why, so the next reader does not re-break it. The two S1
sites must stay in sync; a comment on the JS side points at `site_normalize.clean_notes`.

## Verification

- **S1 against real data.** All 8 records in `320d728` that carried an image description now come
  back clean; the Horrible Hundred record's Notes is removed entirely rather than published. (One
  record, `OMG Girls 5th Edition`, keeps its Notes and should: it is hand-written ride info — paid
  event, price, registration, sponsors — with no separator and no image description.)
- **Parity re-run.** The patched JS was extracted and run verbatim through JavaScriptCore over the
  raw `320d728` data: **25 records x 12 fields -> 0 mismatches** (was 1, the S3 bug). The Notes
  fallback is included in that count.
- **S4 against the patched code.** The real `buildItems` was assembled with its constants and run
  on the edge cases. `Ride Date: 2026-10-27T00:00:00.000Z` now resolves to 2026-10-27 and shows,
  matching live, instead of being dropped. Cases B/C/D are unchanged — they are S5b/S6/S7.
- **Live data end to end.** Patched `buildItems` over the current `rides.json`: 24 records in,
  **24 shown, 0 duplicates**, nothing silently dropped.
- **Syntax.** `ast.parse` on both Python files; `new Function(src)` on `service-worker.js`.

## Record hidden

`rec3niCJ1vxSMKFDx` (`Horrible Hundred 2026 - MBO Cycling Trip`) — **Display on Site unchecked**
via the Airtable API. Confirmed by reading the record back: the checkbox is now absent from the
response, which is how Airtable represents unchecked.

Its Airtable `Notes` still holds the original `---`-prefixed text. That is intentional: the field is
internal, the record is now excluded by the fetch filter at `sync_to_site.py:461`, and even if it
were re-enabled the patched `clean_notes` strips the description.

## What happens without any further action

The laptop cron runs `sync_to_site` **from the working tree**, so the S1 fix is already live for it.
At the next 15-minute sync the published `rides.json` goes from 24 records to 23, the Horrible
Hundred listing disappears, and the leaked description leaves the published file — no deploy needed.
That sync pushes `rides.json`, which costs **one Netlify deploy**.

Note the cron only ever commits `public/rides.json` and `public/organizers/`, so none of the four
code changes above reach production through it. `index-v2.html`, `service-worker.js` and
`site_normalize.py` still ship only via the §6 deploy.

## Still outstanding

- **S2** — rebuild and push the ECS image in the same window as the site deploy, not "later".
  Until that happens a cloud run still produces `rides.json` with no `site` and uncleaned Notes.
- **S5–S7** — behaviour differences vs live; need Sergio's intent, not fixes.
- **S9/S10** — informational, no action proposed.
- ~~The travel-package classifier rule~~ — applied, see §12. It needs the ECS image rebuild to
  take effect in the cloud, which is the same rebuild S2 asks for.

---

# 12. Travel-package rule, applied 2026-10-25

**This touches the scraper**, which §7 asks to leave alone. Sergio approved it explicitly after the
Horrible Hundred record turned out not to be a ride. Uncommitted, in the working tree.

| file | change |
|---|---|
| `config/known_locations.json` | new `travel_package_patterns` list, beside `promo_patterns` |
| `pipeline/classifier.py` | `TRAVEL_PACKAGE_PATTERNS`, `is_travel_package()`, and a branch in `rejection_reason()` returning `"travel package (trip to an event, not a ride)"` |

    "travel_package_patterns": [
      "nights stay", "night stay", "nights of accommodation",
      "accommodation", "lodging"
    ]

## Why these patterns and not the obvious ones

The first rule drafted keyed on price, "per person", "limited spots" and shuttle wording. Measured
against the 119 stored rides it flagged **four** records — the Horrible Hundred *plus* `Coast to
Coast Ride` (x2) and `Miami to Key West Ride`. Those three are real rides: Miami→Key West is 165
miles at 20-22 mph with a 4:45 AM rolling time, and its "$60 shuttle back to Miami (per person)" is
simply the way home. That rule would have hidden three genuine rides to catch one fake.

What actually separates them is an **overnight stay**, which a single-day group ride has no reason
to mention. Deliberately excluded:

- **`register separately`** — Sergio flagged it, correctly. A local ride uses it to mean
  "individually, not as a team" (`"Each rider must register separately, no team entries"`). It was
  also redundant: the Horrible Hundred already trips on `nights of accommodation`.
- **`hotel`** — a local ride can legitimately meet at one (`"Meet at the hotel parking lot on
  Weston Road"`).
- **price / `per person` / `limited spots` / shuttle** — all carried by the genuine long rides above.

Two near-misses worth not repeating: "has no pace or distance" is **not** a discriminator
(`Coast to Coast` has both empty and is real; `Clermont Weekend Experience` has `20-22 mph` and
`100 miles` and is a trip), and a multi-day date range is too broad (it also flags
`17th Annual Chain of Lakes Cycling Classic` and `Ocala Escape`, which are real events people ride).

## Verification

Run with the real classifier — not a reimplementation — over all 119 stored rides:

- **Flagged: 2 / 119.** `Clermont Weekend Experience` (via `nights stay`) and
  `Horrible Hundred 2026 - MBO Cycling Trip` (via `nights of accommodation`, `accommodation`).
- **Gate verdict changes across the corpus: those 2 and nothing else.**
- **Control group all pass:** Coast to Coast (x2), Miami to Key West, OMG Girls 5th Edition,
  Chain of Lakes, Ocala Escape, the Palomos rides.
- **Other gate branches unchanged:** complete ride valid; missing location, `"empty string"`
  sentinel, low confidence and shop promo each still rejected with their own reason.
- **The case Sergio raised passes:** `PAID EVENT $80, REGISTRATION REQUIRED, riders must register
  separately` -> **valid**.

## Note on the extraction itself

The Horrible Hundred flyer reads `DEPARTURE FROM MIAMI FRIDAY, NOV 13 AT 4:00 PM` and
`RETURN TO MIAMI SUNDAY, NOV 15 AT 4:00 PM`. The vision model stored `4:00 PM` on
`Sunday November 15` — i.e. the **return** coach. So the record was wrong twice over: wrong category
and wrong departure. The rule above rejects it before that matters, but it is a reminder that a
"start time" extracted from a multi-day flyer is not trustworthy.

## Takes effect

Locally at once (the laptop cron and any manual scan run from the working tree). In the cloud only
after the ECS image is rebuilt — the same rebuild S2 already requires. Because `rejection_reason()`
now prints itself, a rejected trip post will appear in the scan summary by name, as
`travel package (trip to an event, not a ride)`, instead of a silent count.
