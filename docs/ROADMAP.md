# RideTrackerFL Roadmap

_Last updated: 2026-11-12_

## Where things stand

- **Site** (ridetrackerfl.com, Netlify) is live and healthy: 26 rides, 19 organizers.
  `super-tanuki-783b85.netlify.app` serves the same site and is the one to test
  from the home network, whose filter blocks the apex domain.
- **Laptop sync** runs `pipeline.sync_to_site` from cron every 15 minutes while the Mac
  is awake. It pushes `public/rides.json` only when ride data actually changes
  (fix e4389aa, 2026-11-10).
- **Daily cloud scan**: ECS Fargate in us-east-2, scheduled 10:00 ET. Runs the `:latest`
  image in ECR `ridetrackerfl`. Sends a detailed email report (NEW / CHECK / REJECTED /
  RE-CONFIRMED, with Airtable links) through SNS.
- **Story limits**: omg_cycling, alexbicycles, teamrecoveryweston and mbo_tencycling are
  at 40 slides; all others at 24 (`config/accounts.json`).

## Recently done (Nov 2026)

- Detailed per-run email report (ec34dc2), deployed to AWS 2026-11-06.
- Netlify credit burn fixed: 271 of 335 deploys this cycle changed nothing except
  `generated_at`. Usage was at 75% of 3,000 credits; the cycle resets Nov 18.
- Slide limits raised for three accounts (6dff720).
- Data fixes: 5K run and the OMG ride in Colombia hidden, the Nov 5
  Team Recovery × GRIT ride fixed, and the Weston Flyers Thanksgiving ride's
  Instagram link corrected.

## Critical path: Android and iOS apps

Nothing has been built yet: no accounts, privacy page, wrapper project or store assets.

1. **Open the Google Play developer account** ($25, plus identity verification). *Owner: Sergio.* Longest wait.
2. **Privacy policy page** on the site. Required by both stores, because GA4 is live. *Owner: Claude.*
3. **Store assets**:
   - 1024×1024 icon.
   - Maskable icon crop check. All 9 icons currently declare `any maskable`.
   - `manifest.json` screenshots.
   - `.well-known/assetlinks.json`.

   *Owner: Claude.*
4. **Android TWA build** (Bubblewrap), uploaded to closed testing.
5. **12 testers opted in for 14 consecutive days.** This is Google's rule for new personal accounts. The clock starts at the closed-test upload. Recruit ride organizers and regulars early, because it overlaps Thanksgiving.
6. **Apply for Play production access**, then launch. Target: first or second week of December.

**iOS** runs alongside Android, off the critical path:
- Apple Developer account: $99/yr, 1–2 days to verify.
- Capacitor shell.
- **Ride-reminder push notifications.** This is a genuinely native feature, needed to clear App Store guideline 4.2.2, which rejects content aggregators.
- Budget for one rejection. Target: mid-to-late December.

**Open decision:** Android first (recommended), or both in parallel?

## Backlog

- **Data cleanup**, proposed but not yet approved:
  1. In `push_updated_rides`, clear `airtable_record_id` only on a 404, not on any
     error. Today a transient failure re-creates the record as a live duplicate.
  2. Re-link the S3 state DB to Airtable. 73 of 115 stored record IDs no longer exist.
     Show the re-link list before changing anything.
  3. Airtable touch-ups. Add "NO AUTO-RESTORE" to the notes of the 3 paused 5am Pull Club
     records and S.L.A.B. Pimpin, which stops the sync warning. Also fill in missing Ride
     Types and hide empty shell record recES0YkEq1lBggMM. No deletions.
- **Thirsty Thursday (MBO Ten Cycling)** starts at MBO Brickell, in Miami. Hide it or keep it?
- **Logos on the "Coming up" event cards**: optional; the cards currently show no logos.
- **Runs**: tracked but hidden today; the plan is to add them to the site later.
- **Auto-hiding runs and out-of-area rides**: undecided.
