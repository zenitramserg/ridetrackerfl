# RideTrackerFL Roadmap

_Last updated: 2026-11-20_

## Where things stand

- **Site** (ridetrackerfl.com, Netlify) is live and healthy: 26 rides, 19 organizers.
  `super-tanuki-783b85.netlify.app` serves the same site and is the one to test
  from the home network, whose filter blocks the apex domain.
- **Laptop sync** runs `pipeline.sync_to_site` from cron hourly, 5 AM–10 PM, while the
  Mac is awake. It pushes `public/rides.json` only when ride data actually changes
  (e4389aa), and makes about one Airtable request per run (86c7008). The Airtable free
  plan allows about 1,000 requests a month, and the 15-minute sync hit that limit on
  2026-11-15.
- **Daily cloud scan**: ECS Fargate in us-east-2, scheduled 10:00 ET. Runs the `:latest`
  image in ECR `ridetrackerfl`. Sends a detailed email report (NEW / CHECK / REJECTED /
  RE-CONFIRMED, with Airtable links) through SNS.
- **Story limits**: omg_cycling is at 60 slides; alexbicycles, teamrecoveryweston and
  mbo_tencycling at 40; all others at 24 (`config/accounts.json`).

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

## Costs (as of 2026-11-16)

Tracked in Airtable, in the **Costs** table of the RideTrackerFL base. Update it whenever a plan changes.

| Service | Per month | Notes |
|---|---|---|
| Airtable Team | $24 | Upgraded from Free on Nov 15 after the API request limit was hit. Next charge about Dec 15 (estimated) |
| Netlify Pro | $20 | 3,000 credits/month; 15 credits per deploy. Cycle resets on the 18th |
| Claude Pro | $20 | Dev sessions |
| ChatGPT Plus | $20 | Image creation. Price is the list rate, not yet checked against a receipt |
| Claude API (scanning) | ~$10 | Usage-based, prepaid credits |
| Google Workspace | $9 | hello@ridetrackerfl.com |
| Domain | ~$1.67 | ridetrackerfl.com; renews Mar 24 2027 |
| AWS | $0 | About $0.50/month, covered by free-plan credits ($99.35 left). Plan ends Mar 13 2027 |
| **Total** | **≈ $105** | **≈ $1,260/year** |

Savings to revisit:
- **Airtable.** Downgrading to Free after a month of watching request usage saves $288/year. It needs Ride History archived to stay under 1,000 records per base.
- **AWS.** Decide on the paid plan before Mar 13 2027.

## Backlog

- **Data cleanup**:
  1. ~~Clear `airtable_record_id` only on a 404~~ — done 2026-11-20 (29999db).
  2. ~~Re-link the S3 state DB~~ — done 2026-11-20: Iker entries re-pointed, 13 wrong
     links on past one-offs cleared, 2 dead IDs re-linked. About 70 dead IDs on old
     entries were left alone; they repair themselves when a post is seen again. Root
     cause fixed too: new posts no longer link by (organizer, weekday, time) to one-off
     or RETIRED records, and rides with a past date are never created.
  3. Airtable touch-ups, still open. Add "NO AUTO-RESTORE" to the notes of the 3 paused
     5am Pull Club records and S.L.A.B. Pimpin, which stops the sync warning. Also fill
     in missing Ride Types and hide empty shell record recES0YkEq1lBggMM. No deletions.
  4. The hidden "Ride for Team USA" record (recSvTJiQEk45M4gD) had its date overwritten
     to Nov 21 by the bad linking. Restore it if the record is kept for history.
- **Classifier**: one-off events (e.g. Play Like Iker) are sometimes typed `weekly_ride`,
  which lets the dedup step merge them into recurring rides. Worth tightening.
- **Thirsty Thursday (MBO Ten Cycling)** starts at MBO Brickell, in Miami. Hide it or keep it?
- **Logos on the "Coming up" event cards**: optional; the cards currently show no logos.
- **Runs**: tracked but hidden today; the plan is to add them to the site later.
- **Auto-hiding runs and out-of-area rides**: undecided.
