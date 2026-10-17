# RideTrackerFL — AWS Deployment Status

**As of:** 2026-10-17
**Scope:** Moving the scraper pipeline off-laptop onto scheduled AWS infrastructure.
**Account:** 624005844966, Region `us-east-2`

---

## 1. Bottom line

The scraper is **live and automated**. It runs once a day on AWS Fargate with no
laptop involved, emails a report of what it found after every run, and uses a
cheap pre-filter to avoid paying for full extraction on obviously-irrelevant
content.

| | |
|---|---|
| **Status** | Deployed and scheduled |
| **Next run** | Daily, 10:00 AM America/New_York (± 15 min flex window) |
| **Report delivery** | Email to zenitram.serg@gmail.com via SNS, confirmed |
| **Stack name** | `ridetrackerfl` (CloudFormation, `infra/ridetrackerfl.yaml`) |
| **Cost per run** | ~$0.02 AWS compute + Claude Vision API (varies; near-zero on days with no ride content, since the pre-filter catches those before the expensive call) |

---

## 2. What's deployed

CloudFormation stack `ridetrackerfl` in `us-east-2`:

| Resource | Purpose |
|---|---|
| ECR repo `ridetrackerfl` | Holds the scraper image (~4 GB, mostly Chromium). Keeps last 2 images. |
| S3 bucket `ridetrackerfl-state-624005844966` | `rides_database.json` dedup index, versioned, retained on stack delete. |
| ECS cluster + Fargate task (ARM64, 1 vCPU / 4 GB) | Runs the scraper container. Public subnet + public IP, no inbound. |
| EventBridge Scheduler | Triggers the task once daily at 10:00 ET. **ENABLED.** |
| SNS topic `ridetrackerfl-alerts` | Two uses: (1) EventBridge rule alerts on any non-zero task exit, (2) the scraper publishes a per-run summary report on every successful (non-dry-run) completion. Email subscription confirmed. |
| SSM Parameter Store (`/ridetrackerfl/*`) | **Three** SecureString secrets currently live: `instagram-cookies`, `airtable-api-key`, `anthropic-api-key`. A fourth (`github-token`) is wired into the template but not yet created — see §4. |
| IAM: `TaskExecutionRole`, `TaskRole`, `SchedulerRole` | Scoped narrowly — task role can only touch its own S3 state path and publish to its own SNS topic. |

### Pipeline behavior (as of this writing)

- **Scrape:** Playwright pulls Instagram Stories from 10 active accounts (3 inactive, not scraped). Each account capped at `max_slides` — **raised from 24 to 120 for all active accounts** on 2026-10-17 (see §3).
- **Pre-filter:** A cheap Claude Haiku relevance check runs before the expensive Sonnet extraction call. Validated against real API calls with 0 false negatives on known ride posts; filters out non-ride content (stats recaps, ads, reposts) before it reaches the paid extraction step.
- **Extraction:** Claude Sonnet does full structured extraction only on slides the pre-filter passes.
- **Report:** Every non-dry-run publishes a summary to SNS — ride posts found, DB/Airtable counts, site-sync outcome — regardless of whether anything was found.

## 3. Timeline of work

**2026-10-02 — Initial deployment**
Reviewed and committed a pre-existing local fix (`sync_to_site.py` blank-field guard). Fixed a CloudFormation bug blocking first deploy (em dash in `GroupDescription`, EC2 rejects non-ASCII there). Set three secrets into SSM safely (local script reading files → AWS, never printing values, deleted after use). Built/pushed the ARM64 image; first manual run worked end-to-end through scraping/OCR/vision but crashed writing the debug batch file (`run_scan.py` hardcoded a path under `/app/data`, excluded from the image) — fixed to follow the existing `RIDETRACKER_*` env-override pattern. Added the SNS per-run report feature. Changed schedule from twice daily to once daily at 10:00 ET. Enabled the schedule (caught a `cloudformation deploy` gotcha: omitting a parameter silently keeps the *old* stack value, not the template's new default).

**2026-10-03 — Cost optimization**
Diagnosed why every slide was paying full Sonnet extraction price: the OCR pre-filter's threshold is deliberately near-zero (to avoid missing stylized flyer text), so it barely filters anything; the Ollama pre-filter that's supposed to cut 60-70% of calls isn't usable in production (needs a local server the container doesn't run, fails open silently). Added a Claude Haiku relevance pre-filter between OCR and Sonnet. Validated locally against real screenshots before deploying: 11/11 known ride posts correctly passed (0 false negatives); first prompt draft let through post-ride stats-recap cards, tightened the prompt to name that case explicitly, re-validated. Deployed and verified on a live ECS run: 100% filter rate on real content matching independently-confirmed ground truth, ~45% faster runtime. Separately hit and resolved an Anthropic API credit exhaustion mid-test (unrelated to the filter; user topped up $20).

**2026-10-13 — GitHub API site-sync fallback (partially complete)**
`run_scan.py`'s Phase 6 tries to `git push` `rides.json` to trigger Netlify's auto-deploy, but the ECS image deliberately excludes `.git`. Wrote a GitHub Contents API fallback in `sync_to_site.py` (`_git_push_via_api`) that pushes the file via HTTPS when no local `.git` is present — same end result (a commit on `main`), no local repo needed. Code is committed and present in the repo (ended up bundled into an unrelated commit, "Auto-restore recurring rides after specials expire", made by a different process working this same repo concurrently — not harmful, just worth knowing commit messages don't always match content exactly when multiple sessions touch the repo at once). CloudFormation template updated to wire in a `GITHUB_TOKEN` secret, committed 2026-10-17. **Still blocked:** the SSM parameter `/ridetrackerfl/github-token` doesn't exist yet — needs a fine-grained PAT (Contents: Read-and-write, scoped to this repo only) created and handed off before this can be deployed and used.

**2026-10-15 — Fixed a real production bug (data loss)**
User reported no rides being picked up despite seeing several posted. Investigation found the 2026-10-09 scheduled run *did* detect a real ride post ("Ride 84", conf 0.55) but then crashed immediately with `UnboundLocalError: cannot access local variable 'os'` — a redundant local `import os` inside Phase 6's try block made Python treat `os` as local to the whole `main()` function, breaking an earlier `os.environ.get()` call added on 2026-10-03. The ride was never saved to the database or Airtable. The existing EventBridge failure alert *did* fire correctly (SNS confirmed at 10:22 AM that day) — the safety net worked, the bug itself didn't. Fixed by removing the redundant import (verified with a minimal reproduction matching the exact function shape before deploying). Also ran a rigorous direct test — a full scan with the Haiku pre-filter completely disabled (`--skip-prefilter`) — to check whether the filter itself was over-aggressive; it found the same zero ride posts as the filtered run on the same day, confirming the filter was not at fault.

**2026-10-17 — Fixed a second, more impactful bug (silent truncation)**
Continued investigating the user's "rides not picked up" report after the crash fix — a ride posted by `alexbicycles` on/around December 5 was never caught and had to be added manually. Checked per-account slide counts across multiple days: `alexbicycles`, `mbo_tencycling`, and `teamrecoveryweston` were hitting exactly their 24-slide cap on every single day they posted anything, never fewer. The scraper captures stories starting from the *oldest* active story and advancing forward, stopping at `max_slides` — so an account with more than 24 active stories has its **newest** stories (exactly where a freshly-posted flyer would be) silently cut off, not its oldest. `omg_cycling` had already been raised to 120 at some point, presumably for the same reason, but the other 9 active accounts were still at the default 24. Raised all of them to 120.

**Verified same day:** a manual full-scan run (36m34s, vs the usual ~14m) came back with **600 screenshots across 11 accounts** — more than double the typical 216-264. Five accounts (`omg_cycling`, `unicosta_cycling`, `mbo_tencycling`, `pd.cyclingclub`, `teamrecoveryweston`) all hit the new 120-slide ceiling, confirming they'd been silently losing the majority of their content under the old cap. Clean run, no errors, Haiku pre-filter held up fine at the higher volume.

## 4. Known loose ends / things to watch

- **GitHub API site-sync fallback is coded but not deployable yet** — waiting on a fine-grained PAT (`/ridetrackerfl/github-token`). Until then, Phase 6 will still fail silently inside the container whenever a ride *is* found (harmless — wrapped in try/except, shows as `Site sync: failed: ...` in the report email) — but the live site is, in practice, being kept fresh by a **separate local cron job** on this laptop (`*/15 * * * * cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site`), which explains why the site hasn't actually gone stale despite this gap.
- **Multiple processes/sessions edit this repo concurrently** (this session, a separate "UI"/desktop Claude session, and a local cron). Observed one case where uncommitted working-tree edits from this session got swept into an unrelated commit by another process. Not harmful so far, but worth a `git status`/`git diff` sanity check before assuming a file's committed state matches what you last edited.
- **Verify the 2026-10-17 `max_slides` fix catches real content** — the raised caps (24→120) increase scrape time and Haiku-filter volume proportionally; watch the first few runs for unexpectedly long runtimes if these accounts are posting far more than previously assumed.
- Only 10 accounts are monitored (3 marked inactive: `revoltcyclery`, `letourdeweston`, `pullclub.cc`), and **only Instagram Stories are scraped — never regular feed posts.** If an organizer posts a ride announcement as a feed post rather than a Story, this system will never see it, by design. Worth knowing as a standing limitation, not a bug.

## 5. Useful commands

```bash
# Tail logs live
aws logs tail /ecs/ridetrackerfl --follow

# Check schedule state
aws scheduler get-schedule --name ridetrackerfl

# Manually trigger a run (full scan)
aws ecs run-task --cluster ridetrackerfl --task-definition ridetrackerfl \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-0b2003631889055b8,subnet-01b657d9c4ed6f701,subnet-0ddb8948b172e3645],securityGroups=[sg-03c943061d7b91b35],assignPublicIp=ENABLED}"

# Manually trigger a run scoped to one account (lighter on Instagram/cost)
# ...add to the command above:
  --overrides '{"containerOverrides":[{"name":"scraper","command":["python3","-m","pipeline.run_scan","--account","omg_cycling"]}]}'

# Disable the Haiku pre-filter for one run (compare against normal behavior)
  --overrides '{"containerOverrides":[{"name":"scraper","command":["python3","-m","pipeline.run_scan","--skip-prefilter"]}]}'
```
