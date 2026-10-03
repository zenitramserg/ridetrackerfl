# RideTrackerFL — AWS Deployment Status

**As of:** 2026-10-02
**Scope:** Moving the scraper pipeline off-laptop onto scheduled AWS infrastructure.
**Account:** 624005844966, Region `us-east-2`

---

## 1. Bottom line

The scraper is **live and automated**. It runs once a day on AWS Fargate with no
laptop involved, and emails a report of what it found after every run.

| | |
|---|---|
| **Status** | Deployed and scheduled |
| **Next run** | Daily, 10:00 AM America/New_York (± 15 min flex window) |
| **Report delivery** | Email to zenitram.serg@gmail.com via SNS, confirmed |
| **Stack name** | `ridetrackerfl` (CloudFormation, `infra/ridetrackerfl.yaml`) |

---

## 2. What's deployed

CloudFormation stack `ridetrackerfl` in `us-east-2`:

| Resource | Purpose |
|---|---|
| ECR repo `ridetrackerfl` | Holds the scraper image (~4 GB, mostly Chromium). Keeps last 2 images. |
| S3 bucket `ridetrackerfl-state-624005844966` | `rides_database.json` dedup index, versioned, retained on stack delete. |
| ECS cluster + Fargate task (ARM64, 1 vCPU / 4 GB) | Runs the scraper container. Public subnet + public IP, no inbound. |
| EventBridge Scheduler | Triggers the task once daily at 10:00 ET. **Currently ENABLED.** |
| SNS topic `ridetrackerfl-alerts` | Two uses: (1) existing EventBridge rule alerts on any non-zero task exit, (2) the scraper itself now publishes a per-run summary report on success. Email subscription confirmed. |
| SSM Parameter Store (`/ridetrackerfl/*`) | Three SecureString secrets: `instagram-cookies`, `airtable-api-key`, `anthropic-api-key`. Set once by hand, read by the task at startup. |
| IAM: `TaskExecutionRole`, `TaskRole`, `SchedulerRole` | Scoped narrowly — task role can only touch its own S3 state path and publish to its own SNS topic. |

## 3. Work done this session

1. Reviewed and committed a pre-existing local fix (`sync_to_site.py` — guard against blank Organizer/Day-of-Week collisions), unrelated to the AWS work.
2. Fixed a CloudFormation bug blocking first deploy: `TaskSecurityGroup`'s `GroupDescription` had an em dash, and EC2 rejects non-ASCII there. Cleaned up the orphaned ECR repo and failed stack this caused, then redeployed clean.
3. Set the three secrets into SSM as `SecureString` via a local script that never printed values to any log or chat (read file → subprocess argv → AWS; script deleted after use).
4. Built and pushed the ARM64 image. First manual task run: scraping, OCR, and Claude Vision all worked end-to-end, but crashed writing the debug batch file — `run_scan.py` hardcoded a path under `/app/data`, which doesn't exist in the image (deliberately excluded from the build). Fixed to follow the same `RIDETRACKER_*` env-override pattern already used by `db.py` and `story_scraper.py`, defaulting to `/tmp/scan_batch_latest.json` in the container.
5. Verified the fix path with a second manual run scoped to one account (`--account omg_cycling`, to avoid hitting Instagram twice in quick succession) — exited 0. That run found 0 ride posts, so it hit the early-return path rather than the exact line that crashed before; the fix itself mirrors `db.py`'s existing pattern closely enough that this was accepted as sufficient rather than forcing a third full scrape.
6. Added a feature on request: the scraper now emails a summary (ride posts found, DB/Airtable counts, site-sync status) via SNS at the end of every non-dry-run — not just on failure. `TaskRole` got a narrowly-scoped `sns:Publish` permission for this one topic only.
7. Changed the schedule from twice daily to once daily at 10:00 AM ET, per request.
8. Resolved an SNS subscription confirmation that failed once (bad/expired link) by resubscribing; second confirmation succeeded.
9. Enabled the schedule. Caught and fixed a `cloudformation deploy` gotcha along the way: omitting `ScheduleExpression` from `--parameter-overrides` silently kept the *old* stack value (twice daily), not the template's new default — had to pass it explicitly to actually switch to once-daily.

## 4. Known loose ends / things to watch

- **Phase 6 (site sync) will likely fail inside the container on a real ride-post run.** `sync_to_site.py`'s `git_push` expects a full git working copy, but the image only contains `pipeline/` and two config files — no `.git`. This is already handled gracefully (wrapped in try/except, logs a warning, "no outage") and will show up as `Site sync: failed: ...` in the email report rather than crashing the task — but it means the live site won't actually get updated by the ECS run yet. Worth deciding later whether the laptop/UI session keeps doing that job, or whether it needs its own path in the container.
- **`public/rides.json` is being auto-committed by something else** (commits like `chore: sync rides.json [timestamp]`) — observed during this session but not investigated. Likely the other active Claude session (desktop app) or a local cron. Left untouched.
- The two manual verification runs today scraped live Instagram accounts twice in succession (one full scan, one single-account). No rate-limit issues observed, but worth keeping an eye on the first few real scheduled runs.
- First scheduled run: **tomorrow, 2026-10-03, ~10:00 AM ET.** Check the report email and/or CloudWatch Logs (`/ecs/ridetrackerfl`) afterward.

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
```
