# RideTrackerFL — Cost Audit

**As of:** 2026-09-20
**Scope:** RideTrackerFL only. Shared subscriptions counted at **full cost** (not prorated).
**Sources:** Gmail billing receipts, Netlify billing dashboard, Verisign RDAP, Airtable API, repo config.

---

## 1. Bottom line

| | |
|---|---|
| **Current monthly run rate** | **$51.67 / mo** (~$620 / yr) |
| **Lifetime spend** (Mar 24 – Sep 20, 2026) | **$285 confirmed** (~$325 incl. two unreceipted Claude Pro months) |
| **Biggest line item** | Netlify Pro — $20/mo, upgraded Sep 19 after a credit-overrun suspension |
| **Single biggest structural waste** | Every scraper run triggers a production deploy at 15 Netlify credits each |
| **Site status** | Live and serving rides as of 2026-09-20 18:32 ET — the Sep 19 suspension was cleared by the Pro upgrade |

---

## 2. Service inventory

### Paid

| Service | What it does here | Plan | Cost | Status |
|---|---|---|---|---|
| **Netlify** | Static hosting, serverless functions (`airtable.js`, `subscribe`, `ics`, `rsvp`), CI deploys | Pro, effective Sep 19 2026 | **$20.00 / mo** | Confirmed (dashboard) |
| **Anthropic — Claude Pro** | Dev/strategy sessions (Chat + Cowork) | Individual | **$20.00 / mo** | Confirmed (receipts) |
| **Anthropic — Claude API** | `claude-sonnet-4-6` vision extraction in `pipeline/vision_client.py` | Prepaid credits, org `Ridetrackerfl` | **~$10 / mo** avg | Confirmed (receipts) |
| **Squarespace Domains** | `ridetrackerfl.com` | 1-yr registration | **~$20 / yr** ($1.67/mo) | Registrar confirmed via RDAP; **price unconfirmed** |

### Free tier — $0

| Service | Role | Headroom |
|---|---|---|
| **Airtable** | Source of truth. Base `appp3CTtWpqVcTn6e`, workspace `wspTRGvEf1RIPHExH` | Free plan. ~400 of 1,000 records used — **see Risk 1** |
| **Open-Meteo** | Rain % + wind speed enrichment | Keyless, free, unlimited for this volume |
| **GitHub** | Repo `zenitramserg/ridetrackerfl`, CodeQL, deploy previews | Free |
| **Google Analytics 4** | `G-WM5CNVPHZ2` | Free |
| **Instagram** | 10 active accounts scraped via local Playwright | No paid proxy or API |
| **Ollama** | Optional local vision pre-filter (`--use-ollama`) | Local, free |
| **Make.com** | Account created Aug 6 2026, unused | Free, no charges |

### Shared / adjacent — not billed to this project

| Service | Link to project | Note |
|---|---|---|
| **Google Workspace** (`terranovaegroup.com`) | Sends the `hello@` subscriber welcome emails | Monthly invoice exists (Feb 2026); amount sits inside the PDF attachment — not opened |
| **Mac mini** | Planned scraper automation host | Not purchased. Scraper still runs manually on the MacBook |

---

## 3. Confirmed spend ledger

### Netlify — $80.00 total

| Date | Amount | What |
|---|---|---|
| Apr 4, 2026 | $9.00 | Personal plan (1,000 credits/mo) |
| May 5, 2026 | $9.00 | Personal plan |
| Jun 5, 2026 | $9.00 | Personal plan |
| Jun 29, 2026 | $5.00 | Credit top-up (ran dry mid-cycle) |
| Jul 5, 2026 | $9.00 | Personal plan |
| Aug 20, 2026 | $9.00 | Personal plan (re-upgrade after Aug 5 downgrade to Free) |
| Sep 19, 2026 | $20.00 | **Pro plan** (3,000 credits/mo) |
| Sep 19, 2026 | $10.00 | Credit top-up — 1,500 credits, no expiration |

**Current state:** Pro, billing period Sep 19 → Oct 18. 2,954 / 3,000 plan credits remaining + 1,486 banked non-expiring credits. Auto-recharge **disabled**.

### Anthropic — $185.00 confirmed

| Date | Amount | Line item | Bucket |
|---|---|---|---|
| Mar 28, 2026 | $5.00 | One-time credit purchase | API |
| Apr 4, 2026 | $10.00 | One-time credit purchase | API |
| Apr 17, 2026 | $45.00 | Prepaid extra usage, Individual plan | Claude Pro |
| Apr 26, 2026 | $20.00 | Claude Pro | Claude Pro |
| Apr 28, 2026 | $25.00 | One-time credit purchase | API |
| Jun 26, 2026 | $20.00 | Claude Pro | Claude Pro |
| Jul 26, 2026 | $20.00 | Claude Pro | Claude Pro |
| Aug 24, 2026 | $20.00 | One-time credit purchase | API |
| Sep 14, 2026 | $20.00 | Claude Pro | Claude Pro |

**API credits subtotal: $60.00** (~$10/mo over 6 months)
**Claude Pro subtotal: $125.00**

> **Gap:** Stripe invoice numbers `1PUBA983-0009` (≈ May 26) and `-0012` (≈ Aug 26) are missing from the receipt sequence. Two Claude Pro charges of $20 were almost certainly made. **True Anthropic spend is likely $225.** Verify on the Anthropic billing page.

### Domain — ~$20.00

`ridetrackerfl.com` — Squarespace Domains LLC. Registered **2026-03-24**, expires **2027-03-24**. Nameservers point to Google Cloud DNS, then on to Netlify. No purchase receipt in Gmail and the domain does not appear in the logged-in Squarespace account — **confirm which account holds it before the March renewal.**

---

## 4. Unit economics — what drives the cost

### Netlify: 15 credits per production deploy

| Plan | Monthly credits | Deploys/mo before cutoff |
|---|---|---|
| Free | 300 | 20 |
| Personal ($9) | 1,000 | 66 |
| **Pro ($20)** | **3,000** | **200** |

Current cycle: 3 deploys = 45 credits. Web requests, compute and bandwidth together came to **under 1 credit**. Deploys are ~99% of the bill.

**The scraper commits `public/rides.json` to GitHub, which auto-deploys.** At the observed cadence that is ~24 deploys/month of pure data refresh = ~360 credits/month spent to update a JSON file.

### Anthropic API: ~$0.0117 per story slide

| Component | Value |
|---|---|
| Model | `claude-sonnet-4-6` — $3/MTok in, $15/MTok out |
| Screenshot | 1280 × 900 → ~1,536 image tokens |
| Extraction prompt | ~600 tokens, identical every call |
| Output | ~350 tokens (JSON + `raw_visible_text` transcription) |
| **Input cost** | 2,150 tok × $3/MTok = **$0.00645** |
| **Output cost** | 350 tok × $15/MTok = **$0.00525** |
| **Per slide** | **$0.0117** |

Filters before the API call: Tesseract OCR text check (layer 1), optional Ollama pre-filter (layer 2, only with `--use-ollama`).

### Scan cadence — reality vs. the runbook

| | |
|---|---|
| Runbook says | ~2 scans/day (10 AM and 7 PM) |
| Actual | **0.80 scans/day** — 65 scans over 81 days (Jul 1 – Sep 20) |
| Jul / Aug / Sep | 28 / 25 / 12 scans |

Scan volume is **declining**. September is running at ~0.6/day.

### Storage

`data/screenshots/` — 1,857 PNGs, **733 MB**, 66 scan folders. Gitignored, so no cloud cost, but it grows unbounded on local disk.

---

## 5. Cost reduction — ranked by value

### 1. Stop paying 15 credits to publish a JSON file — ~$0 but removes the failure mode
`sync_to_site.py` → git push → production deploy. Options, cheapest first:
- Write `rides.json` to **Netlify Blobs** and have `index.html` read it from there. Data refreshes stop touching deploys entirely.
- Or serve it from the **raw GitHub URL** for the `main` branch.
- Or batch commits so at most one deploy/day carries data.

This is what caused both the Aug 20 and Sep 19 suspensions. Fix this and the Pro upgrade becomes optional.

### 2. Switch the vision model — saves ~33–67% of API spend
| Model | Input | Output | Cost/slide | Saving |
|---|---|---|---|---|
| `claude-sonnet-4-6` (current) | $3 | $15 | $0.0117 | — |
| `claude-sonnet-5` | $2 | $10 | $0.0078 | **33%** |
| `claude-haiku-4-5` | $1 | $5 | $0.0039 | **67%** |

Sonnet 5 is newer *and* cheaper — that one is close to free money. Haiku 4.5 is worth an A/B on ~50 archived slides before committing; flyer text extraction is well inside its range. One-line change: `CLAUDE_MODEL` in `pipeline/vision_client.py:29`.

### 3. Turn auto-recharge back on with a low cap — prevents outages, costs nothing extra
Auto-recharge is disabled on Netlify and the API org has no floor. That is precisely why a credit overrun became a **full project suspension** on Sep 19 rather than a $5 charge. A $10 cap on each is cheap insurance for a public site.

### 4. Reconsider Netlify Pro after fix #1 — saves $11/mo ($132/yr)
You went Pro reactively on Sep 19. Personal ($9, 1,000 credits = 66 deploys/mo) plus the 1,486 banked non-expiring credits is ample once data refreshes stop consuming deploys. Revisit at the Oct 18 renewal.

### 5. Prune the screenshot archive
733 MB and growing. Keep 14 days; delete the rest. No dollar cost, but it will matter on the Mac mini.

### Deliberately not recommended
- **Lowering `omg_cycling` max_slides** — the 120 cap is load-bearing (ride flyer lands at slide ~80). Documented lesson; leave it.
- **Prompt caching** — the extraction prompt is ~600 tokens, below the 1,024-token cache minimum, and image tokens dominate anyway.
- **Batch API (50% off)** — technically applicable but introduces up-to-24h latency on a same-day ride feed. Not worth it.

---

## 6. Risks and watch items

**Risk 1 — Airtable free-tier record cap.** Free plan allows **1,000 records per base**. Ride History alone is at **253** and grows ~40–50/month; Rides is ~110+. Estimated total ~400. At current growth the base hits the cap in roughly **11–12 months**, and the next tier is Team at **$20/seat/mo billed annually ($240/yr) or $24/seat/mo billed monthly** — which would nearly double total project cost. (Team raises the cap to 50,000 records/base.) Mitigation: archive Ride History rows older than 6 months to CSV, or move history to a second base.

**Risk 2 — Two unreceipted Claude Pro charges.** ~$40 unaccounted. Verify on the Anthropic billing page.

**Risk 3 — Domain ownership is unclear.** `ridetrackerfl.com` renews 2027-03-24 but is not visible in the logged-in Squarespace account. Locate the owning account well before renewal.

**Risk 4 — Anthropic Console not audited.** API usage-by-day and true token spend could not be read (console session not authenticated in the browser). The API figures above are derived from **credit purchases**, not metered usage. Pull `platform.claude.com/usage` for the authoritative number.

---

## 7. Reconciliation

| Bucket | Confirmed | Method |
|---|---|---|
| Netlify | $80.00 | Dashboard invoice list, cross-checked against Gmail |
| Anthropic | $185.00 | 9 Stripe receipts, itemized |
| Domain | ~$20.00 | RDAP registrar confirmed; price is list-rate estimate |
| Airtable / GitHub / GA4 / Open-Meteo / Make | $0.00 | No billing email in 12 months; free-tier limits verified |
| **Total** | **$285.00** | +$40 likely (unreceipted Claude Pro) = **~$325** |

Monthly run rate: $20.00 (Netlify) + $20.00 (Claude Pro) + $10.00 (API avg) + $1.67 (domain) = **$51.67/mo**.

---

## 8. Sources

- Netlify billing dashboard — `app.netlify.com/teams/zenitramserg/billing/general` (plan, credit balance, invoice history, credit-usage breakdown)
- Gmail — 9 Anthropic Stripe receipts, 8 Netlify notices, Google Workspace invoice notice
- [Claude Platform pricing](https://platform.claude.com/docs/en/about-claude/pricing) — per-model token rates
- [Airtable plans overview](https://support.airtable.com/docs/airtable-plans) — free-tier record cap and Team pricing
- Verisign RDAP — `rdap.verisign.com/com/v1/domain/ridetrackerfl.com` (registrar, registration and expiry dates)
- Airtable API — workspace, base schema and record counts
- Repo — `pipeline/vision_client.py`, `pipeline/story_scraper.py`, `config/accounts.json`, `data/screenshots/`

**Tracking table:** `Costs` table in base `appp3CTtWpqVcTn6e` (`tbl7i84tjIGfhQMVE`) — 13 records, one per service. Update Monthly Cost / Next Charge / Last Verified each cycle.
