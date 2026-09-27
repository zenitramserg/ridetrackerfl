# RideTrackerFL — Scope of Works
**Last updated:** April 14, 2026
**Launch target:** Le Tour de Weston, May 9, 2026 — 25 days to go
**Live site:** ridetrackerfl.com

---

## ✅ COMPLETED

### Foundation
- [x] Static HTML/CSS/JS website on Netlify with custom domain (ridetrackerfl.com)
- [x] Airtable as CMS — Rides, Organizers, Subscribers, Organizer Updates tables
- [x] Netlify serverless function proxying Airtable token (never exposed client-side)
- [x] Auto-deploy from GitHub on every push
- [x] Logo + Favicon (PWA icons serving as favicon)
- [x] Organizer Avatars — uploaded to Airtable Organizers table ✅
- [x] Social Links — Instagram, Strava, WhatsApp, Website per organizer on every ride card
- [x] Meeting Point / Google Maps — Maps link on every ride card
- [x] Email Signup Form — live with Netlify serverless function + Subscribers table
- [x] Mobile Optimization — responsive layout, filters, tested on iOS + Android

### Analytics
- [x] GA4 Setup (G-WM5CNVPHZ2)
- [x] Event Tracking — 10+ custom events: ride_view, social_link_click, maps_link_click, register_click, tab_switch, filter_used, pwa_install_prompt_shown, pwa_install_accepted, pwa_install_dismissed

### QA
- [x] Multi-Device Testing — PWA tested iOS + Android, desktop Chrome + Safari

### PWA — Progressive Web App (added since original scope)
- [x] Part 1: Icons + Manifest — 9 icon sizes, iOS apple-touch-icon (180×180)
- [x] Part 2: Service Worker + Offline — Network First for API, Cache First for assets, offline banner
- [x] Part 3: Custom Install Prompt — Android/Chrome, 7-day dismiss, GA4 tracked
- [x] Part 4: Auto-refresh — re-fetches on app resume + every 10 min (fixes iOS frozen data)

### Scraper Pipeline (added since original scope)
- [x] Instagram Scraper — Playwright scrapes stories from 11 accounts
- [x] Claude Vision API — extracts ride name, day, time, location, distance, pace
- [x] Airtable Sync — pyairtable writes new records, Display on Site auto-set to true
- [x] Bug fixes — Path import, weather date guard, duplicate detection, past ride shadowing
- [x] Recurring ride dedup — Tier 3 matching (weekday+time+location) across weeks; reposts merge correctly
- [x] push_updated_rides() — syncs changed date/status/weather back to existing Airtable records
- [x] Ride History table — logs each weekly detection for analytics and organizer sales pitch

### Email & Organizer Systems (added since original scope)
- [x] Welcome email automation — Airtable + Gmail, triggers on signup, Welcome Email Sent checkbox
- [x] Organizer Update System — Airtable form + 9 personalized links + DM templates
- [x] Display on Site — bulk-set to true for all 28 active rides; new scraper records auto-checked

### Organizer Update System (added since original scope)
- [x] Airtable "Organizer Updates" table for tracking inbound organizer requests
- [x] Public Airtable form with pre-filled organizer name + access code
- [x] 9 personalized form links — one per organizer, ready to send
- [x] DM templates + onboarding script (see organizer-update-system.md)

---

## 🔴 HIGH PRIORITY — Must complete before May 9

| Task | Est. Hrs | Notes |
|---|---|---|
| Organizer Outreach | 1.5 | DMs with personalized links — most time-sensitive, send ASAP |
| Scraper Automation (Mac mini cron) | 2.0 | Replace manual 10am/7pm runs — needed for reliability |
| /letour Landing Page + UTM tracking | 1.5 | Attribution for May 9 traffic |
| Final Launch Strategy | 1.0 | Checklist, comms plan, day-of roles |
| Pre-Event Testing | 1.0 | Full run-through on actual devices |
| Event Day Execution (May 9) | 3.5 | On-site, QR codes, live monitoring |

---

## 🟡 MEDIUM PRIORITY — Nice to have before May 9

| Task | Est. Hrs | Notes |
|---|---|---|
| Ride Count Social Proof | 2.0 | Live count from Airtable on homepage |
| Tier 2 Features (Strava links, last verified timestamps) | 1.5 | Schema ready, needs UI wiring |
| QR Code Cards (Design + Print) | 1.5 | For event day handout |
| Load Testing (Netlify free tier) | 1.0 | Verify site holds up under event traffic |
| Instagram Content Plan | 1.5 | Weekly ride roundup post template |

---

## 🔵 BACKLOG — Post-launch

| Task | Est. Hrs | Notes |
|---|---|---|
| Demo Video / Reels | 2.0 | Can do after launch with real footage |
| Testimonials | 1.0 | Collect from riders post-event |
| Post-Event Analysis | 1.0 | GA4 review, subscriber growth, ride views |
| Scraper: update existing records instead of creating new (cleaner Airtable) | 3.0 | Architectural improvement |
| Ride rating / community feedback | — | Future feature |
| Email newsletter automation (weekly digest) | — | Future feature |
| WhatsApp group integration | — | Future feature |
| Strava segment linking per ride | — | Future feature |

---

## Tech Stack Summary

| Layer | Tool |
|---|---|
| Frontend | HTML / CSS / JS (vanilla) |
| Hosting | Netlify (free tier) |
| CMS / Database | Airtable |
| Scraper | Python + Playwright + Claude Vision API |
| Weather | Open-Meteo (free, no key) |
| Analytics | Google Analytics 4 |
| PWA | Manifest + Service Worker |
| Repo | github.com/zenitramserg/ridetrackerfl |

---

## Hours Summary

| Category | Original Est. | Status |
|---|---|---|
| Completed (original scope) | ~18 hrs | ✅ Done |
| Completed (added scope — PWA, Scraper, Organizer System) | ~12 hrs | ✅ Done |
| Remaining High Priority | ~10.5 hrs | 🔴 Before May 9 |
| Remaining Medium Priority | ~7.5 hrs | 🟡 Before May 9 if possible |
| Backlog | Open | 🔵 Post-launch |
