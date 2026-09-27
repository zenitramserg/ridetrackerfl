# RideTrackerFL — Organizer Update System
**Created:** April 9, 2026  
**Purpose:** Reference sheet for organizer outreach (Apr 10–13 rides)

---

## PERSONALIZED LINKS

| Organizer | Personalized Link |
|---|---|
| OMG Cycling | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=OMG+Cycling&prefill_Access+Code=omg-sunrise-2024 |
| Weston Flyers | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Weston+Flyers&prefill_Access+Code=wf-peloton-2024 |
| Revolt Cyclery | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Revolt+Cyclery&prefill_Access+Code=revolt-speed-2024 |
| Unicosta Cycling | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Unicosta+Cycling&prefill_Access+Code=unicosta-waves-2024 |
| Ride 84 | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Ride+84&prefill_Access+Code=ride84-chain-2024 |
| Wild Hogs | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Wild+Hogs&prefill_Access+Code=wildhogs-trail-2024 |
| FP Bike Shop | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=FP+Bike+Shop&prefill_Access+Code=fpbike-spoke-2024 |
| MBO Ten | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=MBO+Ten&prefill_Access+Code=mbo-tempo-2024 |
| Team Recovery Weston | https://airtable.com/appp3CTtWpqVcTn6e/pagy6VkDkACf6V9VJ/form?prefill_Organizer=Team+Recovery+Weston&prefill_Access+Code=recovery-grupo-2024 |

---

## ACCESS CODES (keep private)

| Organizer | Access Code |
|---|---|
| OMG Cycling | `omg-sunrise-2024` |
| Weston Flyers | `wf-peloton-2024` |
| Revolt Cyclery | `revolt-speed-2024` |
| Unicosta Cycling | `unicosta-waves-2024` |
| Ride 84 | `ride84-chain-2024` |
| Wild Hogs | `wildhogs-trail-2024` |
| FP Bike Shop | `fpbike-spoke-2024` |
| MBO Ten | `mbo-tempo-2024` |
| Team Recovery Weston | `recovery-grupo-2024` |

---

## INSTAGRAM DM TEMPLATES

### Template 1 — First Contact / Onboarding
```
Hey [Name]! Thanks for reaching out 🚴

Two ways to update your rides on RideTrackerFL:

1️⃣ Quick DM: Message me here anytime
   "Thursday confirmed"
   "Saturday canceled - weather"

2️⃣ Instant Form (10 sec): [LINK]
   Bookmark it for updates

I'll publish changes within minutes. Sound good?
```

### Template 2 — Confirmation Reply
```
✅ Updated! Your [Day] ride is now [status] on ridetrackerfl.com
Thanks for keeping riders informed 🙏
```

### Template 3 — Need More Info
```
Got it! Which day/ride are you updating?
Or use your quick form: [LINK]
```

### Template 4 — Sending Link
```
Here's your instant update form (bookmark it):
[LINK]

Pick day → Pick status → Submit
Updates live in ~2 min 🚴
```

### Template 5 — Post-Onboarding Follow-Up DM
```
Hey [Name]!

Thanks for partnering with RideTrackerFL 🚴

Your quick update link (bookmark it):
[PERSONALIZED LINK]

Or just DM @ridetrackerfl anytime:
✅ "Thursday confirmed"
❌ "Saturday canceled"
📍 "Route change - [details]"

Updates live within minutes!
```

---

## IN-PERSON ONBOARDING SCRIPT

**Step 1 — Show Site**
> "Hey! Check out RideTrackerFL — shows all Weston rides in one place."
*(Open ridetrackerfl.com on phone, show their rides)*

**Step 2 — Pitch Value**
> "We're launching at Le Tour in May — gonna send riders your way. Want to make sure we have your details right."

**Step 3 — Offer Update Options**
> "Two easy ways to update your rides:
> Option 1: Just DM @ridetrackerfl on Instagram — 'Thursday confirmed' or 'Saturday canceled'
> Option 2: Bookmark this form — takes 10 seconds."
*(Send personalized link via Instagram DM right there)*

**Step 4 — Demo**
*(Open their link on your phone)*
> "See? Your name's filled in. Just pick day, pick status, submit. Done."

**Step 5 — Close**
> "Cool with us featuring your rides?"
*(They say yes)*
> "Perfect! I just sent you the link. Thanks!"

---

## DAILY WORKFLOW (5 min morning + 5 min evening)

1. Check Instagram DMs — if organizer sent update (e.g. "Thursday confirmed"):
   - Go to Rides table → find that ride → update Status field
   - Reply: "✅ Updated!"

2. Check Organizer Updates table — filter: **Processed = unchecked**
   - Read: Organizer, Day, Update Type, Details
   - Apply to matching record in Rides table
   - Return to Organizer Updates → check **Processed** → add note

3. Site auto-updates (Netlify pulls from Airtable live)

---

## AIRTABLE TABLE IDs (for reference)
- Base: `appp3CTtWpqVcTn6e`
- Rides: `tbl7xURgDo5wU4z5t`
- Organizer Updates: `tblP9eccRTTRqVPOz`
- Subscribers: `tblRng0AWjHWEwQae`
- Organizers: `tblYXWvRSgqWkvaVE`
