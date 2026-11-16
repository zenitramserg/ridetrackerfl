from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from datetime import datetime

from pathlib import Path

# Run with: python3.12 scripts/generate_sop.py  (needs reportlab, see requirements.txt)
OUTPUT = str(Path(__file__).resolve().parent.parent / "docs" / "RideTrackerFL_Daily_SOP.pdf")

doc = SimpleDocTemplate(
    OUTPUT,
    pagesize=letter,
    rightMargin=0.85*inch,
    leftMargin=0.85*inch,
    topMargin=0.85*inch,
    bottomMargin=0.85*inch,
)

BASE   = colors.HexColor("#0d0d0d")
MUTED  = colors.HexColor("#525252")
ACCENT = colors.HexColor("#059669")
CODE_BG= colors.HexColor("#f4f4f4")
BORDER = colors.HexColor("#e5e5e5")

styles = getSampleStyleSheet()

title_style = ParagraphStyle("title", fontSize=22, fontName="Helvetica-Bold",
    textColor=BASE, spaceAfter=4, leading=26)
subtitle_style = ParagraphStyle("subtitle", fontSize=12, fontName="Helvetica",
    textColor=MUTED, spaceAfter=20, leading=16)
h2_style = ParagraphStyle("h2", fontSize=13, fontName="Helvetica-Bold",
    textColor=BASE, spaceBefore=20, spaceAfter=6, leading=17)
h3_style = ParagraphStyle("h3", fontSize=11, fontName="Helvetica-Bold",
    textColor=ACCENT, spaceBefore=10, spaceAfter=4, leading=14)
body_style = ParagraphStyle("body", fontSize=10, fontName="Helvetica",
    textColor=BASE, spaceAfter=6, leading=15)
note_style = ParagraphStyle("note", fontSize=9.5, fontName="Helvetica",
    textColor=MUTED, spaceAfter=6, leading=14, leftIndent=12)
code_style = ParagraphStyle("code", fontSize=9.5, fontName="Courier",
    textColor=colors.HexColor("#1a1a1a"), spaceAfter=0, leading=14,
    leftIndent=8, rightIndent=8)

def code_block(text):
    rows = [[Paragraph(line, code_style)] for line in text.strip().split("\n")]
    t = Table(rows, colWidths=[6.3*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), CODE_BG),
        ("BOX",        (0,0),(-1,-1), 0.5, BORDER),
        ("TOPPADDING",    (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 8),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [CODE_BG]),
    ]))
    return t

def section_rule():
    return HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=4, spaceBefore=4)

story = []

# ── Header ────────────────────────────────────────────────────────────────────
story.append(Paragraph("RideTrackerFL", title_style))
story.append(Paragraph("Daily Operations — Standard Operating Procedure", subtitle_style))
story.append(section_rule())
story.append(Paragraph(
    f"Last updated: {datetime.now().strftime('%B %d, %Y')}  ·  "
    "Site: ridetrackerfl.com  ·  Instagram: @ridetrackerfl",
    note_style
))
story.append(Spacer(1, 16))

# ── Overview ──────────────────────────────────────────────────────────────────
story.append(Paragraph("Overview", h2_style))
story.append(Paragraph(
    "Both jobs run on their own. The scraper runs in AWS every day at 10:00 AM ET and emails a "
    "report of what it added, changed and rejected. The laptop syncs Airtable to the site every "
    "hour from 5 AM to 10 PM while it is awake. Your daily task is to read the report email and "
    "fix anything wrong in Airtable. Commands below are run from the terminal inside the "
    "ridetrackerfl project folder.",
    body_style
))
story.append(Spacer(1, 8))

# ── Quick Reference Table ─────────────────────────────────────────────────────
story.append(Paragraph("Quick Reference", h2_style))

table_data = [
    [
        Paragraph("Situation", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
        Paragraph("Command", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    ],
    [
        Paragraph("Push Airtable edits to the site now (instead of within the hour)", body_style),
        Paragraph("python3.12 -m pipeline.sync_to_site", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
    ],
    [
        Paragraph("Log in to AWS (session expires about daily)", body_style),
        Paragraph("aws login", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
    ],
    [
        Paragraph("Read the last sync runs", body_style),
        Paragraph("tail -40 logs/sync.log", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
    ],
    [
        Paragraph("Run the scraper by hand on the laptop (rarely needed)", body_style),
        Paragraph("python3.12 -m pipeline.run_scan", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
    ],
]

ref_table = Table(table_data, colWidths=[2.6*inch, 3.7*inch])
ref_table.setStyle(TableStyle([
    ("BACKGROUND",    (0,0), (-1,0),  colors.HexColor("#f0fdf4")),
    ("BACKGROUND",    (0,1), (-1,-1), colors.white),
    ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ("BOX",           (0,0), (-1,-1), 0.5, BORDER),
    ("INNERGRID",     (0,0), (-1,-1), 0.5, BORDER),
    ("TOPPADDING",    (0,0), (-1,-1), 7),
    ("BOTTOMPADDING", (0,0), (-1,-1), 7),
    ("LEFTPADDING",   (0,0), (-1,-1), 10),
    ("RIGHTPADDING",  (0,0), (-1,-1), 10),
]))
story.append(ref_table)
story.append(Spacer(1, 16))

# ── Flow 1 ────────────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph("Flow 1 — Daily Scraper Run (automatic)", h2_style))
story.append(Paragraph(
    "AWS runs the scraper every day at 10:00 AM ET (ECS Fargate, us-east-2), using the "
    "<font face='Courier'>:latest</font> image in the ridetrackerfl ECR repository. Nothing to "
    "start by hand. About 10 minutes later an email arrives titled "
    "<i>RideTrackerFL: N new, N re-confirmed, N rejected</i>, listing each ride with a link to "
    "its Airtable record:",
    body_style
))
for label, desc in [
    ("NEW", "already live on the site — check name, date, time and start location"),
    ("CHECK", "a post differs from what is on file (time, place or name); not applied, fix by hand if the post is right"),
    ("REJECTED", "not saved, with the reason; the story expires within 24h"),
    ("RE-CONFIRMED", "rides seen again, with their dates"),
]:
    story.append(Paragraph(
        f"<b>{label}:</b> {desc}",
        ParagraphStyle("step", fontSize=10, fontName="Helvetica", textColor=BASE,
                       spaceAfter=4, leading=14, leftIndent=12)
    ))
story.append(Spacer(1, 10))

story.append(Paragraph("What it does automatically:", h3_style))
steps = [
    ("Phase 1", "Scrapes Instagram stories for all active accounts in accounts.json"),
    ("Phase 2", "Sends screenshots to Claude vision API to extract ride details"),
    ("Phase 3", "Validates and deduplicates rides, updates rides_database.json"),
    ("Phase 4", "Fetches weather forecasts for upcoming rides"),
    ("Phase 5", "Pushes new and updated rides to Airtable"),
    ("Phase 6", "Syncs Airtable to public/rides.json and pushes to GitHub — site updates"),
]
for phase, desc in steps:
    story.append(Paragraph(
        f"<b>{phase}:</b> {desc}",
        ParagraphStyle("step", fontSize=10, fontName="Helvetica", textColor=BASE,
                       spaceAfter=4, leading=14, leftIndent=12)
    ))

story.append(Spacer(1, 10))
story.append(Paragraph("After the report arrives:", h3_style))
story.append(Paragraph("Spot-check the rides it names in Airtable. Look for:", body_style))
review = [
    "Rides added with wrong dates, times, or locations",
    "Rides that should be hidden (set Display on Site = false)",
    "Rides the scraper missed that you know about",
    "Any organizer who contacted you with new ride details",
    "New organizer added? Check Active checkbox in Organizers table so their logo shows",
]
for item in review:
    story.append(Paragraph(
        f"  •  {item}",
        ParagraphStyle("bullet", fontSize=10, fontName="Helvetica", textColor=BASE,
                       spaceAfter=3, leading=14, leftIndent=16)
    ))

story.append(Spacer(1, 16))

# ── Flow 2 ────────────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph("Flow 2 — Manual Airtable Edit", h2_style))
story.append(Paragraph(
    "Changes made directly in Airtable — fixing a date, adding a ride from an organizer email, "
    "toggling Display on Site — reach the site at the next hourly sync (5 AM to 10 PM). "
    "New organizer logos can take up to 12 hours. To push them live right away, run:",
    body_style
))
story.append(Spacer(1, 6))
story.append(code_block("cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site"))
story.append(Spacer(1, 8))
story.append(Paragraph(
    "The site updates within about 60 seconds. A manual sync always fetches everything fresh. "
    "To hide a ride, uncheck Display on Site rather than deleting the record.",
    note_style
))

story.append(Spacer(1, 16))

# ── Troubleshooting ────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph("Troubleshooting", h2_style))

issues = [
    (
        "Site shows old data",
        "The hourly sync only runs 5 AM to 10 PM while the laptop is awake. Run it manually:\n"
        "cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site\n"
        "The \"Updated\" label on the site shows when the rides last changed, not when the sync last ran."
    ),
    (
        "Can't open ridetrackerfl.com from home (\"Web Page Blocked\")",
        "The home network filter blocks the domain. The same site is at "
        "https://super-tanuki-783b85.netlify.app — use that to check it."
    ),
    (
        "Sync log shows Airtable error 429 PUBLIC_API_BILLING_LIMIT_EXCEEDED",
        "Airtable's free plan allows about 1,000 API requests a month, shared by the laptop sync "
        "and the daily scan. Every Airtable request fails until the monthly count resets; the site "
        "stays up with its last data. Check usage in Airtable → workspace settings. Do not make the "
        "sync run more often than hourly (crontab -l shows the schedule), and keep "
        "--org-cache-hours 12 on the cron line. First seen 2026-11-15."
    ),
    (
        "Sync log shows \"nodename nor servname\", timeouts or \"Remote end closed connection\"",
        "The laptop was waking up or offline. Harmless — the next run fixes it and the site stays up."
    ),
    (
        "Sync warns \"N hidden weekly ride(s) with no expired special to explain them\"",
        "Only a reminder. If those rides are hidden on purpose, add NO AUTO-RESTORE to their Notes "
        "in Airtable and the warning stops."
    ),
    (
        "AWS command says \"Your session has expired\"",
        "Log in again (it lasts about a day):\n"
        "aws login"
    ),
    (
        "A code change or config change (accounts.json) is not in the daily scan or email yet",
        "AWS runs whatever image is tagged :latest; a commit alone changes nothing there. Build and push:\n"
        "cd ~/ridetrackerfl && docker build --platform linux/arm64 -t ridetrackerfl:new .\n"
        "R=624005844966.dkr.ecr.us-east-2.amazonaws.com\n"
        "aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin $R\n"
        "docker tag ridetrackerfl:new $R/ridetrackerfl:latest && docker push $R/ridetrackerfl:latest\n"
        "The next 10:00 AM run uses it."
    ),
    (
        "Check what the last daily scans did",
        "Read the report emails, or the AWS logs (after aws login):\n"
        "aws logs describe-log-streams --region us-east-2 --log-group-name /ecs/ridetrackerfl "
        "--order-by LastEventTime --descending --max-items 3\n"
        "aws logs get-log-events --region us-east-2 --log-group-name /ecs/ridetrackerfl "
        "--log-stream-name &lt;name from above&gt; --query 'events[].message' --output text"
    ),
    (
        "Scan log says \"Reached the N-slide cap for @account\"",
        "That account posted more stories than its limit, so the newest ones were missed. Raise "
        "max_slides for it in config/accounts.json (default 24; omg_cycling 60; alexbicycles, "
        "teamrecoveryweston, mbo_tencycling 40), commit, then build and push the image as above."
    ),
    (
        "Netlify warns about credit usage",
        "Every push to GitHub main is a production deploy (15 credits; 3,000 a month, resetting "
        "on the 18th). The sync only pushes when ride data changes. Count recent deploys with:\n"
        "git log --since=\"7 days ago\" --oneline | wc -l"
    ),
    (
        "Scraper fails at Phase 1 (Playwright)",
        "Instagram may be rate-limiting. Wait 30 minutes and try again.\n"
        "Check that your Instagram session cookies are still valid in config/instagram_cookies.json"
    ),
    (
        "Scraper fails at Phase 2 (Vision API)",
        "Check your Anthropic API key in config/secrets.env is valid and has credit.\n"
        "Note: an Ollama pre-filter (--use-ollama flag) exists to cut API costs by ~60-70% but is currently OFF.\n"
        "It requires Ollama + llama3.2-vision installed locally. Pending Mac mini migration."
    ),
    (
        "Sync fails / git push error",
        "Make sure you have internet connection. Run git push manually:\n"
        "cd ~/ridetrackerfl && git push"
    ),
    (
        "Ride showing wrong date",
        "Fix the date directly in Airtable, then run the sync command."
    ),
    (
        "New organizer ride not appearing",
        "Add manually in Airtable with Display on Site = true, then run sync.\n"
        "To add the organizer to the scraper, add their handle to config/accounts.json"
    ),
    (
        "Organizer logo/avatar not showing (showing initials instead)",
        "Two possible causes:\n"
        "1. Active checkbox unchecked — Go to Airtable → Organizers → find organizer → check Active → run sync.\n"
        "2. Logo not yet synced — run sync to download and cache it permanently:\n"
        "   cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site"
    ),
    (
        "Duplicate ride appearing after scraper run",
        "The scraper created a new record instead of updating the existing one.\n"
        "Hide the duplicate in Airtable — uncheck Display on Site (keep the older record with the full history).\n"
        "This was caused by a time format mismatch — fixed in v2.2. Should not recur."
    ),
    (
        "Weather showing wrong values",
        "Weather is refreshed automatically by the sync from Open-Meteo.\n"
        "If values look stale, just run the sync command to force a fresh fetch:\n"
        "cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site"
    ),
    (
        "Adding a new organizer logo",
        "Upload the logo to the Avatar field in Airtable → Organizers table.\n"
        "Make sure Active is checked.\n"
        "Run sync — the logo is automatically downloaded to public/organizers/ and committed to GitHub.\n"
        "It will never expire or need re-syncing unless you change the image in Airtable."
    ),
]

for title, detail in issues:
    story.append(Paragraph(title, h3_style))
    story.append(Paragraph(detail.replace("\n", "<br/>"), note_style))

story.append(Spacer(1, 16))

# ── Key files ─────────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph("Key Files & Locations", h2_style))

files = [
    ("config/accounts.json", "List of Instagram accounts the scraper watches"),
    ("config/secrets.env", "API keys — Anthropic, Airtable (never commit to GitHub)"),
    ("config/instagram_cookies.json", "Instagram session — refresh if scraper loses login"),
    ("data/rides_database.json", "Local ride cache — gitignored, stays on your machine"),
    ("public/rides.json", "Static file served to site visitors — generated by sync"),
    ("public/organizers/", "Organizer avatar images — downloaded once, served permanently from Netlify"),
    ("logs/sync.log", "Sync job logs — check here if something seems off"),
    ("docs/ROADMAP.md", "Current state, app plan and backlog"),
    ("data/organizers_cache.json", "Organizers saved by the hourly sync (refreshed every 12h)"),
    ("pipeline/run_scan.py", "Main scraper pipeline orchestrator"),
    ("pipeline/sync_to_site.py", "Airtable → rides.json sync script"),
]

file_data = [[
    Paragraph("File", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Purpose", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
]]
for path, desc in files:
    file_data.append([
        Paragraph(path, ParagraphStyle("fp", fontSize=9, fontName="Courier", textColor=BASE, leading=13)),
        Paragraph(desc, ParagraphStyle("fd", fontSize=9.5, fontName="Helvetica", textColor=MUTED, leading=13)),
    ])

file_table = Table(file_data, colWidths=[2.8*inch, 3.5*inch])
file_table.setStyle(TableStyle([
    ("BACKGROUND",    (0,0), (-1,0),  colors.HexColor("#f0fdf4")),
    ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ("BOX",           (0,0), (-1,-1), 0.5, BORDER),
    ("INNERGRID",     (0,0), (-1,-1), 0.5, BORDER),
    ("TOPPADDING",    (0,0), (-1,-1), 6),
    ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ("LEFTPADDING",   (0,0), (-1,-1), 10),
    ("RIGHTPADDING",  (0,0), (-1,-1), 10),
    ("VALIGN",        (0,0), (-1,-1), "TOP"),
]))
story.append(file_table)
story.append(Spacer(1, 20))

# ── Footer ─────────────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph(
    f"RideTrackerFL — ridetrackerfl.com  ·  Generated {datetime.now().strftime('%B %d, %Y')}",
    ParagraphStyle("footer", fontSize=9, fontName="Helvetica", textColor=MUTED,
                   alignment=TA_CENTER, spaceBefore=8)
))

doc.build(story)
print(f"PDF written to {OUTPUT}")
