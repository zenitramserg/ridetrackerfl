from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from datetime import datetime

OUTPUT = "/sessions/epic-awesome-bell/mnt/ridetrackerfl/RideTrackerFL_Architecture_v2.2.pdf"

doc = SimpleDocTemplate(
    OUTPUT,
    pagesize=letter,
    rightMargin=0.85*inch,
    leftMargin=0.85*inch,
    topMargin=0.85*inch,
    bottomMargin=0.85*inch,
)

BASE    = colors.HexColor("#0d0d0d")
MUTED   = colors.HexColor("#525252")
ACCENT  = colors.HexColor("#059669")
BLUE    = colors.HexColor("#1d4ed8")
AMBER   = colors.HexColor("#b45309")
RED     = colors.HexColor("#b91c1c")
CODE_BG = colors.HexColor("#f4f4f4")
BORDER  = colors.HexColor("#e5e5e5")
GREEN_BG= colors.HexColor("#f0fdf4")
GREEN_BD= colors.HexColor("#bbf7d0")
RED_BG  = colors.HexColor("#fef2f2")
RED_BD  = colors.HexColor("#fecaca")
BLUE_BG = colors.HexColor("#eff6ff")
BLUE_BD = colors.HexColor("#bfdbfe")
AMBER_BG= colors.HexColor("#fffbeb")
AMBER_BD= colors.HexColor("#fde68a")

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
code_style = ParagraphStyle("code", fontSize=9, fontName="Courier",
    textColor=BASE, leading=13)
bullet_style = ParagraphStyle("bullet", fontSize=10, fontName="Helvetica",
    textColor=BASE, spaceAfter=3, leading=14, leftIndent=16)

def hr():
    return HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=4, spaceBefore=4)

def code_block(lines):
    rows = [[Paragraph(l, code_style)] for l in lines]
    t = Table(rows, colWidths=[6.3*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), CODE_BG),
        ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
        ("TOPPADDING",    (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 8),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
    ]))
    return t

def badge(text, bg, bd, tc):
    p = Paragraph(text, ParagraphStyle("badge", fontSize=8.5, fontName="Helvetica-Bold",
        textColor=tc, leading=12))
    t = Table([[p]], colWidths=[None])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), bg),
        ("BOX",           (0,0),(-1,-1), 0.5, bd),
        ("TOPPADDING",    (0,0),(-1,-1), 3),
        ("BOTTOMPADDING", (0,0),(-1,-1), 3),
        ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ("RIGHTPADDING",  (0,0),(-1,-1), 8),
    ]))
    return t

def flow_table(rows_data, col_widths):
    t = Table(rows_data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
        ("INNERGRID",     (0,0),(-1,-1), 0.5, BORDER),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [colors.white, colors.HexColor("#fafafa")]),
        ("TOPPADDING",    (0,0),(-1,-1), 7),
        ("BOTTOMPADDING", (0,0),(-1,-1), 7),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("RIGHTPADDING",  (0,0),(-1,-1), 10),
        ("VALIGN",        (0,0),(-1,-1), "TOP"),
    ]))
    return t

story = []

# ── Header ────────────────────────────────────────────────────────────────────
story.append(Paragraph("RideTrackerFL", title_style))
story.append(Paragraph("Platform Architecture & Release History — v2.2", subtitle_style))
story.append(hr())
story.append(Paragraph(
    f"Document version: 2.2  ·  Released: {datetime.now().strftime('%B %d, %Y')}  ·  "
    "ridetrackerfl.com  ·  @ridetrackerfl",
    note_style
))
story.append(Spacer(1, 16))

# ── What is RideTrackerFL ──────────────────────────────────────────────────────
story.append(Paragraph("What is RideTrackerFL", h2_style))
story.append(Paragraph(
    "RideTrackerFL is a free cycling ride discovery platform for the Weston, South Florida area. "
    "It automatically scrapes Instagram stories from local cycling groups and shops, extracts ride details "
    "using Claude vision AI, stores them in Airtable, and publishes them to a static website. "
    "Rides are updated twice daily and displayed with weather forecasts, pace, distance, and organizer details.",
    body_style
))
story.append(Spacer(1, 8))

# ── Architecture Overview ─────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph("Current Architecture (v2.2)", h2_style))

arch_data = [
    [
        Paragraph("Layer", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
        Paragraph("Technology", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
        Paragraph("Purpose", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    ],
    [
        Paragraph("Scraper", body_style),
        Paragraph("Playwright + Python", code_style),
        Paragraph("Logs into Instagram, captures story screenshots for each account in accounts.json", note_style),
    ],
    [
        Paragraph("Vision AI", body_style),
        Paragraph("Claude Sonnet (Anthropic API)", code_style),
        Paragraph("Analyzes screenshots, extracts ride details (date, time, pace, location) as structured JSON", note_style),
    ],
    [
        Paragraph("Vision pre-filter", body_style),
        Paragraph("Ollama llama3.2-vision (OFF)", code_style),
        Paragraph("Optional local pre-filter that rejects non-ride slides before they reach Claude API, cutting ~60-70% of API costs. Currently OFF — requires Ollama installed locally. Pending Mac mini migration.", note_style),
    ],
    [
        Paragraph("Deduplication", body_style),
        Paragraph("Python pipeline", code_style),
        Paragraph("Matches new extractions to existing rides, updates recurring weekly rides with new dates", note_style),
    ],
    [
        Paragraph("Data store", body_style),
        Paragraph("Airtable + rides_database.json", code_style),
        Paragraph("Airtable is the editorial source of truth. Local JSON is a cache used by the scraper pipeline", note_style),
    ],
    [
        Paragraph("Static export", body_style),
        Paragraph("sync_to_site.py", code_style),
        Paragraph("Pulls display_on_site=true rides from Airtable, generates public/rides.json, commits to GitHub", note_style),
    ],
    [
        Paragraph("Hosting", body_style),
        Paragraph("Netlify (auto-deploy on git push)", code_style),
        Paragraph("Serves static files. Deploys automatically when rides.json is pushed to GitHub", note_style),
    ],
    [
        Paragraph("Frontend", body_style),
        Paragraph("Vanilla JS + HTML", code_style),
        Paragraph("Reads public/rides.json — zero Airtable API calls from visitors", note_style),
    ],
    [
        Paragraph("Avatar cache", body_style),
        Paragraph("public/organizers/", code_style),
        Paragraph("Organizer logos downloaded once from Airtable, stored as static files on Netlify. Never expire, no re-download unless image changes in Airtable.", note_style),
    ],
    [
        Paragraph("Weather", body_style),
        Paragraph("Open-Meteo API (free, no key)", code_style),
        Paragraph("Fetches 7-day forecast per ride at scrape time and live on page load", note_style),
    ],
]

arch_table = Table(arch_data, colWidths=[1.2*inch, 1.8*inch, 3.3*inch])
arch_table.setStyle(TableStyle([
    ("BACKGROUND",    (0,0),(-1,0),  GREEN_BG),
    ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
    ("INNERGRID",     (0,0),(-1,-1), 0.5, BORDER),
    ("TOPPADDING",    (0,0),(-1,-1), 7),
    ("BOTTOMPADDING", (0,0),(-1,-1), 7),
    ("LEFTPADDING",   (0,0),(-1,-1), 10),
    ("RIGHTPADDING",  (0,0),(-1,-1), 10),
    ("VALIGN",        (0,0),(-1,-1), "TOP"),
]))
story.append(arch_table)
story.append(Spacer(1, 16))

# ── Data Flow ─────────────────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph("Data Flow", h2_style))

story.append(Paragraph("Automated scraper run:", h3_style))
flow1 = [
    "Instagram stories  →  Playwright scraper captures screenshots",
    "Screenshots  →  Claude vision API extracts ride JSON",
    "Ride JSON  →  Deduplicator matches / updates rides_database.json",
    "rides_database.json  →  Airtable sync (push_new_rides / push_updated_rides)",
    "Airtable  →  sync_to_site.py generates public/rides.json",
    "public/rides.json  →  git push  →  Netlify deploys  →  Site updated",
]
story.append(code_block(flow1))
story.append(Spacer(1, 10))

story.append(Paragraph("Manual Airtable edit:", h3_style))
flow2 = [
    "Sergio edits ride in Airtable (fix date, add ride, toggle visibility)",
    "Run:  python3.12 -m pipeline.sync_to_site",
    "sync_to_site.py pulls Airtable  →  writes rides.json  →  git push  →  Netlify deploys",
]
story.append(code_block(flow2))
story.append(Spacer(1, 10))

story.append(Paragraph("Visitor page load (v2.2 — zero API calls):", h3_style))
flow3 = [
    "Browser requests ridetrackerfl.com",
    "Netlify serves static index.html + public/rides.json  (no Airtable call)",
    "JS fetches Open-Meteo weather API for live forecast",
    "Rides rendered from rides.json — max 15 min stale under cron, instant after manual sync",
]
story.append(code_block(flow3))
story.append(Spacer(1, 16))

# ── Accounts ──────────────────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph("Monitored Instagram Accounts (v2.2)", h2_style))

accounts = [
    ("omg_cycling",        "OMG Cycling",          "Group",  "Active"),
    ("alexbicycles",       "Alex's Bicycle Pro Shop","Shop",  "Active"),
    ("mbo_tencycling",     "MBO Ten Cycling",       "Shop",   "Active"),
    ("weston_flyers",      "Weston Flyers",         "Group",  "Active"),
    ("teamrecoveryweston", "Team Recovery Weston",  "Group",  "Active"),
    ("fpbikeshop",         "FP Bike Shop",          "Shop",   "Active"),
    ("unicosta_cycling",   "Unicosta Cycling",      "Group",  "Active"),
    ("pd.cyclingclub",     "PD Cycling Club",       "Group",  "Active"),
    ("ride.84",            "Ride 84",               "Group",  "Active"),
    ("alligators.cycling", "Alligators Cycling",    "Shop",   "Active — added v1.2"),
    ("revoltcyclery",      "Revolt Cyclery",        "Shop",   "Paused — organizer request"),
    ("letourdeweston",     "Le Tour de Weston",     "Event",  "Inactive — seasonal"),
]

acc_header = [[
    Paragraph("Handle", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Display Name", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Type", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Status", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
]]
acc_rows = acc_header + [
    [
        Paragraph(f"@{h}", ParagraphStyle("c", fontSize=9, fontName="Courier", textColor=BASE, leading=13)),
        Paragraph(n, note_style),
        Paragraph(t, note_style),
        Paragraph(s, ParagraphStyle("s", fontSize=9, fontName="Helvetica",
            textColor=ACCENT if "Active" in s and "added" not in s
            else MUTED if "Paused" in s or "Inactive" in s
            else BLUE, leading=13)),
    ]
    for h, n, t, s in accounts
]

acc_table = Table(acc_rows, colWidths=[1.5*inch, 1.8*inch, 0.8*inch, 2.2*inch])
acc_table.setStyle(TableStyle([
    ("BACKGROUND",    (0,0),(-1,0),  GREEN_BG),
    ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
    ("INNERGRID",     (0,0),(-1,-1), 0.5, BORDER),
    ("TOPPADDING",    (0,0),(-1,-1), 6),
    ("BOTTOMPADDING", (0,0),(-1,-1), 6),
    ("LEFTPADDING",   (0,0),(-1,-1), 10),
    ("RIGHTPADDING",  (0,0),(-1,-1), 10),
    ("VALIGN",        (0,0),(-1,-1), "TOP"),
]))
story.append(acc_table)
story.append(Spacer(1, 16))

# ── Release History ───────────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph("Release History", h2_style))

releases = [
    {
        "version": "v2.2",
        "date": "June 4, 2026",
        "title": "Weather & Deduplication Fixes",
        "type": "MINOR",
        "type_color": AMBER,
        "type_bg": AMBER_BG,
        "type_bd": AMBER_BD,
        "changes": [
            "Auto-refresh weather on every sync — rides within 7 days get fresh Open-Meteo forecast written back to Airtable",
            "Weather fetched by time-of-day (hourly) not just date — afternoon rides get correct evening forecast",
            "Fixed time format parsing: '7:00AM' (no space) now correctly parses to hour 7 instead of defaulting to 6 AM",
            "Fixed duplicate ride creation caused by time format mismatch ('06:00 AM' vs '6:00 AM')",
            "Time normalisation applied to both scraper match key and Airtable index — leading zeros stripped for consistent comparison",
        ]
    },
    {
        "version": "v2.1",
        "date": "June 4, 2026",
        "title": "Permanent Organizer Avatars",
        "type": "MINOR",
        "type_color": AMBER,
        "type_bg": AMBER_BG,
        "type_bd": AMBER_BD,
        "changes": [
            "Organizer logos now stored as static files in public/organizers/ — never expire",
            "sync_to_site.py downloads avatars from Airtable on first sync, skips if unchanged (size check)",
            "Eliminated 410 Gone errors caused by Airtable attachment URL expiry (~1 hour TTL)",
            "New organizer logos auto-download on next sync after being uploaded to Airtable",
            "Updated logo detected via file size comparison — re-downloaded automatically",
            "Fixed .gitignore to allow webp/jpg/jpeg/png in public/organizers/ only",
            "Featured events section compacted — social links and I'm Riding hidden until expanded",
        ]
    },
    {
        "version": "v2.0",
        "date": "June 3, 2026",
        "title": "Static JSON Architecture",
        "type": "MAJOR",
        "type_color": RED,
        "type_bg": RED_BG,
        "type_bd": RED_BD,
        "changes": [
            "Eliminated all visitor-driven Airtable API calls — site now serves static rides.json",
            "Built sync_to_site.py — pulls from Airtable, generates public/rides.json, git pushes",
            "Wired sync_to_site into run_scan.py as Phase 6 — one command does everything",
            "Airtable API usage capped at ~96 calls/day (cron) regardless of visitor traffic",
            "Added footer disclaimer: ride details sourced from Instagram, confirm with organizer",
            "Documented daily operations in SOP PDF",
        ]
    },
    {
        "version": "v1.2",
        "date": "May 29, 2026",
        "title": "Reliability & Scraper Fixes",
        "type": "MINOR",
        "type_color": AMBER,
        "type_bg": AMBER_BG,
        "type_bd": AMBER_BD,
        "changes": [
            "Fixed vision prompt year bug — injected today's date so model never guesses 2025",
            "Fixed push_updated_rides() 404 drift — stale record IDs now cleared on failure",
            "Fixed empty string sentinel 422 error on singleSelect Airtable fields",
            "Removed two false-positive rides (Arepa Power, Unicosta) from database",
            "Added @alligators.cycling to accounts.json — scraper now monitors their stories",
            "New laptop migration — reinstalled ARM-native Python 3.12 + Playwright on MacBook",
        ]
    },
    {
        "version": "v1.1",
        "date": "May 27–29, 2026",
        "title": "Organizer Onboarding & Content",
        "type": "MINOR",
        "type_color": AMBER,
        "type_bg": AMBER_BG,
        "type_bd": AMBER_BD,
        "changes": [
            "Onboarded Alligators Cycling (Pembroke Pines) — Saturday rides listed",
            "Onboarded Team Galiz — Saturday, Sunday, Wednesday rides listed",
            "Added MBO Robert Is Here Ride (May 30) — 75mi destination ride to Homestead",
            "Added MBO 1st Anniversary Ride (July 5) — 62mi, 1-year celebration event",
            "Instagram profile improvements — bio updated, Story Highlights added",
            "Organizer tagging in stories implemented for reach growth",
        ]
    },
    {
        "version": "v1.0",
        "date": "April 2026",
        "title": "Initial Launch",
        "type": "MAJOR",
        "type_color": RED,
        "type_bg": RED_BG,
        "type_bd": RED_BD,
        "changes": [
            "Playwright scraper monitoring 10 Instagram accounts in Weston, FL area",
            "Claude vision API pipeline for ride extraction from story screenshots",
            "Airtable as editorial database with full ride schema",
            "Netlify-hosted static site with live Airtable API calls (pre-v2.0)",
            "Weather integration via Open-Meteo (free, no API key)",
            "Featured rides, pace/distance/organizer details on ride cards",
            "Instagram account @ridetrackerfl launched",
        ]
    },
]

for rel in releases:
    story.append(Spacer(1, 6))

    # Version header row
    ver_data = [[
        Paragraph(f"<b>{rel['version']}</b> — {rel['title']}", ParagraphStyle(
            "verh", fontSize=11, fontName="Helvetica-Bold", textColor=BASE, leading=14)),
        Paragraph(rel['date'], ParagraphStyle(
            "verd", fontSize=9.5, fontName="Helvetica", textColor=MUTED, leading=14)),
        Paragraph(rel['type'], ParagraphStyle(
            "vert", fontSize=8.5, fontName="Helvetica-Bold",
            textColor=rel['type_color'], leading=12)),
    ]]
    ver_table = Table(ver_data, colWidths=[3.2*inch, 1.5*inch, 1.6*inch])
    ver_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), rel['type_bg']),
        ("BOX",           (0,0),(-1,-1), 0.5, rel['type_bd']),
        ("TOPPADDING",    (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 8),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 10),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ("ALIGN",         (2,0),(-1,-1), "RIGHT"),
    ]))
    story.append(ver_table)

    # Change list
    change_rows = [
        [Paragraph(f"  •  {c}", ParagraphStyle("ch", fontSize=9.5, fontName="Helvetica",
            textColor=BASE, leading=14, spaceAfter=0))]
        for c in rel['changes']
    ]
    change_table = Table(change_rows, colWidths=[6.3*inch])
    change_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), colors.white),
        ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
        ("TOPPADDING",    (0,0),(-1,-1), 5),
        ("BOTTOMPADDING", (0,0),(-1,-1), 5),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
    ]))
    story.append(change_table)

story.append(Spacer(1, 16))

# ── Pending Backlog ────────────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph("Pending Backlog (next releases)", h2_style))

backlog = [
    ("High",   "Cron job automation",          "Set up 15-min cron on Mac mini for fully hands-off syncing"),
    ("High",   "Netlify deploy hook",           "Airtable button to trigger immediate site sync without terminal"),
    ("High",   "Ollama pre-filter (Mac mini)",  "Enable --use-ollama flag on scraper runs. Cuts ~60-70% of Claude vision API calls by rejecting non-ride slides locally before they reach the API. Requires Ollama + llama3.2-vision installed on Mac mini. Pending Mac mini migration."),
    ("Medium", "Pace color badges",             "Green/yellow/red dot on ride cards by pace group (C/B/A)"),
    ("Medium", "Google Event JSON-LD",          "Structured data markup for Google Event rich results in search"),
    ("Medium", "Semantic HTML day headers",     "H2/H3 for date section headers (currently divs) — SEO improvement"),
    ("Low",    "Mobile touch target audit",     "Verify all tap targets are 48x48px minimum"),
    ("Low",    "Weekly Sunday recap post",      "Instagram post: rides this week, total riders out in Weston"),
]

bl_header = [[
    Paragraph("Priority", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Item", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
    Paragraph("Description", ParagraphStyle("th", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE)),
]]
bl_rows = bl_header + [
    [
        Paragraph(p, ParagraphStyle("pri", fontSize=9, fontName="Helvetica-Bold", leading=13,
            textColor=RED if p=="High" else AMBER if p=="Medium" else MUTED)),
        Paragraph(i, ParagraphStyle("it", fontSize=9.5, fontName="Helvetica-Bold", textColor=BASE, leading=13)),
        Paragraph(d, note_style),
    ]
    for p, i, d in backlog
]

bl_table = Table(bl_rows, colWidths=[0.7*inch, 1.8*inch, 3.8*inch])
bl_table.setStyle(TableStyle([
    ("BACKGROUND",    (0,0),(-1,0),  GREEN_BG),
    ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#fafafa")]),
    ("BOX",           (0,0),(-1,-1), 0.5, BORDER),
    ("INNERGRID",     (0,0),(-1,-1), 0.5, BORDER),
    ("TOPPADDING",    (0,0),(-1,-1), 6),
    ("BOTTOMPADDING", (0,0),(-1,-1), 6),
    ("LEFTPADDING",   (0,0),(-1,-1), 10),
    ("RIGHTPADDING",  (0,0),(-1,-1), 10),
    ("VALIGN",        (0,0),(-1,-1), "TOP"),
]))
story.append(bl_table)
story.append(Spacer(1, 20))

# ── Footer ─────────────────────────────────────────────────────────────────────
story.append(hr())
story.append(Paragraph(
    f"RideTrackerFL v2.2  ·  ridetrackerfl.com  ·  "
    f"Generated {datetime.now().strftime('%B %d, %Y')}",
    ParagraphStyle("footer", fontSize=9, fontName="Helvetica", textColor=MUTED,
                   alignment=TA_CENTER, spaceBefore=8)
))

doc.build(story)
print(f"PDF written to {OUTPUT}")
