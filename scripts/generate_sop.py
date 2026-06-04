from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from datetime import datetime

OUTPUT = "/sessions/epic-awesome-bell/mnt/ridetrackerfl/RideTrackerFL_Daily_SOP.pdf"

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
    "There are two daily tasks: running the scraper and syncing any manual Airtable edits. "
    "The scraper handles everything automatically end-to-end. Manual edits require a separate sync command. "
    "Both commands are run from the terminal inside the ridetrackerfl project folder.",
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
        Paragraph("Run the daily scraper", body_style),
        Paragraph("python3.12 -m pipeline.run_scan", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
    ],
    [
        Paragraph("Sync manual Airtable edits to site", body_style),
        Paragraph("python3.12 -m pipeline.sync_to_site", ParagraphStyle("c", fontSize=9.5, fontName="Courier", textColor=BASE, leading=14)),
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
story.append(Paragraph("Flow 1 — Daily Scraper Run", h2_style))
story.append(Paragraph(
    "Run this once or twice a day (typically morning and evening). "
    "The scraper handles everything in a single command:",
    body_style
))
story.append(Spacer(1, 6))
story.append(code_block("cd ~/ridetrackerfl && python3.12 -m pipeline.run_scan"))
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
story.append(Paragraph("After the scraper finishes:", h3_style))
story.append(Paragraph("Open Airtable and review the runs output. Check for:", body_style))
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
    "Any time you make a change directly in Airtable — fixing a date, adding a ride from "
    "an organizer email, toggling Display on Site — run the sync command to push it live:",
    body_style
))
story.append(Spacer(1, 6))
story.append(code_block("cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site"))
story.append(Spacer(1, 8))
story.append(Paragraph(
    "The site updates within 60 seconds. "
    "If you forget to sync, the site just shows the previous data until the next scraper run.",
    note_style
))

story.append(Spacer(1, 16))

# ── Troubleshooting ────────────────────────────────────────────────────────────
story.append(section_rule())
story.append(Paragraph("Troubleshooting", h2_style))

issues = [
    (
        "Site shows old data",
        "Run the sync command manually:\n"
        "cd ~/ridetrackerfl && python3.12 -m pipeline.sync_to_site"
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
        "Delete the duplicate in Airtable (keep the older record with the full history).\n"
        "This was caused by a time format mismatch — fixed in v2.2. Should not recur."
    ),
    (
        "Weather showing wrong values",
        "Weather is refreshed automatically on every sync from Open-Meteo.\n"
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
