#!/usr/bin/env python3
"""
get_info.py — Gather all dynamic content for the DPLA pre-ingest email.

Reads i3.conf to find hubs scheduled for a given month, resolves their
provider display names, calculates the ingest week, and writes everything
to a plain-text file ready for the email template.

All user-supplied content (subject, intro, image) is passed as flags —
no interactive prompts. Designed to be called by a Claude skill.

Usage:
    python3 pre-emails/get_info.py --month 5 --year 2026
    python3 pre-emails/get_info.py --month 5 --year 2026 --dry-run
    python3 pre-emails/get_info.py --month 5 --year 2026 \\
        --subject "DPLA Ingests for May 2026" \\
        --intro "DPLA partners, it is harvest time!..." \\
        --image-url "https://example.com/image.jpg" \\
        --image-caption "Harvest Time, courtesy Illinois Digital Heritage Hub"

Output file (default): pre-emails/txt_emails/pre-ingest-<YYYY>-<MM>_<timestamp>.txt
"""

import argparse
import calendar
import os
import re
import sys
import urllib.request
from datetime import date, datetime
from pathlib import Path


# ── .env loader ───────────────────────────────────────────────────────────────

def _load_dotenv() -> dict:
    cfg = {}
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    return cfg


_env = _load_dotenv()
DEFAULT_CONF_REPO = _env.get(
    "INGESTION3_CONF_REPO",
    os.path.expanduser("~/Documents/Repos/ingestion3-conf"),
)
DEFAULT_CONF_PATH = os.path.join(DEFAULT_CONF_REPO, "i3.conf")


# ── i3.conf parsing ───────────────────────────────────────────────────────────

def parse_i3_conf(conf_path: str) -> dict[str, dict]:
    text = Path(conf_path).read_text(encoding="utf-8")
    hub_keys = set(re.findall(r'^([a-z0-9_-]+)\.provider\s*=', text, re.MULTILINE))

    hubs = {}
    for hub in hub_keys:
        def get_val(key, _hub=hub):
            m = re.search(rf'^{re.escape(_hub)}\.{re.escape(key)}\s*=\s*"([^"]*)"',
                          text, re.MULTILINE)
            return m.group(1).strip() if m else ""

        def get_months(_hub=hub):
            m = re.search(rf'^{re.escape(_hub)}\.schedule\.months\s*=\s*\[([^\]]*)\]',
                          text, re.MULTILINE)
            if not m:
                return []
            return [int(x.strip()) for x in m.group(1).split(",") if x.strip().isdigit()]

        hubs[hub] = {
            "provider": get_val("provider") or hub,
            "months":   get_months(),
            "status":   get_val("schedule.status"),
        }
    return hubs


def get_scheduled_providers(hubs: dict, month: int) -> list[str]:
    seen = set()
    providers = []
    for info in hubs.values():
        if not info["months"]:
            continue
        if month not in info["months"]:
            continue
        if "on-hold" in info["status"].lower():
            continue
        p = info["provider"]
        if p not in seen:
            seen.add(p)
            providers.append(p)
    return sorted(providers)


# ── Date helpers ──────────────────────────────────────────────────────────────

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def last_full_work_week(year: int, month: int) -> tuple[date, date]:
    from datetime import timedelta
    last_day = calendar.monthrange(year, month)[1]
    friday = date(year, month, last_day)
    while friday.weekday() != 4:
        friday -= timedelta(days=1)
    monday = friday - timedelta(days=4)
    if monday.month != month:
        friday -= timedelta(days=7)
        monday = friday - timedelta(days=4)
    return monday, friday


def format_week_range(monday: date, friday: date) -> str:
    if monday.month == friday.month:
        return f"{monday.strftime('%B')} {monday.day}–{friday.day}"
    return f"{monday.strftime('%B')} {monday.day} – {friday.strftime('%B')} {friday.day}"


# ── Image fetch ───────────────────────────────────────────────────────────────

def fetch_image(url: str) -> str:
    """Download image to pre-emails/images/ and return the local path."""
    import urllib.parse
    import urllib.error

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        print(f"\n⚠️  Could not auto-download cover image: unsupported URL scheme '{parsed.scheme}'", file=sys.stderr)
        return ""

    images_dir = Path(__file__).resolve().parent / "images"
    url_path = url.split("?")[0].rstrip("/")
    ext = Path(url_path).suffix or ".jpg"
    filename = Path(url_path).name or f"cover{ext}"
    local_path = images_dir / filename

    def _print_manual_steps(reason: str) -> None:
        print(f"\n⚠️  Could not auto-download cover image: {reason}", file=sys.stderr)
        print(f"\nTo add it manually:", file=sys.stderr)
        print(f"  1. Download the image from:\n     {url}", file=sys.stderr)
        print(f"  2. Save it to:\n     {local_path}", file=sys.stderr)
        print(f"  3. Then add this line to the txt file under COVER IMAGE:", file=sys.stderr)
        print(f"       Local:   {local_path}", file=sys.stderr)
        print(f"\nThe email will send without the cover image until you do this.\n", file=sys.stderr)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            local_path.write_bytes(resp.read())
        return str(local_path)
    except urllib.error.HTTPError as e:
        _print_manual_steps(f"HTTP {e.code} {e.reason}")
    except urllib.error.URLError as e:
        _print_manual_steps(str(e.reason))
    except ValueError as e:
        _print_manual_steps(f"invalid URL — {e}")
    except Exception as e:
        _print_manual_steps(f"unexpected error — {e}")
    return ""


# ── Output builder ────────────────────────────────────────────────────────────

def build_output(month: int, year: int, providers: list[str],
                 week_str: str, monday: date, friday: date,
                 subject: str, intro_body: str,
                 image_url: str = "", image_path: str = "",
                 image_caption: str = "") -> str:
    month_name = MONTH_NAMES[month - 1]
    hub_list = "\n".join(f"  - {p}" for p in providers) if providers else "  (no hubs scheduled)"

    image_block = ""
    if image_url:
        image_block = f"\nCOVER IMAGE:\n  URL:     {image_url}\n"
        if image_path:
            image_block += f"  Local:   {image_path}\n"
        if image_caption:
            image_block += f"  Caption: {image_caption}\n"

    cover_section = (
        f"\n--- COVER IMAGE ---\n\n{image_caption}"
        if image_caption else ""
    )

    return f"""\
DPLA Pre-Ingest Email — {month_name} {year}
Generated: {date.today().isoformat()}
========================================

MONTH:       {month_name} {year}
INGEST WEEK: {week_str}
             ({monday.strftime('%A, %B %-d')} – {friday.strftime('%A, %B %-d, %Y')})

HUBS SCHEDULED ({len(providers)} total):
{hub_list}
{image_block}
========================================
EMAIL BODY
========================================

Subject: {subject}

{intro_body}

--- GUIDELINES ---

1. Please make sure your data is uploaded or up to date in your feed prior to
   the ingest week, as we may attempt to process you as early as 9am ET Monday
   morning of that week.

   Having the full week for ingest gives us a chance to work through any issues
   that may arise with your harvest prior to the indexing process that we
   initiate at the end of the week that creates the new DPLA aggregation.

2. Please let us know the number of records to expect in your harvest—and if
   you are providing a file export to DPLA, please let us know when we can
   expect delivery of the data to your s3 bucket.

   All communications regarding counts and export files can go to ingest@dp.la,
   which is a shared inbox for our team.

--- HUBS ---

The following hubs are scheduled for ingest in {month_name} {year}:

{hub_list}

--- IMPORTANT NOTES ---

We have documentation for our ingestion system to provide a high level overview
of how we map, normalize, validate and enrich your metadata, available at our
GitHub repo. After an ingest is complete, you should receive a report in your
inbox. This documentation also unpacks some of those warnings and error messages
you will see in your ingest report. You can view your hub's ingest schedule and
contacts at our Confluence wiki page.

As you probably know, DPLA is going through a transition currently, and we
experienced some hiccups in December & January. Often, we have found that if
something is amiss, the partners will be the first to notice. If at any time
you experience any issues, such as an ingest behind schedule, no ingest report
emailed to you, or if you need to make updates to any of the information found
there, please contact us right away so we can investigate.

--- CONTACT ---

If you need to talk with someone at DPLA, especially if there is an issue with
an ingest, please email ingest@dp.la or reach out directly to our Director of
Community Engagement, Dominic Byrd-McDevitt.
{cover_section}
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Gather dynamic content for the DPLA pre-ingest email"
    )
    parser.add_argument("--month", type=int, default=None,
                        help="Target month 1–12 (default: current month)")
    parser.add_argument("--year", type=int, default=None,
                        help="Target year (default: current year)")
    parser.add_argument("--i3-conf", default=None,
                        help=f"Path to i3.conf (default: {DEFAULT_CONF_PATH})")
    parser.add_argument("--subject", default=None,
                        help="Email subject line (default: 'DPLA Ingests for <Month> <Year>')")
    parser.add_argument("--intro", default=None,
                        help="Intro body text (everything before Guidelines)")
    parser.add_argument("--image-url", default="",
                        help="URL of the cover image to download and embed")
    parser.add_argument("--image-caption", default="",
                        help="Caption / credit line for the cover image")
    parser.add_argument("--out", default=None,
                        help="Output file path (default: txt_emails/pre-ingest-YYYY-MM_<ts>.txt)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print to stdout instead of writing a file")
    args = parser.parse_args()

    today = date.today()
    month = args.month if args.month is not None else today.month
    year  = args.year  if args.year  is not None else today.year

    if not (1 <= month <= 12):
        print("Error: --month must be 1–12", file=sys.stderr)
        sys.exit(1)

    conf_path = args.i3_conf or os.environ.get("I3_CONF") or DEFAULT_CONF_PATH
    if not os.path.exists(conf_path):
        print(f"Error: i3.conf not found at {conf_path}", file=sys.stderr)
        print("Set INGESTION3_CONF_REPO in your .env or pass --i3-conf", file=sys.stderr)
        sys.exit(1)

    hubs      = parse_i3_conf(conf_path)
    providers = get_scheduled_providers(hubs, month)
    monday, friday = last_full_work_week(year, month)
    week_str  = format_week_range(monday, friday)
    month_name = MONTH_NAMES[month - 1]

    # Defaults for optional content
    subject = args.subject or f"DPLA Ingests for {month_name} {year}"
    intro = args.intro or (
        f"DPLA partners, it is harvest time!\n\n"
        f"This is the new format for our pre-ingest notices. If you have thoughts on how\n"
        f"this information is presented, please feel free to let us know!\n\n"
        f"You are receiving this email because your hub's data is due to be re-ingested\n"
        f"this month. We always intend to run all ingests on the last full week of the\n"
        f"month. For this month, that is the week of {week_str}."
    )

    # Download image if URL provided
    image_path = fetch_image(args.image_url) if args.image_url else ""

    output = build_output(month, year, providers, week_str, monday, friday,
                          subject, intro, args.image_url, image_path, args.image_caption)

    if args.dry_run:
        print(output)
        return

    out_dir = Path(__file__).resolve().parent / "txt_emails"
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_path = args.out or str(out_dir / f"pre-ingest-{year}-{month:02d}_{timestamp}.txt")
    Path(out_path).write_text(output, encoding="utf-8")
    print(f"Written: {out_path}")
    print(f"  Month:       {month_name} {year}")
    print(f"  Ingest week: {week_str}")
    print(f"  Hubs:        {len(providers)}")


if __name__ == "__main__":
    main()
