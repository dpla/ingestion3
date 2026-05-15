#!/usr/bin/env python3
"""
send_email.py — Send the DPLA pre-ingest email via AWS SES.

Reads a prepared txt file from pre-emails/txt_emails/ and sends it as a
styled HTML email matching the DPLA ingest email template.

TEST MODE (current): sends only to TEST_EMAIL.
PRODUCTION (when ready): To: ingest@dp.la, BCC: all hub contact emails from i3.conf.
To switch, set TEST_MODE = False below.

Usage:
    python3 pre-emails/send_email.py                        # send latest txt file
    python3 pre-emails/send_email.py --file pre-ingest-2026-05_20260513T143022.txt
    python3 pre-emails/send_email.py --dry-run              # print HTML to stdout
    python3 pre-emails/send_email.py --aws-profile dpla     # override AWS profile
"""

import argparse
import os
import re
import sys
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Mode toggle ───────────────────────────────────────────────────────────────
TEST_MODE  = False
TEST_EMAIL = "spieges124@gmail.com"

SENDER   = "ingest@dp.la"
PROD_TO  = "ingest@dp.la"

LOGO_PATH = Path(__file__).resolve().parent / "standard_images" / "image.png"
LOGO_CID  = "dpla-logo"
COVER_CID = "cover-image"

# Brand colours
SIDEBAR_COLOR = "#4a6d8c"
HEADER_COLOR  = "#1a2a3a"
TITLE_COLOR   = "#1a2a3a"
BODY_COLOR    = "#1a1a1a"
LINK_COLOR    = "#2a5a8c"


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
    creds = cfg.get("AWS_SHARED_CREDENTIALS_FILE")
    if creds:
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", creds)
    return cfg


_env = _load_dotenv()
DEFAULT_CONF_REPO = _env.get(
    "INGESTION3_CONF_REPO",
    os.path.expanduser("~/Documents/Repos/ingestion3-conf"),
)
DEFAULT_CONF_PATH = os.path.join(DEFAULT_CONF_REPO, "i3.conf")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


# ── txt file helpers ──────────────────────────────────────────────────────────

def latest_txt_file(txt_dir: Path) -> Path:
    files = sorted(txt_dir.glob("pre-ingest-*.txt"), key=lambda f: f.stat().st_mtime)
    if not files:
        print(f"Error: no txt files found in {txt_dir}", file=sys.stderr)
        sys.exit(1)
    return files[-1]


def parse_txt(txt_path: Path) -> tuple[str, str]:
    """Parse subject and body from the EMAIL BODY section. Returns (subject, body)."""
    content = txt_path.read_text(encoding="utf-8")
    if "EMAIL BODY" not in content:
        print(f"Error: no 'EMAIL BODY' section in {txt_path}", file=sys.stderr)
        sys.exit(1)
    body_section = content.split("EMAIL BODY", 1)[1]
    body_section = re.sub(r'^=+\n', '', body_section.lstrip("\n"))
    subject_match = re.search(r'^Subject:\s*(.+)$', body_section, re.MULTILINE)
    if not subject_match:
        print(f"Error: no Subject line in {txt_path}", file=sys.stderr)
        sys.exit(1)
    subject = subject_match.group(1).strip()
    body    = body_section[subject_match.end():].strip()
    return subject, body


def parse_month(txt_path: Path) -> int:
    """Extract the target month (1–12) from the txt filename or header."""
    # Fastest: filename always contains pre-ingest-YYYY-MM
    fm = re.search(r'pre-ingest-\d{4}-(\d{2})', txt_path.name)
    if fm:
        return int(fm.group(1))
    # Fallback: parse "MONTH: May 2026" from header
    month_names = ["january","february","march","april","may","june",
                   "july","august","september","october","november","december"]
    try:
        content = txt_path.read_text(encoding="utf-8").split("EMAIL BODY", 1)[0]
        mm = re.search(r'^MONTH:\s+(\w+)', content, re.MULTILINE)
        if mm:
            return month_names.index(mm.group(1).lower()) + 1
    except (OSError, ValueError):
        pass
    from datetime import date
    return date.today().month


def parse_cover_image(txt_path: Path) -> tuple[str, str]:
    """Extract cover image local path and caption from txt header. Returns (path, caption)."""
    content  = txt_path.read_text(encoding="utf-8")
    pre_body = content.split("EMAIL BODY", 1)[0]
    local_m   = re.search(r'^\s+Local:\s+(.+)$',   pre_body, re.MULTILINE)
    caption_m = re.search(r'^\s+Caption:\s+(.+)$', pre_body, re.MULTILINE)
    return (
        local_m.group(1).strip()   if local_m   else "",
        caption_m.group(1).strip() if caption_m else "",
    )


# ── BCC list from i3.conf ─────────────────────────────────────────────────────

def get_hub_emails(conf_path: str, month: int) -> list[str]:
    """Return bare email addresses for hubs scheduled in `month` (skips on-hold)."""
    text     = Path(conf_path).read_text(encoding="utf-8")
    hub_keys = set(re.findall(r'^([a-z0-9_-]+)\.provider\s*=', text, re.MULTILINE))
    emails   = set()

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

        if month not in get_months():
            continue
        if "on-hold" in get_val("schedule.status").lower():
            continue

        for part in get_val("email").split(","):
            part = part.strip()
            if not part:
                continue
            # Strip display names: "First Last<addr@example.com>" → "addr@example.com"
            addr_m = re.search(r'<([^>]+)>', part)
            addr   = addr_m.group(1).strip() if addr_m else part
            if "@" in addr:
                emails.add(addr)

    return sorted(emails)


# ── Plain text → HTML body ────────────────────────────────────────────────────

def _esc(t: str) -> str:
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _linkify(t: str) -> str:
    """Turn markdown links, email addresses, and bare URLs into hyperlinks."""
    link_style = f'color:{LINK_COLOR};text-decoration:underline'
    # markdown-style links: [text](url)
    t = re.sub(
        r'\[([^\]]+)\]\((https?://[^\)]+)\)',
        rf'<a href="\2" style="{link_style}" target="_blank">\1</a>',
        t,
    )
    # email addresses
    t = re.sub(
        r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})',
        rf'<a href="mailto:\1" style="{link_style}">\1</a>',
        t,
    )
    # bare https:// URLs not already in an href
    t = re.sub(
        r'(?<!href=")(https?://[^\s<>"]+)',
        rf'<a href="\1" style="{link_style}" target="_blank">\1</a>',
        t,
    )
    return t


def _bold_week(t: str) -> str:
    """Bold the ingest week date range, e.g. 'the week of May 25–29'."""
    return re.sub(
        r'(the week of\s+)([A-Z][a-z]+ \d+[–\-]\d+|\d+[–\-]\d+)',
        r'\1<strong>\2</strong>',
        t,
    )


def _section_title(raw: str) -> str:
    """Convert '--- IMPORTANT NOTES ---' → 'Important Notes'."""
    words = raw.strip().split()
    return " ".join(w.capitalize() for w in words)


def body_to_html(body: str) -> str:
    """
    Convert structured plain-text body to HTML matching the DPLA email design.

    Handles:
      --- SECTION --- → rust-red h2
      1. Item / 2. Item → ordered list with sub-bullet support
         (indented continuation paragraphs become sub-bullets)
      "  - Hub Name" → italic square-bullet list
      Regular paragraphs → <p>
    """
    paragraphs = re.split(r'\n{2,}', body.strip())
    chunks = []  # list of (type, content) tuples
    for para in paragraphs:
        # Use raw lines (before stripping) for hub detection — strip() removes leading spaces
        raw_lines = para.splitlines()
        para      = para.strip()
        if not para:
            continue
        header_m = re.match(r'^---\s+(.+?)\s+---$', para)
        num_m    = re.match(r'^(\d+)\.\s+(.+)', para, re.DOTALL)

        # A hub block: every non-empty raw line starts with "  - "
        is_hub_block = (
            any(re.match(r'^\s{2}-\s+', ln) for ln in raw_lines) and
            all(re.match(r'^\s{2}-\s+', ln) or not ln.strip() for ln in raw_lines)
        )

        if header_m:
            chunks.append(("header", header_m.group(1)))
        elif num_m:
            chunks.append(("numbered", (num_m.group(1), num_m.group(2).strip())))
        elif is_hub_block:
            # Expand multi-line hub block into individual hub chunks
            for ln in raw_lines:
                hub_m = re.match(r'^\s{2}-\s+(.+)', ln)
                if hub_m:
                    chunks.append(("hub", hub_m.group(1).strip()))
        else:
            chunks.append(("para", para))

    html = []
    i = 0
    while i < len(chunks):
        kind, val = chunks[i]

        if kind == "header":
            html.append(
                f'<h2 style="font-family:Georgia,serif;color:{HEADER_COLOR};'
                f'font-size:22px;margin:28px 0 10px;font-weight:bold">'
                f'{_section_title(val)}</h2>'
            )
            i += 1

        elif kind == "numbered":
            # Open an ordered list and collect consecutive numbered + sub-bullet items
            html.append(
                '<ol style="margin:8px 0;padding-left:24px;font-family:Georgia,serif">'
            )
            while i < len(chunks) and chunks[i][0] in ("numbered", "para"):
                k2, v2 = chunks[i]
                if k2 == "numbered":
                    num, text = v2
                    text_html = _linkify(_bold_week(_esc(text)))
                    # Peek ahead for sub-paragraphs
                    sub_items = []
                    j = i + 1
                    while j < len(chunks) and chunks[j][0] == "para":
                        sub_items.append(chunks[j][1])
                        j += 1
                    li_html = (
                        f'<li style="margin:6px 0;line-height:1.6">{text_html}'
                    )
                    if sub_items:
                        li_html += (
                            '<ul style="list-style-type:square;margin:6px 0;'
                            'padding-left:20px;color:#333">'
                        )
                        for sub in sub_items:
                            sub_html = _linkify(_esc(sub))
                            li_html += (
                                f'<li style="margin:4px 0;line-height:1.6">{sub_html}</li>'
                            )
                        li_html += '</ul>'
                        i = j
                    else:
                        i += 1
                    li_html += '</li>'
                    html.append(li_html)
                else:
                    # lone para between numbered items — shouldn't normally happen
                    i += 1
            html.append('</ol>')

        elif kind == "hub":
            # Collect all consecutive hub items into one italic list
            html.append(
                '<ul style="list-style-type:square;margin:8px 0;padding-left:24px;'
                'font-family:Georgia,serif">'
            )
            while i < len(chunks) and chunks[i][0] == "hub":
                hub_name = _esc(chunks[i][1])
                html.append(
                    f'<li style="margin:3px 0;font-style:italic">{hub_name}</li>'
                )
                i += 1
            html.append('</ul>')

        else:  # para
            text_html = _linkify(_bold_week(_esc(val)))
            html.append(
                f'<p style="margin:10px 0;line-height:1.6;font-family:Georgia,serif">'
                f'{text_html}</p>'
            )
            i += 1

    return "\n".join(html)


# ── Full HTML email template ──────────────────────────────────────────────────

def build_html_email(subject: str, body: str,
                     cover_caption: str = "",
                     has_cover: bool = False) -> str:
    body_html    = body_to_html(body)
    cover_block  = ""
    if has_cover:
        caption_html = (
            f'<p style="text-align:center;font-size:12px;color:#666;'
            f'font-style:italic;margin:6px 0 0;font-family:Georgia,serif">'
            f'{_esc(cover_caption)}</p>'
            if cover_caption else ""
        )
        cover_block = f"""\
        <!-- Cover image -->
        <tr>
          <td align="center" style="padding:4px 40px 20px">
            <img src="cid:{COVER_CID}" alt="Cover image"
                 width="520" style="display:block;max-width:100%;height:auto">
            {caption_html}
          </td>
        </tr>"""

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#e0e4e8;font-family:Georgia,serif;color:{BODY_COLOR}">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#e0e4e8">
  <tr><td align="center" style="padding:24px 16px">

    <!-- Card with blue sidebars -->
    <table width="620" cellpadding="0" cellspacing="0"
           style="background:#ffffff;border-collapse:collapse">
      <tr>
        <!-- Left sidebar -->
        <td width="18" style="background:{SIDEBAR_COLOR}">&nbsp;</td>

        <!-- Main content column -->
        <td>
          <table width="100%" cellpadding="0" cellspacing="0">

            <!-- Logo -->
            <tr>
              <td align="center" style="padding:28px 40px 16px">
                <img src="cid:{LOGO_CID}" alt="DPLA" width="220"
                     style="display:block;max-width:220px;height:auto">
              </td>
            </tr>

            <!-- Title -->
            <tr>
              <td align="center" style="padding:0 40px 16px">
                <h1 style="margin:0;font-size:28px;font-weight:normal;
                           color:{TITLE_COLOR};font-family:Georgia,serif;
                           letter-spacing:-0.3px">
                  {_esc(subject)}
                </h1>
              </td>
            </tr>

            <!-- Divider -->
            <tr>
              <td style="padding:0 40px 20px">
                <div style="border-top:1px solid #cccccc"></div>
              </td>
            </tr>

            {cover_block}

            <!-- Body -->
            <tr>
              <td style="padding:0 40px 36px;font-size:15px;line-height:1.6">
                {body_html}
              </td>
            </tr>

          </table>
        </td>

        <!-- Right sidebar -->
        <td width="18" style="background:{SIDEBAR_COLOR}">&nbsp;</td>
      </tr>
    </table>

  </td></tr>
</table>
</body>
</html>"""


# ── Build MIME message ────────────────────────────────────────────────────────

def build_message(subject: str, plain_body: str, to_addrs: list[str],
                  cover_img_path: str = "", cover_caption: str = "") -> MIMEMultipart:
    has_cover = bool(cover_img_path and Path(cover_img_path).exists())
    html_body = build_html_email(subject, plain_body, cover_caption, has_cover)

    # multipart/related wraps HTML + inline images
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(to_addrs)

    # multipart/alternative: plain text fallback + HTML
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body,  "html",  "utf-8"))
    msg.attach(alt)

    # Inline logo
    if LOGO_PATH.exists():
        img = MIMEImage(LOGO_PATH.read_bytes(), _subtype="png")
        img.add_header("Content-ID", f"<{LOGO_CID}>")
        img.add_header("Content-Disposition", "inline", filename=LOGO_PATH.name)
        msg.attach(img)
    else:
        print(f"  Warning: logo not found at {LOGO_PATH}", file=sys.stderr)

    # Inline cover image
    if has_cover:
        cover_path = Path(cover_img_path)
        subtype    = cover_path.suffix.lstrip(".").lower() or "jpeg"
        img = MIMEImage(cover_path.read_bytes(), _subtype=subtype)
        img.add_header("Content-ID", f"<{COVER_CID}>")
        img.add_header("Content-Disposition", "inline", filename=cover_path.name)
        msg.attach(img)

    return msg


# ── Send ──────────────────────────────────────────────────────────────────────

def send(subject: str, body: str, aws_profile: str, conf_path: str,
         cover_img_path: str = "", cover_caption: str = "",
         override_to: str = "", month: int = 0) -> bool:
    try:
        import boto3
    except ImportError:
        print("Error: boto3 not installed. Run: pip install boto3 --break-system-packages",
              file=sys.stderr)
        return False

    if override_to:
        to_addrs  = [override_to]
        bcc_addrs = []
        print(f"  DIRECT SEND — To: {override_to} (no BCC)")
    elif TEST_MODE:
        to_addrs  = [TEST_EMAIL]
        bcc_addrs = []
        print(f"  TEST MODE — sending only to {TEST_EMAIL}")
        print(f"  (flip TEST_MODE = False in send_email.py to send for real)")
    else:
        to_addrs  = [PROD_TO]
        bcc_addrs = get_hub_emails(conf_path, month)
        print(f"  PRODUCTION — To: {PROD_TO}, BCC: {len(bcc_addrs)} hub contacts (month {month})")
        for addr in bcc_addrs:
            print(f"    {addr}")
        print()
        confirm = input("Send? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return False

    msg          = build_message(subject, body, to_addrs, cover_img_path, cover_caption)
    destinations = to_addrs + bcc_addrs

    try:
        session = boto3.Session(profile_name=aws_profile)
        ses     = session.client("ses", region_name=AWS_REGION)
        ses.send_raw_email(
            Source=SENDER,
            Destinations=destinations,
            RawMessage={"Data": msg.as_string().encode("utf-8")},
        )
        print("  Sent.")
        return True
    except Exception as e:
        print(f"  Send failed: {e}", file=sys.stderr)
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Send the DPLA pre-ingest email via AWS SES"
    )
    parser.add_argument("--file",        default=None,
                        help="txt file to send (default: latest in txt_emails/)")
    parser.add_argument("--i3-conf",     default=None,
                        help=f"Path to i3.conf for BCC list (default: {DEFAULT_CONF_PATH})")
    parser.add_argument("--aws-profile", default="dpla",
                        help="AWS profile for SES (default: dpla)")
    parser.add_argument("--to",          default="",
                        help="Send directly to this address only (no BCC, overrides TEST_MODE)")
    parser.add_argument("--dry-run",     action="store_true",
                        help="Write rendered HTML to stdout instead of sending")
    args = parser.parse_args()

    txt_dir = Path(__file__).resolve().parent / "txt_emails"

    if args.file:
        txt_path = txt_dir / args.file if not Path(args.file).is_absolute() else Path(args.file)
    else:
        txt_path = latest_txt_file(txt_dir)

    if not txt_path.exists():
        print(f"Error: file not found: {txt_path}", file=sys.stderr)
        sys.exit(1)

    subject, body           = parse_txt(txt_path)
    cover_img_path, caption = parse_cover_image(txt_path)
    conf_path = args.i3_conf or os.environ.get("I3_CONF") or DEFAULT_CONF_PATH

    print(f"File:    {txt_path.name}")
    print(f"Subject: {subject}")
    print(f"Logo:    {'found' if LOGO_PATH.exists() else 'MISSING'}")
    print(f"Cover:   {cover_img_path or '(none)'}")
    print()

    if args.dry_run:
        has_cover = bool(cover_img_path and Path(cover_img_path).exists())
        html = build_html_email(subject, body, caption, has_cover)
        print(html)
        return

    month = parse_month(txt_path)
    ok = send(subject, body, args.aws_profile, conf_path, cover_img_path, caption, args.to, month)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
