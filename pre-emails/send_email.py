#!/usr/bin/env python3
"""
send_email.py — Send the DPLA pre-ingest email via AWS SES.

Reads a prepared txt file from pre-emails/txt_emails/ and sends it as an
HTML email with the DPLA logo at the top.

TEST MODE (current): sends only to TEST_EMAIL.
PRODUCTION (when ready): To: ingest@dp.la, BCC: all hub contact emails from i3.conf.
To switch, set TEST_MODE = False below.

Usage:
    python3 pre-emails/send_email.py                        # send latest txt file
    python3 pre-emails/send_email.py --file pre-ingest-2026-05_20260513T143022.txt
    python3 pre-emails/send_email.py --dry-run              # print without sending
    python3 pre-emails/send_email.py --aws-profile dpla     # override AWS profile
"""

import argparse
import base64
import os
import re
import sys
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Mode toggle ───────────────────────────────────────────────────────────────
# Flip to False when ready to send to real hub contacts
TEST_MODE  = True
TEST_EMAIL = "spieges124@gmail.com"

SENDER     = "ingest@dp.la"
PROD_TO    = "ingest@dp.la"
# BCC pulled from i3.conf when TEST_MODE = False

LOGO_PATH  = Path(__file__).resolve().parent / "standard_images" / "image.png"
LOGO_CID   = "dpla-logo"


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
    """Return the most recently modified txt file in txt_dir."""
    files = sorted(txt_dir.glob("pre-ingest-*.txt"), key=lambda f: f.stat().st_mtime)
    if not files:
        print(f"Error: no txt files found in {txt_dir}", file=sys.stderr)
        sys.exit(1)
    return files[-1]


def parse_txt(txt_path: Path) -> tuple[str, str]:
    """
    Parse subject and full body from a pre-ingest txt file.
    Returns (subject, body).
    """
    content = txt_path.read_text(encoding="utf-8")

    # Find the EMAIL BODY section
    marker = "EMAIL BODY"
    if marker not in content:
        print(f"Error: could not find '{marker}' section in {txt_path}", file=sys.stderr)
        sys.exit(1)

    body_section = content.split(marker, 1)[1]
    # Strip the separator line
    body_section = re.sub(r'^=+\n', '', body_section.lstrip("\n"))

    # Extract subject line
    subject_match = re.search(r'^Subject:\s*(.+)$', body_section, re.MULTILINE)
    if not subject_match:
        print(f"Error: no Subject line found in {txt_path}", file=sys.stderr)
        sys.exit(1)

    subject = subject_match.group(1).strip()

    # Body is everything after the Subject line
    body = body_section[subject_match.end():].strip()

    return subject, body


# ── Plain text → HTML ─────────────────────────────────────────────────────────

def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def body_to_html(body: str) -> str:
    """Convert the structured plain-text email body to clean HTML."""
    lines = body.splitlines()
    html_lines = []
    in_list = False

    for line in lines:
        # Section headers like --- GUIDELINES ---
        header_match = re.match(r'^---\s+(.+?)\s+---\s*$', line)
        if header_match:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f'<h3 style="color:#1a1a2e;border-bottom:1px solid #ddd;padding-bottom:4px;margin-top:28px">{_escape(header_match.group(1))}</h3>')
            continue

        # Hub list items: "  - Hub Name"
        hub_match = re.match(r'^\s{2}-\s+(.+)$', line)
        if hub_match:
            if not in_list:
                html_lines.append('<ul style="margin:6px 0;padding-left:24px">')
                in_list = True
            html_lines.append(f'<li style="margin:2px 0">{_escape(hub_match.group(1))}</li>')
            continue

        # Close list if we exit bullet territory
        if in_list and not hub_match:
            html_lines.append("</ul>")
            in_list = False

        # Numbered list items: "1. ..." or "2. ..."
        num_match = re.match(r'^(\d+)\.\s+(.+)$', line)
        if num_match:
            html_lines.append(
                f'<p style="margin:8px 0"><strong>{num_match.group(1)}.</strong> {_escape(num_match.group(2))}</p>'
            )
            continue

        # Continuation lines (indented under a numbered item)
        if re.match(r'^\s{3,}', line) and line.strip():
            html_lines.append(f'<p style="margin:4px 0 4px 24px;color:#333">{_escape(line.strip())}</p>')
            continue

        # Blank line → paragraph break
        if not line.strip():
            html_lines.append('<p style="margin:0">&nbsp;</p>')
            continue

        # Regular line
        html_lines.append(f'<p style="margin:6px 0">{_escape(line)}</p>')

    if in_list:
        html_lines.append("</ul>")

    return "\n".join(html_lines)


def build_html_email(body: str, logo_cid: str) -> str:
    """Wrap body HTML in a full email template with the logo at the top."""
    body_html = body_to_html(body)
    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Georgia,serif;color:#1a1a1a">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5">
    <tr><td align="center" style="padding:24px 16px">
      <table width="600" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:4px;overflow:hidden;
                    box-shadow:0 1px 4px rgba(0,0,0,.12)">

        <!-- Logo -->
        <tr>
          <td align="center" style="padding:28px 40px 20px">
            <img src="cid:{logo_cid}" alt="DPLA" width="200"
                 style="display:block;max-width:200px;height:auto">
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:0 40px 36px;font-size:15px;line-height:1.6;color:#1a1a1a">
            {body_html}
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ── BCC list from i3.conf ─────────────────────────────────────────────────────

def get_hub_emails(conf_path: str) -> list[str]:
    """Pull all hub contact emails from i3.conf for BCC."""
    text = Path(conf_path).read_text(encoding="utf-8")
    emails = set()
    for m in re.finditer(r'^[a-z0-9_-]+\.email\s*=\s*"([^"]+)"', text, re.MULTILINE):
        for addr in m.group(1).split(","):
            addr = addr.strip()
            if addr:
                emails.add(addr)
    return sorted(emails)


# ── Build MIME message ────────────────────────────────────────────────────────

def build_message(subject: str, plain_body: str,
                  to_addrs: list[str]) -> MIMEMultipart:
    """
    Build a multipart/related HTML email with the DPLA logo as an inline CID image.
    Falls back to plain text only if the logo file is missing.
    """
    # Outer envelope
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(to_addrs)

    if LOGO_PATH.exists():
        html_body = build_html_email(plain_body, LOGO_CID)

        # multipart/alternative holds the plain + html fallback pair
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain_body, "plain", "utf-8"))
        alt.attach(MIMEText(html_body,  "html",  "utf-8"))
        msg.attach(alt)

        # Inline logo
        logo_data = LOGO_PATH.read_bytes()
        img = MIMEImage(logo_data, _subtype="png")
        img.add_header("Content-ID", f"<{LOGO_CID}>")
        img.add_header("Content-Disposition", "inline", filename=LOGO_PATH.name)
        msg.attach(img)
    else:
        print(f"  Warning: logo not found at {LOGO_PATH} — sending plain text",
              file=sys.stderr)
        msg = MIMEText(plain_body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"]    = SENDER
        msg["To"]      = ", ".join(to_addrs)

    return msg


# ── Send ──────────────────────────────────────────────────────────────────────

def send(subject: str, body: str, aws_profile: str, conf_path: str) -> bool:
    try:
        import boto3
    except ImportError:
        print("Error: boto3 not installed. Run: pip install boto3 --break-system-packages",
              file=sys.stderr)
        return False

    if TEST_MODE:
        to_addrs  = [TEST_EMAIL]
        bcc_addrs = []
        print(f"  TEST MODE — sending only to {TEST_EMAIL}")
        print(f"  (flip TEST_MODE = False in send_email.py to send for real)")
    else:
        to_addrs  = [PROD_TO]
        bcc_addrs = get_hub_emails(conf_path)
        print(f"  PRODUCTION — To: {PROD_TO}, BCC: {len(bcc_addrs)} hub contacts")

    msg          = build_message(subject, body, to_addrs)
    destinations = to_addrs + bcc_addrs

    try:
        session = boto3.Session(profile_name=aws_profile)
        ses = session.client("ses", region_name=AWS_REGION)
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
    parser.add_argument("--file", default=None,
                        help="txt file to send (default: latest in txt_emails/)")
    parser.add_argument("--i3-conf", default=None,
                        help=f"Path to i3.conf for BCC list (default: {DEFAULT_CONF_PATH})")
    parser.add_argument("--aws-profile", default="dpla",
                        help="AWS profile for SES (default: dpla)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and print the plain-text body without sending")
    args = parser.parse_args()

    txt_dir = Path(__file__).resolve().parent / "txt_emails"

    if args.file:
        txt_path = txt_dir / args.file if not Path(args.file).is_absolute() else Path(args.file)
    else:
        txt_path = latest_txt_file(txt_dir)

    if not txt_path.exists():
        print(f"Error: file not found: {txt_path}", file=sys.stderr)
        sys.exit(1)

    subject, body = parse_txt(txt_path)
    conf_path = args.i3_conf or os.environ.get("I3_CONF") or DEFAULT_CONF_PATH

    print(f"File:    {txt_path.name}")
    print(f"Subject: {subject}")
    print(f"Logo:    {'found' if LOGO_PATH.exists() else 'MISSING — will send plain text'}")
    print()

    if args.dry_run:
        print("--- DRY RUN — not sending ---")
        print()
        print(f"Subject: {subject}")
        print()
        print(body)
        return

    ok = send(subject, body, args.aws_profile, conf_path)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
