#!/usr/bin/env python3
"""
send_email.py — Send the DPLA pre-ingest email via AWS SES.

Reads a prepared txt file from pre-emails/txt_emails/ and sends it.

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
import os
import re
import sys
from email.mime.text import MIMEText
from pathlib import Path

# ── Mode toggle ───────────────────────────────────────────────────────────────
# Flip to False when ready to send to real hub contacts
TEST_MODE  = True
TEST_EMAIL = "spieges124@gmail.com"

SENDER     = "ingest@dp.la"
PROD_TO    = "ingest@dp.la"
# BCC pulled from i3.conf when TEST_MODE = False


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

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(to_addrs)
    # BCC: not added to headers (that's what makes it a BCC)

    destinations = to_addrs + bcc_addrs

    try:
        session = boto3.Session(profile_name=aws_profile)
        ses = session.client("ses", region_name=AWS_REGION)
        ses.send_raw_email(
            Source=SENDER,
            Destinations=destinations,
            RawMessage={"Data": msg.as_string().encode("utf-8")},
        )
        print(f"  Sent.")
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
                        help="Parse and print without sending")
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
