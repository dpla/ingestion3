#!/usr/bin/env python3
"""
Send a single harvest-failure notification email to tech@dp.la via AWS SES.

Usage: send-harvest-failure-email.py <hub> "<error message>" ["<email body>"]

When a third argument (email body) is provided, it is used as the email body
verbatim — the Scala pipeline (OaiHarvestException.buildEmailBody) is the
single source of truth for the notification content.

When only two arguments are given (backward-compat / manual invocation), the
script wraps the error message in a simple template.

Environment:
  AWS_PROFILE  - AWS profile for SES (default: dpla)
  AWS_REGION   - SES region (default: us-east-1)

Best-effort: on failure (no credentials, SES not verified, etc.) prints to
stderr and exits non-zero; the caller should not block on this.
"""

import os
import sys
from email.mime.text import MIMEText

TECH_EMAIL = "tech@dp.la"
SENDER = "DPLA Bot <tech@dp.la>"


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: send-harvest-failure-email.py <hub> <error message> [<email body>]", file=sys.stderr)
        sys.exit(1)
    hub = sys.argv[1]
    error_msg = sys.argv[2]
    # When the Scala pipeline provides the complete email body, use it as-is.
    email_body = sys.argv[3] if len(sys.argv) > 3 else None

    if email_body:
        body = email_body
    else:
        body = f"""DPLA Harvest Failure

Hub: {hub}

Error:
{error_msg}

---
If you have questions, contact tech@dp.la.
"""
    subject = f"[DPLA Ingest] Harvest failure: {hub}"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = TECH_EMAIL

    try:
        import boto3
    except ImportError:
        print("Warning: boto3 not installed; skipping harvest failure email.", file=sys.stderr)
        sys.exit(0)

    profile = os.environ.get("AWS_PROFILE", "dpla")
    region = os.environ.get("AWS_REGION", "us-east-1")
    try:
        session = boto3.Session(profile_name=profile)
        ses = session.client("ses", region_name=region)
        ses.send_raw_email(
            Source=SENDER,
            Destinations=[TECH_EMAIL],
            RawMessage={"Data": msg.as_string().encode("utf-8")},
        )
        print(f"Harvest failure email sent to {TECH_EMAIL} for {hub}")
    except Exception as e:
        print(f"Warning: Failed to send harvest failure email: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
