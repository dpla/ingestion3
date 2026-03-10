#!/usr/bin/env python3
"""
Monthly pre-scheduling email: one summary email to all contacts of hubs
scheduled for a given month (from i3.conf). Informs hubs of the last calendar
week ingest window and asks for data readiness or skip requests.

Usage:
  python -m scheduler.orchestrator.scheduling_emails --month=2 --dry-run
  python -m scheduler.orchestrator.scheduling_emails --month=2 --draft
  python -m scheduler.orchestrator.scheduling_emails --month=2 --send

CC on every send: ingest@dp.la, dominic@dp.la
"""

import argparse
import calendar
import os
import sys
from datetime import date
from email.mime.text import MIMEText
from pathlib import Path

# CC on every scheduling email
CC_ALWAYS = ["ingest@dp.la", "dominic@dp.la"]
SENDER = "DPLA Bot <tech@dp.la>"

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def last_calendar_week_range(year: int, month: int) -> tuple[date, date]:
    """Return (start_date, end_date) for the last calendar week (Mon–Sun) of the month."""
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    # Monday of that week (weekday() 0=Monday, 6=Sunday)
    start = last_day
    for _ in range(7):
        if start.weekday() == 0:
            break
        start = date(start.year, start.month, start.day - 1)
    return start, last_day


def format_date_range(start: date, end: date) -> str:
    """Format as 'Month D – Month D, YYYY' (no leading zero on day)."""
    return f"{start.strftime('%B')} {start.day} – {end.strftime('%B')} {end.day}, {end.year}"


def build_preview_and_payload(config, month: int, year: int):
    """
    Load config, get scheduled hubs, build hub list with display names and email status,
    collect deduplicated To addresses, and build subject/body.
    Returns (preview_lines, hub_entries, to_emails, subject, body).
    """
    from scheduler.orchestrator.config import Config, load_config
    if not isinstance(config, Config):
        config = load_config(i3_conf_path=str(config) if config else None)

    hubs = config.get_scheduled_hubs(month)
    if not hubs:
        return (
            ["No hubs scheduled for this month."],
            [],
            [],
            f"DPLA Monthly Ingest Notification – {MONTH_NAMES[month - 1]} {year}",
            "",
        )

    start, end = last_calendar_week_range(year, month)
    date_range_str = format_date_range(start, end)
    month_name = MONTH_NAMES[month - 1]

    hub_entries = []  # (display_line, has_email)
    all_emails = set()
    for hub_name in hubs:
        hc = config.get_hub_config(hub_name)
        provider = (hc.provider or hub_name).strip() or hub_name
        if provider and provider != hub_name:
            display_line = f"{hub_name} ({provider})"
        else:
            display_line = hub_name
        emails = [e.strip() for e in (hc.email or "").split(",") if e.strip()]
        has_email = bool(emails)
        if has_email:
            all_emails.update(emails)
        else:
            display_line = f"{display_line} (no email in i3.conf – add manually if needed)"
        hub_entries.append((display_line, has_email))

    preview_lines = [
        "Preview",
        "-------",
        f"Date range (last calendar week): {date_range_str}",
        "Hubs included:",
    ]
    for display_line, _ in hub_entries:
        preview_lines.append(f"  • {display_line}")
    preview_lines.append("")

    to_emails = sorted(all_emails)
    subject = f"DPLA Monthly Ingest Notification – {month_name} {year}"

    # For body we list hubs without the "(no email...)" suffix so the email text is clean
    hub_list_body = "\n".join(
        f"• {display.replace(' (no email in i3.conf – add manually if needed)', '')}"
        for display, _ in hub_entries
    )

    body = f"""Dear DPLA Hub Contacts,

This is a reminder that DPLA will run metadata ingests during the last calendar week of {month_name} {year}.

Date range: {date_range_str}

Hubs included this month:
{hub_list_body}

Please ensure your metadata feed or data export is ready before this window. If you need to be skipped this month or have any issues (e.g. feed changes, downtime), please reply to this email or contact tech@dp.la as soon as possible.

Best regards,
DPLA Content Team
"""

    return preview_lines, hub_entries, to_emails, subject, body


def print_preview(preview_lines: list[str]) -> None:
    for line in preview_lines:
        print(line)


def run_dry_run(config, month: int, year: int) -> None:
    preview_lines, _, to_emails, subject, body = build_preview_and_payload(config, month, year)
    print_preview(preview_lines)
    print("To:", ", ".join(to_emails) if to_emails else "(no addresses from i3.conf)")
    print("CC:", ", ".join(CC_ALWAYS))
    print()
    print("Subject:", subject)
    print()
    print("Body:")
    print("-" * 40)
    print(body)
    print("-" * 40)


def run_draft(config, month: int, year: int, emails_dir: Path) -> Path | None:
    preview_lines, _, to_emails, subject, body = build_preview_and_payload(config, month, year)
    print_preview(preview_lines)

    emails_dir.mkdir(parents=True, exist_ok=True)
    draft_path = emails_dir / f"scheduling-{year}-{month:02d}.txt"
    content = f"To: {', '.join(to_emails) if to_emails else '(no addresses)'}\nCC: {', '.join(CC_ALWAYS)}\n\nSubject: {subject}\n\n{body}"
    draft_path.write_text(content, encoding="utf-8")
    print(f"Draft written: {draft_path}")
    return draft_path


def run_send(config, month: int, year: int, aws_profile: str) -> bool:
    import boto3

    preview_lines, _, to_emails, subject, body = build_preview_and_payload(config, month, year)
    print_preview(preview_lines)

    if not to_emails:
        print("No recipient addresses (no hub emails in i3.conf). Refusing to send.")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = ", ".join(to_emails)
    msg["Cc"] = ", ".join(CC_ALWAYS)
    dest = to_emails + CC_ALWAYS
    raw = msg.as_string()

    try:
        session = boto3.Session(profile_name=aws_profile)
        ses = session.client("ses", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        ses.send_raw_email(
            Source="tech@dp.la",
            Destinations=dest,
            RawMessage={"Data": raw.encode("utf-8")},
        )
        print(f"Sent to {len(to_emails)} To and {len(CC_ALWAYS)} CC.")
        return True
    except Exception as e:
        print(f"Send failed: {e}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monthly pre-scheduling email: one summary to all hubs scheduled for the month (from i3.conf)"
    )
    parser.add_argument("--month", type=int, default=None, help="Target month 1–12 (default: current month)")
    parser.add_argument("--dry-run", action="store_true", help="Print preview, To/CC, and body; no draft or send")
    parser.add_argument("--draft", action="store_true", help="Print preview and write draft file to scheduler/emails/")
    parser.add_argument("--send", action="store_true", help="Print preview and send via SES (CC: ingest@dp.la, dominic@dp.la)")
    parser.add_argument("--i3-conf", type=Path, default=None, help="Path to i3.conf (default: from env I3_CONF)")
    parser.add_argument("--aws-profile", default="dpla", help="AWS profile for SES (default: dpla)")
    args = parser.parse_args()

    if not args.dry_run and not args.draft and not args.send:
        parser.error("One of --dry-run, --draft, or --send is required")

    from scheduler.orchestrator.config import load_config
    config = load_config(i3_conf_path=str(args.i3_conf) if args.i3_conf else None)

    now = date.today()
    year = now.year
    month = args.month if args.month is not None else now.month
    if not (1 <= month <= 12):
        print("Error: --month must be 1–12", file=sys.stderr)
        sys.exit(1)

    # Year when --month is explicit: e.g. in Jan asking for Dec = previous year; in Dec asking for Feb = next year
    if args.month is not None:
        if now.month == 1 and args.month == 12:
            year = now.year - 1
        elif now.month == 12 and args.month < 12:
            year = now.year + 1

    if args.dry_run:
        run_dry_run(config, month, year)
    elif args.draft:
        repo_root = Path(__file__).resolve().parent.parent.parent
        emails_dir = repo_root / "scheduler" / "emails"
        run_draft(config, month, year, emails_dir)
    else:
        ok = run_send(config, month, year, args.aws_profile)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
