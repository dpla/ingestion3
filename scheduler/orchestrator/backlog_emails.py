#!/usr/bin/env python3
"""
Backlog mapping summary emails: discover hubs with mapping output on S3
(Dec 2025, Jan 2026, Feb 2026) and send or draft ingest-summary emails.

Dry-run: list hubs that would be contacted, their emails, and ingest dates (YYYY-MM).
Send: use --send; CC scott@dp.la, tech@dp.la, dominic@dp.la on every email.
If no hub contact, send only to scott@dp.la, dominic@dp.la, tech@dp.la.
"""

import argparse
import email
import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from pathlib import Path

# CC on every notification
CC_ALWAYS = ["scott@dp.la", "tech@dp.la", "dominic@dp.la"]
# When no hub contact, send only to these
FALLBACK_TO = ["scott@dp.la", "dominic@dp.la", "tech@dp.la"]
SENDER = "DPLA Bot <tech@dp.la>"
MAX_ATTACHMENT_BYTES = 7_485_760  # ~7MB like Emailer.scala

# Backlog date prefixes (mapping run dirs start with YYYYMMDD_...)
BACKLOG_PREFIXES = ("202512", "202601", "202602")

# S3 prefix -> hub name (inverse of Config.S3_PREFIX_MAP)
S3_PREFIX_TO_HUB = {
    "hathitrust": "hathi",
    "tennessee": "tn",
}


def _s3_prefix_to_hub(s3_prefix: str) -> str:
    """Resolve hub name from S3 top-level prefix."""
    return S3_PREFIX_TO_HUB.get(s3_prefix, s3_prefix)


def _mapping_dir_to_yyyy_mm(mapping_dir_name: str) -> str | None:
    """Extract YYYY-MM from a mapping dir name like 20260202_041902-hub-MAP...."""
    if len(mapping_dir_name) >= 6 and mapping_dir_name[:6].isdigit():
        yyyy = mapping_dir_name[:4]
        mm = mapping_dir_name[4:6]
        return f"{yyyy}-{mm}"
    return None


def _discover_via_boto3(bucket: str, aws_profile: str) -> list[dict]:
    """Use boto3 to discover hubs and backlog mapping dates."""
    import boto3

    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client("s3")

    hub_prefixes: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/", Prefix=""):
        for cp in page.get("CommonPrefixes", []):
            prefix = cp["Prefix"].rstrip("/")
            if prefix and not prefix.startswith("."):
                hub_prefixes.append(prefix)

    result: list[dict] = []
    for s3_prefix in sorted(hub_prefixes):
        mapping_prefix = f"{s3_prefix}/mapping/"
        ingest_dates: set[str] = set()
        mapping_dirs: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Delimiter="/", Prefix=mapping_prefix):
            for cp in page.get("CommonPrefixes", []):
                full = cp["Prefix"].rstrip("/")
                mapping_dir_name = full.split("/")[-1]
                if any(mapping_dir_name.startswith(p) for p in BACKLOG_PREFIXES):
                    mapping_dirs.append(mapping_dir_name)
                    yyyy_mm = _mapping_dir_to_yyyy_mm(mapping_dir_name)
                    if yyyy_mm:
                        ingest_dates.add(yyyy_mm)
        if ingest_dates and mapping_dirs:
            hub_name = _s3_prefix_to_hub(s3_prefix)
            result.append({
                "hub_name": hub_name,
                "s3_prefix": s3_prefix,
                "ingest_dates_yyyy_mm": sorted(ingest_dates),
                "mapping_dirs": sorted(mapping_dirs),
            })
    return result


def _discover_via_aws_cli(bucket: str, aws_profile: str) -> list[dict]:
    """Use AWS CLI to discover hubs and backlog mapping dates (fallback when boto3 missing)."""
    import subprocess

    result: list[dict] = []
    # List top-level: aws s3 ls s3://bucket/ --profile dpla
    cmd = ["aws", "s3", "ls", f"s3://{bucket}/", "--profile", aws_profile]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        print(f"Warning: AWS CLI failed: {out.stderr}", file=sys.stderr)
        return result

    hub_prefixes = []
    for line in out.stdout.strip().splitlines():
        # PRE prefix/
        if line.strip().startswith("PRE "):
            prefix = line.strip()[4:].rstrip("/")
            if prefix and not prefix.startswith("."):
                hub_prefixes.append(prefix)

    for s3_prefix in sorted(hub_prefixes):
        cmd2 = ["aws", "s3", "ls", f"s3://{bucket}/{s3_prefix}/mapping/", "--profile", aws_profile]
        out2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=60)
        if out2.returncode != 0:
            continue
        ingest_dates: set[str] = set()
        mapping_dirs: list[str] = []
        for line in out2.stdout.strip().splitlines():
            if not line.strip().startswith("PRE "):
                continue
            mapping_dir_name = line.strip()[4:].rstrip("/")
            if any(mapping_dir_name.startswith(p) for p in BACKLOG_PREFIXES):
                mapping_dirs.append(mapping_dir_name)
                yyyy_mm = _mapping_dir_to_yyyy_mm(mapping_dir_name)
                if yyyy_mm:
                    ingest_dates.add(yyyy_mm)
        if ingest_dates and mapping_dirs:
            hub_name = _s3_prefix_to_hub(s3_prefix)
            result.append({
                "hub_name": hub_name,
                "s3_prefix": s3_prefix,
                "ingest_dates_yyyy_mm": sorted(ingest_dates),
                "mapping_dirs": sorted(mapping_dirs),
            })
    return result


def discover_backlog_hubs(
    bucket: str,
    aws_profile: str = "dpla",
) -> list[dict]:
    """
    List S3 top-level prefixes (hubs), then for each hub list mapping/ and
    collect mapping run dirs that start with 202512, 202601, 202602.
    Returns list of {hub_name, s3_prefix, ingest_dates_yyyy_mm}.
    Uses boto3 if available, else AWS CLI.
    """
    try:
        import boto3  # noqa: F401
        return _discover_via_boto3(bucket, aws_profile)
    except ImportError:
        return _discover_via_aws_cli(bucket, aws_profile)


def load_config_and_attach_emails(
    i3_conf_path: Path | None,
    rows: list[dict],
) -> list[dict]:
    """Load i3 config and attach email(s) and provider name for each hub."""
    project_root = Path(__file__).resolve().parent.parent.parent
    if project_root not in sys.path:
        sys.path.insert(0, str(project_root))

    from scheduler.orchestrator.config import load_config

    conf = load_config(i3_conf_path=str(i3_conf_path) if i3_conf_path else None)
    for row in rows:
        hub_config = conf.get_hub_config(row["hub_name"])
        if hub_config:
            row["emails"] = [e.strip() for e in hub_config.email.split(",") if e.strip()] if hub_config.email else []
            row["provider"] = hub_config.provider or row["hub_name"].upper()
        else:
            row["emails"] = []
            row["provider"] = row["hub_name"].upper()
    return rows


def _email_body_prefix_suffix():
    """Match Emailer.scala prefix and suffix."""
    prefix = """
This is an automated email summarizing the DPLA ingest. Please see attached ZIP file
for record level information about errors and warnings.

If you have questions please contact us at <a href="mailto:tech@dp.la">tech@dp.la</a>

- <a href="https://github.com/dpla/ingestion3/">Ingestion documentation</a>
""".strip().split("\n")
    suffix = """


Bleep bloop.

-----------------  END  -----------------
""".strip().split("\n")
    return prefix, suffix


def _fetch_summary_body(s3_client, bucket: str, key: str) -> str:
    """Fetch _SUMMARY from S3 and return HTML body (prefix + summary lines drop last 5 + suffix)."""
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        content = resp["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        content = "(Summary not available from S3.)"
    lines = content.strip().split("\n")
    if len(lines) > 5:
        lines = lines[:-5]
    prefix, suffix = _email_body_prefix_suffix()
    body_lines = prefix + lines + suffix
    return "<pre>" + "\n".join(body_lines) + "</pre>"


def _build_and_send_email(
    row: dict,
    bucket: str,
    aws_profile: str,
) -> tuple[bool, str]:
    """
    Build MIME message and send via SES. Returns (success, message).
    To: hub emails if present, else FALLBACK_TO. CC: CC_ALWAYS when hub has email.
    """
    import boto3

    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client("s3")
    ses = session.client("ses", region_name=os.environ.get("AWS_REGION", "us-east-1"))

    hub_name = row["hub_name"]
    s3_prefix = row["s3_prefix"]
    mapping_dirs = row["mapping_dirs"]
    provider = row.get("provider", hub_name.upper())
    hub_emails = row.get("emails") or []

    latest_mapping_dir = max(mapping_dirs)
    summary_key = f"{s3_prefix}/mapping/{latest_mapping_dir}/_SUMMARY"
    errors_zip_key = f"{s3_prefix}/mapping/{latest_mapping_dir}/_LOGS/errors.zip"

    body_html = _fetch_summary_body(s3, bucket, summary_key)

    if hub_emails:
        to_addrs = hub_emails
        cc_addrs = CC_ALWAYS
    else:
        to_addrs = FALLBACK_TO
        cc_addrs = []

    subject = f"DPLA Ingest Summary for {provider} - Backlog Dec 2025–Feb 2026"

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)

    part = MIMEText(body_html, "html", "utf-8")
    msg.attach(part)

    # Try errors.zip then logs.zip (pipeline/Emailer.scala writes logs.zip from _LOGS/errors/)
    attachment_data = None
    attachment_name = "errors.zip"
    for key, name in [
        (errors_zip_key, "errors.zip"),
        (f"{s3_prefix}/mapping/{latest_mapping_dir}/_LOGS/logs.zip", "logs.zip"),
    ]:
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            size = head.get("ContentLength", 0)
            if size and size <= MAX_ATTACHMENT_BYTES:
                resp = s3.get_object(Bucket=bucket, Key=key)
                attachment_data = resp["Body"].read()
                attachment_name = name
                break
        except Exception:
            continue

    # Fallback: build zip from _LOGS/errors/ if no zip object exists
    if not attachment_data:
        errors_prefix = f"{s3_prefix}/mapping/{latest_mapping_dir}/_LOGS/errors/"
        try:
            import io
            import zipfile
            buf = io.BytesIO()
            total = 0
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=errors_prefix):
                    for obj in page.get("Contents") or []:
                        key = obj["Key"]
                        if key == errors_prefix or not key.strip("/").split("/")[-1]:
                            continue
                        size = obj.get("Size", 0)
                        if total + size > MAX_ATTACHMENT_BYTES:
                            break
                        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                        zf.writestr(key.split("/")[-1], body)
                        total += size
                    if total >= MAX_ATTACHMENT_BYTES:
                        break
            buf.seek(0)
            if total > 0:
                raw = buf.getvalue()
                if len(raw) <= MAX_ATTACHMENT_BYTES:
                    attachment_data = raw
                    attachment_name = "errors.zip"
        except Exception:
            pass

    if attachment_data:
        att = MIMEBase("application", "zip")
        att.set_payload(attachment_data)
        email.encoders.encode_base64(att)
        att.add_header("Content-Disposition", "attachment", filename=attachment_name)
        msg.attach(att)

    raw = msg.as_string()
    dest = to_addrs + cc_addrs
    try:
        ses.send_raw_email(
            Source="tech@dp.la",
            Destinations=dest,
            RawMessage={"Data": raw.encode("utf-8")},
        )
        return True, f"Sent to {', '.join(dest)}" + (" (with " + attachment_name + ")" if attachment_data else " (no attachment)")
    except Exception as e:
        return False, str(e)


def run_send(
    bucket: str,
    aws_profile: str,
    i3_conf_path: Path | None,
    hub_filter: str | None,
) -> None:
    """Discover hubs, load config, and send one email per hub. Report success and failure."""
    rows = discover_backlog_hubs(bucket=bucket, aws_profile=aws_profile)
    if hub_filter:
        rows = [r for r in rows if r["hub_name"] == hub_filter]
    rows = load_config_and_attach_emails(i3_conf_path, rows)

    print("=" * 70)
    print("BACKLOG MAPPING EMAILS — SEND")
    print("Bucket:", bucket)
    print("Profile:", aws_profile)
    print("CC on every email:", ", ".join(CC_ALWAYS))
    print("No hub contact → To:", ", ".join(FALLBACK_TO))
    print("=" * 70)

    success: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for row in rows:
        hub_name = row["hub_name"]
        try:
            ok, detail = _build_and_send_email(row, bucket, aws_profile)
            if ok:
                success.append((hub_name, detail))
                print(f"  OK {hub_name}")
            else:
                failed.append((hub_name, detail))
                print(f"  FAIL {hub_name}: {detail}")
        except Exception as e:
            failed.append((hub_name, str(e)))
            print(f"  FAIL {hub_name}: {e}")

    print("\n" + "=" * 70)
    print("SUCCESSFUL NOTIFICATIONS")
    print("-" * 70)
    for hub, detail in success:
        print(f"  {hub}: {detail}")
    if not success:
        print("  (none)")
    print("\nFAILED NOTIFICATIONS")
    print("-" * 70)
    for hub, detail in failed:
        print(f"  {hub}: {detail}")
    if not failed:
        print("  (none)")
    print("\n" + "=" * 70)
    print(f"Total: {len(success)} succeeded, {len(failed)} failed")
    print("=" * 70)


def run_dry_run(
    bucket: str,
    aws_profile: str,
    i3_conf_path: Path | None,
    hub_filter: str | None,
) -> None:
    """Print hubs that would be contacted, their emails, and ingest dates (YYYY-MM)."""
    rows = discover_backlog_hubs(bucket=bucket, aws_profile=aws_profile)
    if hub_filter:
        rows = [r for r in rows if r["hub_name"] == hub_filter]
    rows = load_config_and_attach_emails(i3_conf_path, rows)

    # Exclude hubs with no email (still show them with a note)
    with_email = [r for r in rows if r["emails"]]
    without_email = [r for r in rows if not r["emails"]]

    print("=" * 70)
    print("BACKLOG MAPPING EMAILS — DRY RUN")
    print("Bucket:", bucket)
    print("Months: 2025-12, 2026-01, 2026-02")
    print("=" * 70)

    print("\n1. Hubs that WILL be contacted (have mapping output + email in i3.conf):")
    print("-" * 70)
    if not with_email:
        print("  (none)")
    else:
        for r in with_email:
            emails_str = ", ".join(r["emails"])
            dates_str = ", ".join(r["ingest_dates_yyyy_mm"])
            print(f"  Hub:     {r['hub_name']}")
            print(f"  Emails:  {emails_str}")
            print(f"  Ingest:  {dates_str}")
            print()

    print("\n2. Hubs with mapping output but NO email in i3.conf (will be skipped):")
    print("-" * 70)
    if not without_email:
        print("  (none)")
    else:
        for r in without_email:
            dates_str = ", ".join(r["ingest_dates_yyyy_mm"])
            print(f"  Hub: {r['hub_name']}  Ingest: {dates_str}")

    print("\n" + "=" * 70)
    print(f"Total with email: {len(with_email)}  |  Total without email: {len(without_email)}")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backlog mapping summary emails (dry-run: list hubs, emails, ingest dates)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list hubs, emails, and ingest dates (YYYY-MM); do not send or draft",
    )
    parser.add_argument(
        "--bucket",
        default="dpla-master-dataset",
        help="S3 bucket (default: dpla-master-dataset)",
    )
    parser.add_argument(
        "--aws-profile",
        default="dpla",
        help="AWS profile for S3 (default: dpla)",
    )
    parser.add_argument(
        "--i3-conf",
        type=Path,
        default=None,
        help="Path to i3.conf (default: from env I3_CONF or scheduler default)",
    )
    parser.add_argument(
        "--hub",
        metavar="NAME",
        help="Limit to a single hub (for testing)",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Send emails via SES (CC: scott@dp.la, tech@dp.la, dominic@dp.la; no hub contact → To those only)",
    )
    args = parser.parse_args()

    if args.send:
        run_send(
            bucket=args.bucket,
            aws_profile=args.aws_profile,
            i3_conf_path=args.i3_conf,
            hub_filter=args.hub,
        )
    else:
        run_dry_run(
            bucket=args.bucket,
            aws_profile=args.aws_profile,
            i3_conf_path=args.i3_conf,
            hub_filter=args.hub,
        )


if __name__ == "__main__":
    main()
