#!/usr/bin/env python3
"""
DPLA monthly ingest status report.

Cross-references the schedule in i3.conf against S3 to show which scheduled
hubs have been ingested this month and which are still outstanding.

A hub is "DONE" if s3://dpla-master-dataset/<hub>/jsonl/ contains a folder
whose name starts with the target year+month (e.g. 20260428... for April 2026).
That's the canonical "ready for the next index rebuild" signal — same source
of truth the index rebuild reads from.

Usage:
    python3 postchecks.py                # report for current month
    python3 postchecks.py --month 4      # April
    python3 postchecks.py --month 5      # May (look ahead)

Requirements:
    - aws CLI installed and authenticated (aws sts get-caller-identity should work)
    - AWS_PROFILE=dpla, or pass --profile in your shell env
    - Read access to s3://dpla-master-dataset/
"""

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime

# ---------- config ----------
CONF_PATH = os.environ.get("I3_CONF") or os.path.expanduser(
    "~/Documents/Repos/ingestion3-conf/i3.conf"
)
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")
S3_BUCKET = "dpla-master-dataset"

# Some hubs use a different name in S3 than in i3.conf. Add overrides here.
# Most hubs (bpl, p2p, etc.) use the same name in both places, so the dict
# stays small.
S3_HUB_OVERRIDE = {
    "si": "smithsonian",
}

SPECIAL_CASE_NOTES = {
    "nara":           "delta merge required (use NaraMergeUtil, not ingest.sh)",
    "smithsonian":    "preprocessing required (download from S3 + fix-si.sh)",
    "si":             "preprocessing required (download from S3 + fix-si.sh)",
    "community-webs": "preprocessing required (SQLite -> JSONL/ZIP)",
    "maryland":       "EC2 IP blocked, run locally",
    "getty":          "EC2 IP blocked, run locally",
    "hathitrust":     "EC2 IP blocked, run locally",
    "illinois":       "VPN required, run locally",
    "indiana":        "VPN required, run locally",
    "mwdl":           "VPN required, run locally",
    "ct":             "file-export, check S3 for fresh export first",
    "fl":             "file-export, check S3 for fresh export first",
    "ohio":           "file-export, check S3 for fresh export first",
    "nypl":           "file-export, check S3 for fresh export first",
    "vt":             "file-export, check S3 for fresh export first",
    "txdl":           "file-export, check S3 for fresh export first",
    "txd":            "file-export, check S3 for fresh export first",
    "virginias":      "multi-repo assembly (clone dplava org repos first)",
}

MONTH_NAMES = ("", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December")


# ---------- conf parsing (mirrors prechecks.py logic) ----------
def list_scheduled_hubs(month, conf_path=CONF_PATH):
    """Parse i3.conf and return hubs scheduled for the given month.
    Returns a list of dicts with keys: hub, months, status, harvest_type."""
    if not os.path.exists(conf_path):
        return None
    with open(conf_path, "r", encoding="utf-8") as f:
        text = f.read()
    text = re.sub(r"(?m)^\s*(#|//).*$", "", text)

    hubs = {}
    months_re = re.compile(
        r"^\s*([a-z0-9_-]+)\.schedule\.months\s*[=:]\s*\[([0-9,\s]+)\]",
        re.MULTILINE | re.IGNORECASE,
    )
    status_re = re.compile(
        r"""^\s*([a-z0-9_-]+)\.schedule\.status\s*[=:]\s*["']([^"']+)["']""",
        re.MULTILINE | re.IGNORECASE,
    )
    type_re = re.compile(
        r"""^\s*([a-z0-9_-]+)\.harvest\.type\s*[=:]\s*["']?([A-Za-z._]+)["']?""",
        re.MULTILINE | re.IGNORECASE,
    )

    for m in months_re.finditer(text):
        name = m.group(1).lower()
        months = [int(x.strip()) for x in m.group(2).split(",") if x.strip().isdigit()]
        hubs.setdefault(name, {})["months"] = months
    for m in status_re.finditer(text):
        hubs.setdefault(m.group(1).lower(), {})["status"] = m.group(2)
    for m in type_re.finditer(text):
        hubs.setdefault(m.group(1).lower(), {})["harvest_type"] = m.group(2).lower()

    matching = []
    for name, data in sorted(hubs.items()):
        if "months" not in data or month not in data["months"]:
            continue
        matching.append({
            "hub": name,
            "months": data["months"],
            "status": data.get("status", "active"),
            "harvest_type": data.get("harvest_type", "?"),
        })
    return matching


# ---------- S3 scanning ----------
S3_FOLDER_DATE_RE = re.compile(r"PRE\s+(\d{8})[-_]")
S3_PREFIX_RE = re.compile(r"^\s*PRE\s+([^\s/]+)/?\s*$")


def aws_s3_ls(s3_path):
    """Run aws s3 ls and return stdout, or empty string on failure."""
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", s3_path, "--profile", AWS_PROFILE],
            capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout


def list_all_hubs_in_s3():
    """List the top-level hub directories in s3://dpla-master-dataset/.
    Each top-level prefix is a hub name (e.g. bpl/, smithsonian/, p2p/).
    Returns a sorted list of names."""
    out = aws_s3_ls(f"s3://{S3_BUCKET}/")
    hubs = []
    for line in out.splitlines():
        m = S3_PREFIX_RE.match(line)
        if m:
            hubs.append(m.group(1))
    return sorted(hubs)


def s3_jsonl_folders(s3_hub_name):
    """Return list of (yyyymmdd_str, full_folder_name) for JSONL folders
    under s3://dpla-master-dataset/<s3_hub_name>/jsonl/.
    Note: takes the S3-side hub name (e.g. 'smithsonian'), not the conf-side
    hub name (e.g. 'si')."""
    out = aws_s3_ls(f"s3://{S3_BUCKET}/{s3_hub_name}/jsonl/")
    folders = []
    for line in out.splitlines():
        m = S3_FOLDER_DATE_RE.search(line)
        if m:
            full = line.split()[-1].rstrip("/")
            folders.append((m.group(1), full))
    return folders


def scan_s3_for_month(year, month):
    """Walk S3 and return {s3_hub_name: (yyyymmdd, folder_name)} for every hub
    that has a JSONL folder dated within the given year+month. The yyyymmdd
    is the most recent matching folder for that hub."""
    target_prefix = f"{year:04d}{month:02d}"
    done = {}
    for hub_name in list_all_hubs_in_s3():
        folders = s3_jsonl_folders(hub_name)
        matches = [f for f in folders if f[0].startswith(target_prefix)]
        if matches:
            matches.sort(key=lambda x: x[0], reverse=True)
            done[hub_name] = matches[0]
    return done


def conf_name_for_s3_name(s3_name):
    """Reverse-lookup: given an S3 path name, return the conf-side hub name.
    Most hubs use the same name in both; only S3_HUB_OVERRIDE entries differ."""
    for conf_name, mapped in S3_HUB_OVERRIDE.items():
        if mapped == s3_name:
            return conf_name
    return s3_name


# ---------- report ----------
def print_status_report(year, month):
    print(f"\nMonthly Ingest Status: {MONTH_NAMES[month]} {year} (month {month})")
    print(f"Conf: {CONF_PATH}")
    print(f"S3:   s3://{S3_BUCKET}/<hub>/jsonl/")
    print("Scanning S3 for JSONL folders dated this month...\n")

    # Scan S3 first — this is the source of truth for "what's done."
    s3_done = scan_s3_for_month(year, month)

    # Cross-reference with the schedule.
    scheduled = list_scheduled_hubs(month) or []
    if scheduled is None:
        print(f"  (warning: could not read {CONF_PATH} — schedule cross-reference skipped)\n")
        scheduled = []

    active = [h for h in scheduled if (h["status"] or "active").lower() == "active"]
    on_hold = [h for h in scheduled if h not in active]
    scheduled_active_names = {h["hub"] for h in active}

    # Map each S3-done hub back to its conf-side name so we can match it
    # against the schedule (e.g. S3 'smithsonian' ↔ conf 'si').
    s3_done_by_conf_name = {}
    for s3_name, (date_str, folder) in s3_done.items():
        conf_name = conf_name_for_s3_name(s3_name)
        s3_done_by_conf_name[conf_name] = (date_str, folder, s3_name)

    # 3 buckets:
    #   on-schedule + done       → "DONE"
    #   on-schedule + not done   → "PENDING"
    #   off-schedule + done      → "OFF-SCHEDULE" (extra ingests this month
    #                              that weren't on the official April list)
    on_schedule_done = []
    on_schedule_pending = []
    for h in active:
        if h["hub"] in s3_done_by_conf_name:
            d, folder, s3_name = s3_done_by_conf_name[h["hub"]]
            h["s3_date"] = d
            h["s3_folder"] = folder
            h["s3_name"] = s3_name
            on_schedule_done.append(h)
        else:
            on_schedule_pending.append(h)

    off_schedule_done = []
    for conf_name, (d, folder, s3_name) in s3_done_by_conf_name.items():
        if conf_name not in scheduled_active_names:
            off_schedule_done.append({
                "hub": conf_name,
                "s3_name": s3_name,
                "s3_date": d,
                "s3_folder": folder,
            })

    # ---- print ----
    print(f"  DONE ({len(on_schedule_done)} of {len(active)} scheduled)")
    if not on_schedule_done:
        print("    (none yet)")
    else:
        for h in sorted(on_schedule_done, key=lambda x: x["s3_date"], reverse=True):
            d = h["s3_date"]
            iso = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
            print(f"    {h['hub']:<20} {h['harvest_type']:<10} ingested {iso}")
    print()

    print(f"  PENDING ({len(on_schedule_pending)} of {len(active)} scheduled)")
    if not on_schedule_pending:
        print("    (none — all scheduled hubs done!)")
    else:
        for h in on_schedule_pending:
            note = SPECIAL_CASE_NOTES.get(h["hub"], "")
            note_str = f"   ⚠ {note}" if note else ""
            print(f"    {h['hub']:<20} {h['harvest_type']:<10}{note_str}")
    print()

    if off_schedule_done:
        print(f"  OFF-SCHEDULE — done in S3 this month but NOT on the April schedule ({len(off_schedule_done)})")
        for h in sorted(off_schedule_done, key=lambda x: x["s3_date"], reverse=True):
            d = h["s3_date"]
            iso = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
            label = h["hub"]
            if h["s3_name"] != h["hub"]:
                label = f"{h['hub']} (S3 name: {h['s3_name']})"
            print(f"    {label:<28} ingested {iso}")
        print()

    if on_hold:
        print(f"  ON-HOLD ({len(on_hold)}, skipped from totals)")
        for h in on_hold:
            print(f"    {h['hub']:<20} {h['harvest_type']:<10}   {h['status']}")
        print()


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="DPLA monthly ingest status report")
    parser.add_argument(
        "--month", type=int, default=None, metavar="MONTH",
        help="Month (1-12). Default: current month.",
    )
    parser.add_argument(
        "--year", type=int, default=None, metavar="YEAR",
        help="Year (e.g. 2026). Default: current year.",
    )
    args = parser.parse_args()

    now = datetime.now()
    target_month = args.month if args.month else now.month
    target_year = args.year if args.year else now.year
    if not (1 <= target_month <= 12):
        sys.exit(f"Invalid month: {target_month}. Use 1-12.")

    print_status_report(target_year, target_month)


if __name__ == "__main__":
    main()