#!/usr/bin/env python3
"""
NARA delivery copy script.

Copies a NARA delta delivery from NARA's S3 bucket (ngc-storage01) to both
DPLA's S3 bucket (dpla-hub-nara) and the ingest EC2, ready for nara-ingest.sh.

Why the middleman? The NARA credentials (profile: nara) only have read access
to ngc-storage01. EC2's default IAM role has write access to dpla-hub-nara.
A direct cross-account sync isn't possible with a single credential set, so we:
  1. Download ZIPs from ngc-storage01 → /tmp/nara-<YYYYMM>/ on EC2 (using nara profile)
  2. Upload ZIPs from EC2 → s3://dpla-hub-nara/raw_ingest_files/<YYYYMM>/ (using EC2 role)
  3. Move ZIPs from /tmp/nara-<YYYYMM>/ → ~/dpla/data/nara/originalRecords/<YYYYMM>/
     (ready for nara-ingest.sh — no second download needed)

After this script completes, run:
    python3 nara/launch_nara.py --month <YYYYMM>

Usage:
    python3 copy_nara.py --month 202604

Prerequisites:
    - AWS CLI installed and authenticated locally
    - [nara] profile in ~/.aws/credentials on EC2 (NARA-issued credentials)
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation on the target instance
"""

import argparse
import base64
import json
import re
import subprocess
import sys
import time

# ---------- config ----------
INSTANCE_ID       = "i-0a0def8581efef783"
NARA_DST          = "s3://dpla-hub-nara/raw_ingest_files"
NARA_PROFILE      = "nara"
NARA_ORIGINALS_EC2 = "/home/ec2-user/dpla/data/nara/originalRecords"


# ---------- AWS / SSM helpers ----------
def aws(args):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    """Run a shell command on EC2 via SSM as ec2-user. Returns stdout."""
    encoded = base64.b64encode(shell_cmd.encode()).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})
    cmd_id  = aws([
        "ssm", "send-command",
        "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--timeout-seconds", str(timeout_seconds),
        "--parameters", params,
        "--query", "Command.CommandId",
        "--output", "text",
    ])
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ssm_bg(shell_cmd, log_path):
    """Fire a long-running command as a background nohup job. Returns PID."""
    launch = f"nohup bash -c {json.dumps(shell_cmd)} > {log_path} 2>&1 & echo $!"
    out = ssm_run(launch, timeout_seconds=60)
    return out.strip().split()[-1]


def wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=None):
    """Poll until the process exits, tailing the log each cycle."""
    start = time.time()
    while True:
        time.sleep(poll_seconds)
        alive = ssm_run(
            f"ps -p {pid} -o pid= 2>/dev/null || echo dead",
            timeout_seconds=30,
        ).strip()
        log_tail = ssm_run(
            f"[ -f {log_path} ] && tail -20 {log_path} || echo '(no log yet)'",
            timeout_seconds=30,
        ).rstrip()
        print("\n" + "─" * 60)
        print(log_tail)
        if alive in ("dead", ""):
            break
        if timeout_seconds and (time.time() - start) > timeout_seconds:
            raise RuntimeError(f"Process {pid} timed out after {timeout_seconds}s")


# ---------- UI helpers ----------
def step(n, title):
    print()
    print("=" * 70)
    print(f"  STEP {n}: {title}")
    print("=" * 70)


def confirm(msg, default_yes=True):
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        answer = input(f"\n  {msg}{suffix}").strip().lower()
    except EOFError:
        answer = ""
    ok = answer not in ("n", "no") if default_yes else answer in ("y", "yes")
    if not ok:
        sys.exit("Aborted.")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Copy NARA delivery from ngc-storage01 to dpla-hub-nara via EC2")
    args = parser.parse_args()

    # Prompt for the S3 link NARA provided
    try:
        src = input("\n  Paste the NARA S3 link for this delivery: ").strip().rstrip("/") + "/"
    except EOFError:
        sys.exit("No input provided.")

    if not src.startswith("s3://"):
        sys.exit(f"Expected an S3 URL (s3://...), got: {src!r}")

    # Derive month (YYYYMM) from the URL — expects a segment like 2026/M04 or /202604/
    month_match = re.search(r"(\d{4})/M(\d{1,2})", src)
    if month_match:
        month = f"{month_match.group(1)}{int(month_match.group(2)):02d}"
    else:
        month_match = re.search(r"(\d{6})", src)
        if month_match:
            month = month_match.group(1)
        else:
            try:
                month = input("  Could not auto-detect month from URL. Enter YYYYMM: ").strip()
            except EOFError:
                sys.exit("No input provided.")
            if not re.match(r"^\d{6}$", month):
                sys.exit(f"--month must be YYYYMM (6 digits), got: {month!r}")

    dst           = f"{NARA_DST}/{month}/"
    tmp_dir       = f"/tmp/nara-{month}"
    ec2_ingest_dir = f"{NARA_ORIGINALS_EC2}/{month}"
    log_path      = f"/tmp/nara-copy-{month}.log"

    print(f"\nNARA COPY — {month}")
    print(f"Instance:  {INSTANCE_ID}")
    print(f"Source:    {src}")
    print(f"S3 dest:   {dst}")
    print(f"EC2 dest:  {ec2_ingest_dir}")
    print(f"Temp dir:  {tmp_dir} (removed after move)")
    print()

    # Check if destination already has files
    step(1, f"Verify source files in {src}")
    try:
        existing = aws(["s3", "ls", dst])
        existing_count = len([l for l in existing.splitlines() if l.strip()])
    except RuntimeError:
        existing_count = 0

    if existing_count > 0:
        print(f"  WARNING: {dst} already has {existing_count} file(s).")
        confirm(f"Files already exist in {dst}. Overwrite/re-sync anyway?", default_yes=False)

    count = ssm_run(
        f"aws s3 ls {src} --profile {NARA_PROFILE} | wc -l",
        timeout_seconds=30,
    ).strip()
    print(f"  Found {count} file(s) in {src}")
    if count == "0":
        sys.exit(f"No files found at {src} — check the month and try again.")
    confirm(f"Copy {count} file(s) to {dst}?")

    # Run the two-step sync as a background job
    step(2, "Download from ngc-storage01 → EC2 → dpla-hub-nara")
    print(f"  Log: {log_path}")

    sync_cmd = (
        f"set -e && "
        f"echo 'Step 1/4: Creating temp dir...' && "
        f"mkdir -p {tmp_dir} && "
        f"echo 'Step 2/4: Downloading from ngc-storage01...' && "
        f"aws s3 sync {src} {tmp_dir}/ --profile {NARA_PROFILE} && "
        f"echo 'Step 3/4: Uploading to dpla-hub-nara...' && "
        f"aws s3 sync {tmp_dir}/ {dst} && "
        f"echo 'Step 4/4: Moving files to EC2 ingest dir...' && "
        f"mkdir -p {ec2_ingest_dir} && "
        f"mv {tmp_dir}/* {ec2_ingest_dir}/ && "
        f"rm -rf {tmp_dir} && "
        f"echo 'DONE'"
    )

    pid = ssm_bg(sync_cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=7200)

    # Verify destination
    step(3, "Verify")
    print(f"  Checking S3: {dst}")
    s3_count = ssm_run(
        f"aws s3 ls {dst} | wc -l",
        timeout_seconds=30,
    ).strip()
    print(f"  {s3_count} file(s) in {dst}")

    print(f"  Checking EC2: {ec2_ingest_dir}")
    ec2_count = ssm_run(
        f"ls {ec2_ingest_dir} 2>/dev/null | wc -l || echo 0",
        timeout_seconds=30,
    ).strip()
    print(f"  {ec2_count} file(s) in {ec2_ingest_dir}")

    tmp_gone = ssm_run(
        f"[ -d {tmp_dir} ] && echo EXISTS || echo GONE",
        timeout_seconds=30,
    ).strip()
    print(f"  Temp dir {tmp_dir}: {tmp_gone}")

    print()
    print("=" * 70)
    print(f"  NARA copy complete — {month}!")
    print(f"  S3:  {dst}")
    print(f"  EC2: {ec2_ingest_dir}")
    print(f"  Run: python3 launch_nara.py --month {month}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()