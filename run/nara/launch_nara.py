#!/usr/bin/env python3
"""
NARA delta ingest orchestrator.

NARA uses a delta format — each delivery is one or more ZIPs organized by
export group. ZIPs are stored in s3://dpla-hub-nara/raw_ingest_files/<YYYYMM>/
and synced there by the NARA team each delivery cycle.

This script:
  1. Auto-detects the latest YYYYMM subfolder in s3://dpla-hub-nara/raw_ingest_files/
     (or uses --month to target a specific delivery)
  2. Copies ZIP(s) from dpla-hub-nara to ~/dpla/data/nara/originalRecords/<YYYYMM>/ on EC2
  3. Runs nara-ingest.sh --month=<YYYYMM> as a background job and tails the log

nara-ingest.sh handles preprocessing, harvest, merge, and pipeline internally.
Never use ingest.sh for NARA.

Usage:
    python3 launch_nara.py                  # auto-detect latest delivery
    python3 launch_nara.py --month 202604   # target a specific delivery

Skip flags (for resuming a failed run):
    --skip-copy         ZIPs already on EC2 at ~/dpla/data/nara/originalRecords/<month>/
    --skip-to-pipeline  Skip straight to pipeline on existing merged harvest
    --harvest           EC2 path to merged harvest (use with --skip-to-pipeline)
    --skip-pipeline     Stop after merge step

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation on the target instance
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time

# ---------- config ----------
INSTANCE_ID    = "i-0a0def8581efef783"
REPO_PATH      = "/home/ec2-user/ingestion3"
NARA_SCRIPT    = f"{REPO_PATH}/scripts/nara-ingest.sh"
NARA_S3_BUCKET = "s3://dpla-hub-nara/raw_ingest_files"

# Path where nara-ingest.sh expects original ZIP files.
# Matches $DPLA_DATA/nara/originalRecords in common.sh — verify this matches
# the value of DPLA_DATA on the EC2 box if the script ever can't find its input.
NARA_ORIGINALS = "/home/ec2-user/dpla/data/nara/originalRecords"

INGEST_LOG     = "/tmp/nara-ingest.log"
INGEST_TIMEOUT = 43200   # 12 hours (NARA pipeline ~10 hours per the script)


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
            f"[ -f {log_path} ] && tail -30 {log_path} || echo '(no log yet)'",
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


# ---------- stages ----------

def detect_latest_month():
    """List subfolders in s3://dpla-hub-nara/raw_ingest_files/ and return the latest YYYYMM."""
    print(f"  Scanning {NARA_S3_BUCKET}/ for latest delivery...")
    output = aws(["s3", "ls", f"{NARA_S3_BUCKET}/"])
    folders = []
    for line in output.splitlines():
        parts = line.strip().split()
        if parts:
            name = parts[-1].rstrip("/")
            if re.match(r"^\d{6}$", name):
                folders.append(name)
    if not folders:
        sys.exit(f"No YYYYMM subfolders found in {NARA_S3_BUCKET}/")
    latest = sorted(folders)[-1]
    print(f"  Latest delivery: {latest}")
    return latest


def stage_copy_to_ec2(month):
    step(1, f"Copy ZIP(s) from s3://dpla-hub-nara/raw_ingest_files/{month}/ to EC2")
    s3_src   = f"{NARA_S3_BUCKET}/{month}/"
    dest_dir = f"{NARA_ORIGINALS}/{month}"
    print(f"  Source:      {s3_src}")
    print(f"  Destination: {dest_dir}")

    ssm_run(f"mkdir -p {dest_dir}", timeout_seconds=30)

    print(f"  Syncing ZIPs to EC2 (this may take a few minutes)...")
    ssm_run(
        f"aws s3 sync {s3_src} {dest_dir}/",
        timeout_seconds=1800,  # 30 min for large deliveries
    )

    listing = ssm_run(f"ls -lh {dest_dir}/ | head -20", timeout_seconds=30)
    count   = ssm_run(f"ls {dest_dir}/*.zip 2>/dev/null | wc -l", timeout_seconds=30).strip()
    print(f"\n{listing.rstrip()}")
    print(f"\n  {count} ZIP(s) on EC2.")
    confirm("Files look right on EC2? Continue to ingest?")


def stage_ingest(month, extra_flags=""):
    step(2, f"Run nara-ingest.sh --month={month}")
    cmd = f"bash {NARA_SCRIPT} --month={month} {extra_flags}".strip()
    print(f"  Command: {cmd}")
    print(f"  Log:     {INGEST_LOG}")

    # Clear stale log
    ssm_run(f"rm -f {INGEST_LOG}", timeout_seconds=30)

    pid = ssm_bg(cmd, INGEST_LOG)
    print(f"  PID: {pid} — tailing every 30s (pipeline takes ~10 hours)...")
    wait_for_pid(pid, INGEST_LOG, poll_seconds=30, timeout_seconds=INGEST_TIMEOUT)

    # Show tail of log for final status
    final = ssm_run(f"tail -40 {INGEST_LOG} 2>/dev/null || echo '(log not found)'",
                    timeout_seconds=30).rstrip()
    print(f"\n{final}")
    print("\n  nara-ingest.sh has exited.")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="NARA delta ingest orchestrator")
    parser.add_argument("--month",
                        help="Delivery month YYYYMM (default: auto-detect latest in s3://dpla-hub-nara/raw_ingest_files/)")
    parser.add_argument("--skip-copy", action="store_true",
                        help="Skip S3→EC2 copy (ZIPs already in originalRecords/<month>/ on EC2)")
    parser.add_argument("--skip-to-pipeline", action="store_true",
                        help="Pass --skip-to-pipeline to nara-ingest.sh (uses existing merged harvest)")
    parser.add_argument("--harvest",
                        help="EC2 path to merged harvest avro (use with --skip-to-pipeline)")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Pass --skip-pipeline to nara-ingest.sh (stop after merge)")
    args = parser.parse_args()

    if args.month and not re.match(r"^\d{6}$", args.month):
        sys.exit(f"--month must be YYYYMM (6 digits), got: {args.month!r}")

    # Auto-detect month if not provided
    if args.month:
        month = args.month
    elif args.skip_to_pipeline or args.skip_copy:
        sys.exit("--month is required when using --skip-copy or --skip-to-pipeline")
    else:
        month = detect_latest_month()

    # Build extra flags to pass through to nara-ingest.sh
    extra_flags = []
    if args.skip_to_pipeline:
        extra_flags.append("--skip-to-pipeline")
    if args.harvest:
        extra_flags.append(f"--harvest={args.harvest}")
    if args.skip_pipeline:
        extra_flags.append("--skip-pipeline")

    print(f"\nNARA INGEST — {month}")
    print(f"Instance:  {INSTANCE_ID}")
    print(f"Script:    {NARA_SCRIPT}")
    print(f"Source:    {NARA_S3_BUCKET}/{month}/")
    print()

    if args.skip_to_pipeline:
        stage_ingest(month, " ".join(extra_flags))
    else:
        if not args.skip_copy:
            stage_copy_to_ec2(month)
        else:
            print("Step 1: S3→EC2 copy — skipped.")

        stage_ingest(month, " ".join(extra_flags))

    print()
    print("=" * 70)
    print(f"  NARA ingest complete — {month}!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()