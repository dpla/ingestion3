#!/usr/bin/env python3
"""
NARA delta ingest orchestrator.

NARA uses a delta format — each delivery is one or more ZIPs organized by
export group. This script:
  1. Uploads local ZIP(s) to s3://dpla-scratch/nara/<YYYYMM>/ via presigned URL
     (EC2's ingestion3-spark role generates the URL; your Mac curls it up)
  2. Downloads ZIP(s) from S3 to ~/dpla/data/nara/originalRecords/<YYYYMM>/ on EC2
  3. Runs nara-ingest.sh --month=<YYYYMM> as a background job and tails the log

nara-ingest.sh handles preprocessing, harvest, merge, and pipeline internally.
Never use ingest.sh for NARA.

Usage:
    python3 launch_nara.py --month 202604 --zips ~/Downloads/nara-202604-*.zip

Skip flags (for resuming a failed run):
    --skip-upload       ZIPs already in s3://dpla-scratch/nara/<month>/
    --skip-copy         ZIPs already on EC2 at ~/dpla/data/nara/originalRecords/<month>/
    --skip-to-pipeline  Skip straight to pipeline on existing merged harvest
    --harvest           EC2 path to merged harvest (use with --skip-to-pipeline)

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ssm:SendCommand, ssm:GetCommandInvocation on the target instance
"""

import argparse
import base64
import glob
import json
import os
import subprocess
import sys
import time

# ---------- config ----------
INSTANCE_ID    = "i-0a0def8581efef783"
REPO_PATH      = "/home/ec2-user/ingestion3"
NARA_SCRIPT    = f"{REPO_PATH}/scripts/nara-ingest.sh"
S3_STAGING     = "s3://dpla-scratch/nara"

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

def get_presigned_put_url(s3_key):
    """Have EC2 generate a presigned PUT URL for dpla-scratch using its IAM role."""
    script = (
        f"import boto3; "
        f"s3=boto3.client('s3', region_name='us-east-1'); "
        f"print(s3.generate_presigned_url('put_object', "
        f"Params={{'Bucket':'dpla-scratch','Key':'{s3_key}'}}, ExpiresIn=3600))"
    )
    # Write to temp file to avoid quoting hell
    ssm_run(
        f"echo {base64.b64encode(script.encode()).decode()} | base64 -d > /tmp/_presign.py",
        timeout_seconds=30,
    )
    out = ssm_run("python3 /tmp/_presign.py", timeout_seconds=30)
    url = next((ln.strip() for ln in out.splitlines() if ln.strip().startswith("https://")), None)
    if not url:
        raise RuntimeError(f"Could not extract presigned URL from output:\n{out}")
    return url


def stage_upload(month, zip_paths):
    step(1, "Upload ZIP(s) to S3 via EC2 presigned URL")
    s3_keys = []
    for zip_path in zip_paths:
        fname   = os.path.basename(zip_path)
        s3_key  = f"nara/{month}/{fname}"
        s3_dest = f"s3://dpla-scratch/{s3_key}"
        size_mb = os.path.getsize(zip_path) / 1024 / 1024
        print(f"\n  {fname}  ({size_mb:.1f} MB)  →  {s3_dest}")

    confirm(f"Upload {len(zip_paths)} ZIP(s) to dpla-scratch/nara/{month}/?")

    for zip_path in zip_paths:
        fname  = os.path.basename(zip_path)
        s3_key = f"nara/{month}/{fname}"
        print(f"\n  Generating presigned URL for {fname}...")
        url = get_presigned_put_url(s3_key)
        print(f"  Uploading...")
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-X", "PUT", "--upload-file", zip_path, "--header", "Expect:", url],
            capture_output=False, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"curl upload failed for {fname}")
        print(f"  ✓ {fname} uploaded")

    s3_keys = [f"nara/{month}/{os.path.basename(z)}" for z in zip_paths]
    return s3_keys


def stage_copy_to_ec2(month, s3_keys):
    step(2, f"Copy ZIP(s) from S3 to EC2 originalRecords/{month}/")
    dest_dir = f"{NARA_ORIGINALS}/{month}"
    print(f"  Destination: {dest_dir}")
    ssm_run(f"mkdir -p {dest_dir}", timeout_seconds=30)

    for key in s3_keys:
        fname = os.path.basename(key)
        print(f"  Downloading {fname}...")
        ssm_run(
            f"aws s3 cp s3://dpla-scratch/{key} {dest_dir}/{fname}",
            timeout_seconds=600,
        )

    listing = ssm_run(f"ls -lh {dest_dir}/", timeout_seconds=30)
    print(f"\n{listing.rstrip()}")
    confirm("Files look right on EC2? Continue to ingest?")


def stage_ingest(month, extra_flags=""):
    step(3, f"Run nara-ingest.sh --month={month}")
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
    parser.add_argument("--month", required=True,
                        help="Delivery month YYYYMM, e.g. 202604")
    parser.add_argument("--zips", nargs="+", metavar="ZIP",
                        help="Path(s) to local NARA delta ZIP file(s)")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip upload (ZIPs already in s3://dpla-scratch/nara/<month>/)")
    parser.add_argument("--skip-copy", action="store_true",
                        help="Skip S3→EC2 copy (ZIPs already in originalRecords/<month>/ on EC2)")
    parser.add_argument("--skip-to-pipeline", action="store_true",
                        help="Pass --skip-to-pipeline to nara-ingest.sh (uses existing merged harvest)")
    parser.add_argument("--harvest",
                        help="EC2 path to merged harvest avro (use with --skip-to-pipeline)")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Pass --skip-pipeline to nara-ingest.sh (stop after merge)")
    args = parser.parse_args()

    import re
    if not re.match(r"^\d{6}$", args.month):
        sys.exit(f"--month must be YYYYMM (6 digits), got: {args.month!r}")

    # Expand globs in zip paths
    zip_paths = []
    if args.zips:
        for pattern in args.zips:
            expanded = glob.glob(os.path.expanduser(pattern))
            if not expanded:
                sys.exit(f"No files matched: {pattern}")
            zip_paths.extend(sorted(expanded))

    if not args.skip_upload and not args.skip_copy and not args.skip_to_pipeline and not zip_paths:
        sys.exit("Provide --zips, or use --skip-upload/--skip-copy/--skip-to-pipeline to resume.")

    month = args.month

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
    if zip_paths:
        print(f"ZIPs:      {', '.join(os.path.basename(z) for z in zip_paths)}")
    print()

    # Skip-to-pipeline goes straight to ingest with no upload/copy
    if args.skip_to_pipeline:
        stage_ingest(month, " ".join(extra_flags))
    else:
        s3_keys = [f"nara/{month}/{os.path.basename(z)}" for z in zip_paths]

        if not args.skip_upload:
            stage_upload(month, zip_paths)
        else:
            print("Step 1: Upload — skipped.")

        if not args.skip_copy:
            stage_copy_to_ec2(month, s3_keys)
        else:
            print("Step 2: S3→EC2 copy — skipped.")

        stage_ingest(month, " ".join(extra_flags))

    print()
    print("=" * 70)
    print(f"  NARA ingest complete — {month}!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
