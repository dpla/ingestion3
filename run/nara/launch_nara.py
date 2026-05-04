#!/usr/bin/env python3
"""
NARA delta ingest orchestrator.

NARA uses a delta format: each delivery has new/updated records + a deletes list.
This script:
  1. Uploads your local ZIP to s3://dpla-hub-nara/
  2. Downloads & preprocesses on EC2 (separates deletes, renames, recompresses)
  3. Runs the delta harvest via ingest.sh
  4. Merges delta into the prior full harvest via NaraMergeUtil
  5. Runs mapping → enrichment → JSONL → S3 sync

Usage:
    python3 run_nara.py --date 20260501 --zip ~/Downloads/nara-delta.zip

Skip flags (for resuming a failed run mid-way):
    --skip-upload       ZIP already in s3://dpla-hub-nara/
    --skip-preprocess   EC2 directory already set up
    --skip-harvest      Delta avro already exists (requires --delta-avro)
    --skip-merge        Merged avro already exists (requires --merged-avro)

Prerequisites:
    - AWS CLI installed and authenticated locally
    - IAM: ec2:DescribeInstances, ssm:SendCommand, ssm:GetCommandInvocation,
           s3:PutObject on dpla-hub-nara, s3:GetObject on dpla-hub-nara
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
INSTANCE_ID        = "i-0a0def8581efef783"
REPO_PATH          = "/home/ec2-user/ingestion3"
NARA_PATH          = "/home/ec2-user/nara"
S3_STAGING         = "s3://dpla-hub-nara"
HUB                = "nara"
INGEST_SCRIPT      = f"{REPO_PATH}/scripts/ingest.sh"

HARVEST_TIMEOUT_S  = 7200    # 2 hours
MERGE_TIMEOUT_S    = 10800   # 3 hours
PIPELINE_TIMEOUT_S = 10800   # 3 hours


# ---------- AWS / SSM helpers ----------
def aws(args):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=120, poll_seconds=5):
    """Run a shell command on EC2 via SSM and return stdout. Raises on failure."""
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
            raise RuntimeError(f"SSM command timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def ssm_bg(shell_cmd, log_path):
    """Fire a command as a background nohup job on EC2. Returns the PID string."""
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
    """Prompt the user. Returns True for yes, exits on no."""
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        answer = input(f"\n  {msg}{suffix}").strip().lower()
    except EOFError:
        answer = ""
    if default_yes:
        ok = answer not in ("n", "no")
    else:
        ok = answer in ("y", "yes")
    if not ok:
        sys.exit("Aborted.")
    return True


# ---------- stages ----------

def stage_upload(date_str, zip_path):
    step(1, "Upload ZIP to S3")
    s3_dest   = f"{S3_STAGING}/{date_str}-nara-delta.zip"
    size_mb   = os.path.getsize(zip_path) / 1024 / 1024
    print(f"  Local:  {zip_path}  ({size_mb:.1f} MB)")
    print(f"  S3:     {s3_dest}")
    confirm(f"Upload {size_mb:.1f} MB to S3?")
    result = subprocess.run(["aws", "s3", "cp", zip_path, s3_dest, "--no-progress"], text=True)
    if result.returncode != 0:
        raise RuntimeError(f"S3 upload failed: {result.stderr.strip()}")
    print(f"  ✓ Uploaded → {s3_dest}")
    return s3_dest


def stage_preprocess(date_str, s3_zip):
    step(2, "Download & preprocess on EC2")
    delta_dir   = f"{NARA_PATH}/delta/{date_str}"
    deletes_dir = f"{delta_dir}/deletes"
    harvest_dir = f"{NARA_PATH}/harvest"

    print(f"  Creating directory structure on EC2...")
    ssm_run(f"mkdir -p {deletes_dir} {harvest_dir}", timeout_seconds=30)

    print(f"  Downloading ZIP from S3...")
    ssm_run(f"aws s3 cp {s3_zip} {delta_dir}/nara-delta.zip", timeout_seconds=600)

    print(f"  Unzipping...")
    ssm_run(f"cd {delta_dir} && unzip -o nara-delta.zip", timeout_seconds=300)

    print(f"  Moving and renaming deletes_*.xml files...")
    ssm_run(
        f"cd {delta_dir} && "
        f"for f in deletes_*.xml; do "
        f"  [ -f \"$f\" ] && mv \"$f\" deletes/{date_str}_$f && echo \"  moved: $f\"; "
        f"done",
        timeout_seconds=30,
    )

    print(f"  Recompressing update files (excluding deletes/ and .zip)...")
    ssm_run(
        f"cd {delta_dir} && "
        f"tar czf {date_str}-nara-delta.tar.gz "
        f"--exclude='./deletes' --exclude='*.zip' --exclude='*.tar.gz' .",
        timeout_seconds=600,
    )

    # Show structure for human sign-off
    struct = ssm_run(
        f"echo '--- {delta_dir} ---' && ls -lh {delta_dir}/ && "
        f"echo '--- {deletes_dir} ---' && ls {deletes_dir}/ | head -10",
        timeout_seconds=30,
    )
    print(f"\n{struct.rstrip()}")
    confirm("Directory structure look right? Continue to harvest?")


def stage_harvest(date_str):
    step(3, "Delta harvest (nara.file.delta)")
    log_path = f"{NARA_PATH}/delta/{date_str}/harvest.log"
    cmd = f"bash {INGEST_SCRIPT} harvest {HUB}"

    print(f"  Log: {log_path}")
    pid = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=HARVEST_TIMEOUT_S)

    # Find the fresh delta avro — newest avro in the standard nara data dir
    avro_path = ssm_run(
        f"find /home/ec2-user/data/nara/originalRecords -name '*.avro' "
        f"-newer {NARA_PATH}/delta/{date_str}/nara-delta.zip 2>/dev/null | sort | tail -1",
        timeout_seconds=30,
    ).strip()
    if not avro_path:
        raise RuntimeError(
            "Harvest finished but no new delta avro found under "
            "/home/ec2-user/data/nara/originalRecords/. Check the harvest log above."
        )
    print(f"\n  Delta avro: {avro_path}")
    confirm(f"Harvest complete. Delta avro at {avro_path} — continue to merge?")
    return avro_path


def stage_merge(date_str, delta_avro):
    step(4, "NaraMergeUtil — merge delta into prior full harvest")
    harvest_dir = f"{NARA_PATH}/harvest"
    deletes_dir = f"{NARA_PATH}/delta/{date_str}/deletes"
    output_avro = f"{harvest_dir}/{date_str}-nara-OriginalRecord.avro"
    log_path    = f"{NARA_PATH}/delta/{date_str}/merge.log"

    # Find the most recent prior full harvest (exclude the output we're about to write)
    prior_avro = ssm_run(
        f"ls -1t {harvest_dir}/*.avro 2>/dev/null | grep -v {date_str} | head -1",
        timeout_seconds=30,
    ).strip()

    print(f"  Prior full harvest: {prior_avro or '(none — treating delta as base)'}")
    print(f"  Delta avro:         {delta_avro}")
    print(f"  Deletes dir:        {deletes_dir}")
    print(f"  Output avro:        {output_avro}")
    print(f"  Log:                {log_path}")

    if not prior_avro:
        print(
            "\n  [CAUTION] No prior full harvest found in ~/nara/harvest/. "
            "If this is the first NARA run, that's expected — "
            "NaraMergeUtil will use the delta as the full dataset."
        )
        prior_avro = delta_avro  # first run: delta IS the full dataset

    confirm("Paths look right? Run NaraMergeUtil (this takes a while)?")

    merge_cmd = (
        f"cd {REPO_PATH} && "
        f"sbt \"runMain dpla.ingestion3.utils.NaraMergeUtil "
        f"  {prior_avro} "
        f"  {delta_avro} "
        f"  {deletes_dir} "
        f"  {output_avro} "
        f"  local[*]\""
    )
    pid = ssm_bg(merge_cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=MERGE_TIMEOUT_S)

    # Pull the merge summary lines from the log
    summary = ssm_run(
        f"grep -iE 'insert|update|delet|total|record|summary|error|warn' {log_path} | tail -20",
        timeout_seconds=30,
    ).rstrip()
    print(f"\n  Merge summary:\n{summary}")

    confirm("Merge complete. Continue to mapping → enrichment → JSONL → S3?")
    return output_avro


def stage_pipeline(date_str):
    step(5, "Mapping → enrichment → JSONL → S3 sync")
    log_path = f"{NARA_PATH}/delta/{date_str}/pipeline.log"
    cmd = f"bash {INGEST_SCRIPT} --mapping-only {HUB}"

    print(f"  Log: {log_path}")
    pid = ssm_bg(cmd, log_path)
    print(f"  PID: {pid} — tailing every 30s...")
    wait_for_pid(pid, log_path, poll_seconds=30, timeout_seconds=PIPELINE_TIMEOUT_S)
    print("\n  Pipeline complete.")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="NARA delta ingest orchestrator")
    parser.add_argument("--date", required=True,
                        help="Delta date YYYYMMDD, e.g. 20260501")
    parser.add_argument("--zip", required=True,
                        help="Path to the local NARA delta ZIP file")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip S3 upload (ZIP already at s3://dpla-hub-nara/<date>-nara-delta.zip)")
    parser.add_argument("--skip-preprocess", action="store_true",
                        help="Skip download/unzip/recompress (EC2 dirs already set up)")
    parser.add_argument("--skip-harvest", action="store_true",
                        help="Skip harvest step (requires --delta-avro)")
    parser.add_argument("--delta-avro",
                        help="EC2 path to existing delta avro (use with --skip-harvest)")
    parser.add_argument("--skip-merge", action="store_true",
                        help="Skip merge step (requires --merged-avro)")
    parser.add_argument("--merged-avro",
                        help="EC2 path to existing merged avro (use with --skip-merge)")
    args = parser.parse_args()

    if not re.match(r"^\d{8}$", args.date):
        sys.exit(f"--date must be YYYYMMDD, got: {args.date!r}")
    if not args.skip_upload and not os.path.isfile(args.zip):
        sys.exit(f"ZIP file not found: {args.zip}")
    if args.skip_harvest and not args.delta_avro:
        sys.exit("--skip-harvest requires --delta-avro")
    if args.skip_merge and not args.merged_avro:
        sys.exit("--skip-merge requires --merged-avro")

    date_str = args.date
    s3_zip   = f"{S3_STAGING}/{date_str}-nara-delta.zip"

    print(f"\nNARA DELTA INGEST — {date_str}")
    print(f"Instance:  {INSTANCE_ID}")
    print(f"Local ZIP: {args.zip}")
    print(f"S3 ZIP:    {s3_zip}\n")

    if not args.skip_upload:
        stage_upload(date_str, args.zip)
    else:
        print("Step 1: Upload — skipped.")

    if not args.skip_preprocess:
        stage_preprocess(date_str, s3_zip)
    else:
        print("Step 2: Preprocess — skipped.")

    if not args.skip_harvest:
        delta_avro = stage_harvest(date_str)
    else:
        delta_avro = args.delta_avro
        print(f"Step 3: Harvest — skipped. Using: {delta_avro}")

    if not args.skip_merge:
        stage_merge(date_str, delta_avro)
    else:
        print(f"Step 4: Merge — skipped. Using: {args.merged_avro}")

    stage_pipeline(date_str)

    print()
    print("=" * 70)
    print(f"  NARA delta ingest complete for {date_str}!")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()