#!/usr/bin/env python3
"""
Smithsonian ingest orchestrator.

Runs the full Smithsonian ingest pipeline end-to-end with human review
checkpoints after preprocessing and harvest.

Stages:
    1  download    aws s3 sync from dpla-hub-si
    2  preprocess  fix-si.sh                             → HUMAN CHECKPOINT
    3  harvest     harvest.sh smithsonian                → HUMAN CHECKPOINT
    4  pipeline    ingest.sh smithsonian --skip-harvest  → HUMAN CHECKPOINT

Stages already complete are skipped automatically. Use --start-at to resume
from a specific stage after a failure.

Usage:
    python3 run_smithsonian.py
    python3 run_smithsonian.py --date 2026-04-03
    python3 run_smithsonian.py --start-at harvest
    python3 run_smithsonian.py --start-at pipeline
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time

# ── config ───────────────────────────────────────────────────────────────────
INSTANCE_ID = "i-0a0def8581efef783"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")

S3_DELIVERY_BUCKET = "dpla-hub-si"
DATA_ROOT          = "/home/ec2-user/data/smithsonian"
INGEST_DIR         = "/home/ec2-user/ingestion3"
HARVEST_LOG        = "/home/ec2-user/data/si-harvest.log"
PIPELINE_LOG       = "/home/ec2-user/data/smithsonian-ingest.log"

DOWNLOAD_TIMEOUT_S  = 1800   # 30 min — 5 GB at reasonable speed
PREPROCESS_TIMEOUT_S = 1200  # 20 min — NMNHBOTANY ~6 min, total ~10–15 min
POLL_INTERVAL_S     = 30     # seconds between status polls for harvest/pipeline
LOG_TAIL_LINES      = 20

STAGE_ORDER = ["download", "preprocess", "harvest", "pipeline"]

S3_DATE_RE = re.compile(r"PRE\s+(\d{4}-\d{2}-\d{2})/")


# ── AWS / SSM helpers ─────────────────────────────────────────────────────────
def _aws(args):
    r = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args)} failed:\n{r.stderr.strip()}")
    return r.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=30, poll_seconds=4):
    """Run a shell command on the EC2 box via SSM; block until it completes."""
    encoded = base64.b64encode(shell_cmd.encode("utf-8")).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params  = json.dumps({"commands": [wrapped]})
    cmd_id  = _aws([
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
        status = _aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--query", "Status",
            "--output", "text",
        ])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    output = _aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])
    if status != "Success":
        raise RuntimeError(f"SSM ended with status {status}.\nOutput:\n{output}")
    return output


def ssm_bg(shell_cmd):
    """
    Launch a long-running command on the EC2 box in the background via nohup.
    Returns immediately — the process outlives the SSM session.

    Base64-encodes the inner command to avoid quoting issues (same approach as
    ssm_run). The outer nohup wrapper decodes and execs it in a login shell.
    """
    inner_b64 = base64.b64encode(shell_cmd.encode("utf-8")).decode("ascii")
    bg_wrapper = (
        f"nohup bash -lc 'echo {inner_b64} | base64 -d | bash -l' "
        f"</dev/null >/dev/null 2>&1 & disown; echo launched"
    )
    result = ssm_run(bg_wrapper, timeout_seconds=20)
    if "launched" not in result:
        raise RuntimeError(f"Background launch may have failed. SSM output: {result!r}")


def aws_s3_ls(s3_path):
    try:
        r = subprocess.run(
            ["aws", "s3", "ls", s3_path, "--profile", AWS_PROFILE],
            capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    return r.stdout if r.returncode == 0 else ""


def find_latest_delivery():
    out   = aws_s3_ls(f"s3://{S3_DELIVERY_BUCKET}/")
    dates = [m.group(1) for m in (S3_DATE_RE.search(l) for l in out.splitlines()) if m]
    return sorted(dates)[-1] if dates else None


# ── display helpers ───────────────────────────────────────────────────────────
def banner(msg):
    print(f"\n{'─' * 64}\n  {msg}\n{'─' * 64}")


def info(msg=""):
    print(f"  {msg}")


def err(msg):
    print(f"\n  [ERROR] {msg}", file=sys.stderr)


def checkpoint_prompt(label):
    """Block until the operator types y/yes or n/no."""
    print(f"\n{'═' * 64}")
    print(f"  ✋  HUMAN CHECKPOINT — {label}")
    print(f"{'═' * 64}")
    while True:
        try:
            ans = input("  Continue to next stage? [y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Interrupted.")
            return False
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            info("Aborting. Re-run with --start-at to resume from any stage.")
            return False
        info("Please enter y or n.")


# ── polling loop ──────────────────────────────────────────────────────────────
def poll_until_done(check_fn, log_fn, interval=POLL_INTERVAL_S):
    """
    Call check_fn() every `interval` seconds.
    check_fn() returns None while running, or a result string when done.
    log_fn() returns a string snippet to show in the spinner line.
    """
    spin = ["|", "/", "─", "\\"]
    i    = 0
    while True:
        result = check_fn()
        if result is not None:
            print()  # end spinner line
            return result
        snippet = (log_fn() or "").strip().splitlines()
        last    = snippet[-1] if snippet else ""
        print(f"\r  {spin[i % len(spin)]}  {last[:75]:<75}", end="", flush=True)
        i += 1
        time.sleep(interval)


# ── Stage 1: Download ─────────────────────────────────────────────────────────
def run_download(date):
    banner(f"Stage 1/4 — Download  s3://{S3_DELIVERY_BUCKET}/{date}/")
    dest = f"{DATA_ROOT}/originalRecords/{date}"

    count_str = ssm_run(
        f'find "{dest}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
    ).strip()
    if count_str.isdigit() and int(count_str) > 0:
        size = ssm_run(f'du -sh "{dest}" 2>/dev/null | cut -f1').strip()
        info(f"Already present: {count_str} files, {size}. Skipping download.")
        return

    info(f"Syncing s3://{S3_DELIVERY_BUCKET}/{date}/ → {dest}/")
    info(f"(timeout {DOWNLOAD_TIMEOUT_S // 60} min)")
    cmd = (
        f"aws s3 sync s3://{S3_DELIVERY_BUCKET}/{date}/ {dest}/ "
        f"--profile {AWS_PROFILE}"
    )
    ssm_run(cmd, timeout_seconds=DOWNLOAD_TIMEOUT_S, poll_seconds=15)

    count_str = ssm_run(
        f'find "{dest}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
    ).strip()
    size = ssm_run(f'du -sh "{dest}" 2>/dev/null | cut -f1').strip()
    info(f"Download complete: {count_str} files, {size}")


# ── Stage 2: Preprocess ───────────────────────────────────────────────────────
def run_preprocess(date):
    banner("Stage 2/4 — Preprocess  (fix-si.sh)")
    dest   = f"{DATA_ROOT}/originalRecords/{date}"
    backup = f"{dest}/original_backup"

    check = ssm_run(f'[ -d "{backup}" ] && echo yes || echo no').strip()
    if check == "yes":
        count = ssm_run(
            f'find "{backup}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
        ).strip()
        info(f"Already preprocessed (original_backup/ has {count} files). Skipping.")
        return

    info("Running fix-si.sh inline (this takes ~10–15 min) …")
    cmd = f"cd {INGEST_DIR} && ./scripts/harvest/fix-si.sh {dest}"
    ssm_run(cmd, timeout_seconds=PREPROCESS_TIMEOUT_S, poll_seconds=15)

    count = ssm_run(
        f'find "{backup}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
    ).strip()
    size  = ssm_run(f'du -sh "{dest}" 2>/dev/null | cut -f1').strip()
    info(f"fix-si.sh done. original_backup/ has {count} files. Folder: {size}")


def preprocess_checkpoint(date):
    dest   = f"{DATA_ROOT}/originalRecords/{date}"
    backup = f"{dest}/original_backup"
    raw_count    = ssm_run(
        f'find "{dest}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
    ).strip()
    backup_count = ssm_run(
        f'find "{backup}" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l'
    ).strip()
    folder_size  = ssm_run(f'du -sh "{dest}" 2>/dev/null | cut -f1').strip()

    info()
    info("Preprocessing summary:")
    info(f"  originalRecords/{date}/   : {raw_count} xml.gz files")
    info(f"  original_backup/          : {backup_count} xml.gz files")
    info(f"  Total folder size         : {folder_size}")
    info()
    info("Things to verify before continuing:")
    info("  • File counts match what you expect for this delivery")
    info("  • smithsonian.harvest.endpoint in i3.conf points to this date")
    info("    (grep '^smithsonian.harvest.endpoint' ~/ingestion3-conf/i3.conf)")


# ── Stage 3: Harvest ──────────────────────────────────────────────────────────
def run_harvest():
    banner("Stage 3/4 — Harvest  (harvest.sh smithsonian)")

    # If _SUCCESS already exists, skip.
    check = ssm_run(
        f'LATEST=$(ls -1dt {DATA_ROOT}/harvest/*/ 2>/dev/null | head -1 | sed "s:/$::")\n'
        f'[ -f "$LATEST/_SUCCESS" ] && echo "done:$LATEST" || echo pending'
    ).strip()
    if check.startswith("done:"):
        info(f"Harvest already complete (_SUCCESS found): {check[5:]}. Skipping.")
        return

    info(f"Launching harvest.sh smithsonian in background …")
    info(f"Log: {HARVEST_LOG}")
    info(f"Polling every {POLL_INTERVAL_S}s. Typically 3–4 hours.\n")

    cmd = f"cd {INGEST_DIR} && ./scripts/harvest.sh smithsonian > {HARVEST_LOG} 2>&1"
    ssm_bg(cmd)

    def check_fn():
        out = ssm_run(
            f'LATEST=$(ls -1dt {DATA_ROOT}/harvest/*/ 2>/dev/null | head -1 | sed "s:/$::")\n'
            f'RUNNING=$(pgrep -af "harvest.sh smithsonian" || true)\n'
            f'if [ -f "$LATEST/_SUCCESS" ]; then\n'
            f'  echo "done:$LATEST"\n'
            f'elif [ -z "$RUNNING" ] && [ -n "$LATEST" ]; then\n'
            f'  echo "failed:$LATEST"\n'
            f'else\n'
            f'  echo "running"\n'
            f'fi'
        ).strip()
        if out.startswith("done:") or out.startswith("failed:"):
            return out
        return None

    def log_fn():
        return ssm_run(f'tail -2 {HARVEST_LOG} 2>/dev/null || true')

    result = poll_until_done(check_fn, log_fn)
    if result.startswith("failed:"):
        err(f"Harvest process exited without _SUCCESS at: {result[7:]}")
        err(f"Check {HARVEST_LOG} for details. Re-run with --start-at harvest to retry.")
        sys.exit(1)


def harvest_checkpoint():
    out = ssm_run(
        f'LATEST=$(ls -1dt {DATA_ROOT}/harvest/*/ 2>/dev/null | head -1 | sed "s:/$::")\n'
        f'echo "path=$LATEST"\n'
        f'echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"\n'
        f'[ -f "$LATEST/_MANIFEST" ] && grep -i "record count" "$LATEST/_MANIFEST" | head -1 || true\n'
        f'echo "mtime=$(stat -c \'%y\' "$LATEST" 2>/dev/null | cut -d\'.\' -f1)"'
    ).strip()
    log = ssm_run(f'tail -{LOG_TAIL_LINES} {HARVEST_LOG} 2>/dev/null || echo "(no log)"')

    info()
    info("Harvest summary:")
    for line in out.splitlines():
        info(f"  {line}")
    info()
    info(f"Log tail (last {LOG_TAIL_LINES} lines of {HARVEST_LOG}):")
    for line in log.strip().splitlines():
        info(f"  {line}")
    info()
    info("Things to verify before continuing:")
    info("  • Record count is in the millions (Smithsonian typically ~7–8M)")
    info("  • _SUCCESS is present (success=yes above)")
    info("  • No ERROR / exception lines in the log tail above")


# ── Stage 4: Pipeline ─────────────────────────────────────────────────────────
def run_pipeline():
    banner("Stage 4/4 — Pipeline  (ingest.sh smithsonian --skip-harvest)")

    info("Launching ingest.sh smithsonian --skip-harvest in background …")
    info(f"Log: {PIPELINE_LOG}")
    info(f"Polling every {POLL_INTERVAL_S}s. Typically 1–2 hours.\n")

    cmd = (
        f"cd {INGEST_DIR} && "
        f"./scripts/ingest.sh smithsonian --skip-harvest > {PIPELINE_LOG} 2>&1"
    )
    ssm_bg(cmd)

    def check_fn():
        out = ssm_run(
            f'LATEST=$(ls -1dt {DATA_ROOT}/jsonl/*/ 2>/dev/null | head -1 | sed "s:/$::")\n'
            f'RUNNING=$(pgrep -af "ingest.sh smithsonian" || true)\n'
            f'if [ -f "$LATEST/_SUCCESS" ]; then\n'
            f'  echo "done:$LATEST"\n'
            f'elif [ -z "$RUNNING" ]; then\n'
            f'  echo "failed"\n'
            f'else\n'
            f'  echo "running"\n'
            f'fi'
        ).strip()
        if out.startswith("done:") or out == "failed":
            return out
        return None

    def log_fn():
        return ssm_run(f'tail -2 {PIPELINE_LOG} 2>/dev/null || true')

    result = poll_until_done(check_fn, log_fn)
    if result == "failed":
        err("Pipeline process exited without jsonl/_SUCCESS.")
        err(f"Check {PIPELINE_LOG} for details. Re-run with --start-at pipeline to retry.")
        sys.exit(1)


def pipeline_checkpoint():
    jsonl_out = ssm_run(
        f'LATEST=$(ls -1dt {DATA_ROOT}/jsonl/*/ 2>/dev/null | head -1 | sed "s:/$::")\n'
        f'echo "path=$LATEST"\n'
        f'echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"\n'
        f'[ -f "$LATEST/_MANIFEST" ] && grep -i "record count" "$LATEST/_MANIFEST" | head -1 || true'
    ).strip()
    log = ssm_run(f'tail -{LOG_TAIL_LINES} {PIPELINE_LOG} 2>/dev/null || echo "(no log)"')

    info()
    info("Pipeline summary (jsonl):")
    for line in jsonl_out.splitlines():
        info(f"  {line}")
    info()
    info(f"Log tail (last {LOG_TAIL_LINES} lines of {PIPELINE_LOG}):")
    for line in log.strip().splitlines():
        info(f"  {line}")
    info()
    info("Things to verify before finishing:")
    info("  • jsonl _SUCCESS is present (success=yes above)")
    info("  • Record count is close to the harvest count")
    info("  • Log shows S3 upload lines (no S3 errors)")


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Smithsonian ingest orchestrator — fully automated with human checkpoints."
    )
    parser.add_argument(
        "--date",
        help="Delivery date YYYY-MM-DD (default: latest in s3://dpla-hub-si/)",
    )
    parser.add_argument(
        "--start-at",
        choices=STAGE_ORDER,
        default="download",
        metavar="STAGE",
        help=f"Resume from this stage. Choices: {', '.join(STAGE_ORDER)}. Default: download",
    )
    args = parser.parse_args()

    date = args.date
    if not date:
        date = find_latest_delivery()
        if not date:
            sys.exit(f"Could not find any deliveries in s3://{S3_DELIVERY_BUCKET}/")
        print(f"(auto-detected latest delivery: {date})")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        sys.exit(f"Invalid --date {date!r}. Use YYYY-MM-DD.")

    start_idx = STAGE_ORDER.index(args.start_at)

    print(f"\n  Smithsonian Ingest Orchestrator")
    print(f"  Delivery : {date}")
    print(f"  Start at : {args.start_at}")

    # ── 1. Download ───────────────────────────────────────────────────────────
    if start_idx <= STAGE_ORDER.index("download"):
        run_download(date)

    # ── 2. Preprocess + checkpoint ────────────────────────────────────────────
    if start_idx <= STAGE_ORDER.index("preprocess"):
        run_preprocess(date)
        preprocess_checkpoint(date)
        if not checkpoint_prompt("after preprocessing — verify file counts and conf endpoint"):
            sys.exit(0)

    # ── 3. Harvest + checkpoint ───────────────────────────────────────────────
    if start_idx <= STAGE_ORDER.index("harvest"):
        run_harvest()
        harvest_checkpoint()
        if not checkpoint_prompt("after harvest — verify record count and log"):
            sys.exit(0)

    # ── 4. Pipeline + checkpoint ──────────────────────────────────────────────
    if start_idx <= STAGE_ORDER.index("pipeline"):
        run_pipeline()
        pipeline_checkpoint()
        if not checkpoint_prompt("after pipeline — verify jsonl output and S3 sync"):
            sys.exit(0)

    print("\n  ✅  All stages complete. Smithsonian ingest done.\n")


if __name__ == "__main__":
    main()