#!/usr/bin/env python3
"""
Read i3.conf for this month's hub schedule and fire all active standard
hubs as a sequential batch on the ingest EC2.

How it works
------------
1. Reads i3.conf FROM THE EC2 BOX via SSM (no local conf needed).
2. Parses the monthly schedule; skips special-case hubs (nara, smithsonian,
   community-webs) and on-hold hubs.
3. Builds a bash script that runs ingest.sh for each hub in sequence
   (blocking, not background) and fires it on EC2 as a nohup background job.
4. Returns immediately — EC2 runs the batch; Slack notifies per-hub progress.

Special hubs (nara, smithsonian/si, community-webs) are always skipped —
they need preprocessing steps before ingest.sh can run and must be launched
individually via their own scripts or the "Launch Hub Ingest" GHA workflow.

Usage
-----
  python3 run_monthly_batch.py               # current month
  python3 run_monthly_batch.py --month 9     # September
  python3 run_monthly_batch.py --dry-run     # print hub list, don't launch
  CI=1 INGEST_INSTANCE_ID=i-xxx python3 run_monthly_batch.py
"""

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── env / config ─────────────────────────────────────────────────────────────

HERE = Path(__file__).resolve().parent

def _load_env():
    cfg = {}
    env_file = HERE.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg

_env = _load_env()

INSTANCE_ID = os.environ.get("INGEST_INSTANCE_ID") or _env.get("INGEST_INSTANCE_ID", "")
GHA_ACTOR   = os.environ.get("GHA_ACTOR", "")
REGION      = "us-east-1"
CONF_PATH   = os.environ.get("I3_CONF") or "/home/ec2-user/ingestion3-conf/i3.conf"
SCRIPTS_DIR = "/home/ec2-user/ingestion3/scripts"
LOG_DIR     = "/home/ec2-user/data"

AWS_PROFILE = (
    os.environ.get("AWS_PROFILE")
    or _env.get("AWS_PROFILE")
    or ("dpla" if (HERE.parent / ".env").exists() else None)
)

SPECIAL_HUBS = {"nara", "smithsonian", "si", "community-webs"}

# ── output helpers ────────────────────────────────────────────────────────────

def _ts():
    return datetime.utcnow().strftime("%H:%M:%S UTC")

def info(msg):   print(f"  {msg}", flush=True)
def ok(msg):     print(f"  ✓ {msg}", flush=True)
def warn(msg):   print(f"  ⚠ {msg}", flush=True)
def bad(msg):    print(f"  ✗ {msg}", flush=True)

def header(msg):
    bar = "═" * 60
    print(f"\n{bar}", flush=True)
    print(f"  {msg}", flush=True)
    print(f"{bar}", flush=True)

# ── AWS / SSM ─────────────────────────────────────────────────────────────────

def _profile_args():
    return ["--profile", AWS_PROFILE] if AWS_PROFILE else []


def ssm_run(shell_cmd, timeout_seconds=60, poll_seconds=3):
    """Run a shell command on the EC2 instance via SSM; return stdout."""
    encoded = base64.b64encode(shell_cmd.encode()).decode()
    inner   = f"echo {encoded} | base64 -d | sudo -u ec2-user bash -l"
    params  = json.dumps({"commands": [inner]})

    cmd_id = subprocess.check_output(
        ["aws", "ssm", "send-command"] + _profile_args() + [
            "--instance-ids",  INSTANCE_ID,
            "--document-name", "AWS-RunShellScript",
            "--timeout-seconds", "30",
            "--parameters", params,
            "--region", REGION,
            "--query", "Command.CommandId",
            "--output", "text",
        ],
        text=True,
    ).strip()

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        time.sleep(poll_seconds)
        r = subprocess.run(
            ["aws", "ssm", "get-command-invocation",
             "--command-id", cmd_id,
             "--instance-id", INSTANCE_ID,
             "--region", REGION] + _profile_args(),
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            continue
        data   = json.loads(r.stdout)
        status = data.get("Status", "")
        if status in ("Success", "Failed", "TimedOut", "Cancelled"):
            return data.get("StandardOutputContent", "")
    raise RuntimeError(f"SSM command timed out after {timeout_seconds}s")

# ── i3.conf parsing ───────────────────────────────────────────────────────────

def already_ran_this_month(hub, month, year):
    """Return True if a JSONL snapshot for hub exists in S3 dated this month.

    Checks s3://dpla-master-dataset/{hub}/jsonl/ for any entry whose name
    starts with YYYY-MM — meaning the hub was already ingested this month
    and should be skipped in the monthly batch.

    Raises RuntimeError on AWS errors (non-zero exit) so that a credentials
    or bucket problem aborts hub selection rather than silently treating every
    hub as un-run.
    """
    prefix = f"{year}-{month:02d}"
    r = subprocess.run(
        ["aws", "s3", "ls", f"s3://dpla-master-dataset/{hub}/jsonl/",
         "--region", REGION] + _profile_args(),
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"aws s3 ls failed for hub '{hub}' (exit {r.returncode}): {r.stderr.strip()}"
        )
    # Extract just the object name from each ls line to avoid matching the
    # timestamp column (e.g. "2026-09-16 12:34:56") against the YYYY-MM prefix.
    # PRE lines: "                           PRE 2026-09-16T123456/"
    # File lines: "2026-09-16 12:34:56      12345 filename"
    for line in r.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        name = parts[-1].rstrip("/")   # last field is always the object name
        if name.startswith(prefix):
            return True
    return False


def get_monthly_hubs(month):
    """Read i3.conf from EC2 via SSM; return active standard hub names for the month.

    Special hubs (nara, smithsonian/si, community-webs) are always excluded —
    they require preprocessing steps and must be launched separately.
    Hubs with a JSONL snapshot already dated this month are also skipped.
    """
    info(f"Reading i3.conf from EC2 ({CONF_PATH})…")
    # Grep only schedule lines — catting the full file hits SSM's ~48 KB
    # StandardOutputContent limit and silently truncates large confs.
    conf_text = ssm_run(
        f"grep -E '^[a-z0-9_-]+\\.schedule\\.' {CONF_PATH} 2>/dev/null || echo ''",
        timeout_seconds=30,
    )
    if not conf_text.strip():
        sys.exit(f"[bad] Could not read schedule entries from i3.conf at {CONF_PATH} on EC2.")

    text = re.sub(r"(?m)^\s*(#|//).*$", "", conf_text)

    hubs: dict[str, dict] = {}
    for m in re.finditer(
        r"^\s*([a-z0-9_-]+)\.schedule\.months\s*[=:]\s*\[([0-9,\s]+)\]",
        text, re.MULTILINE | re.IGNORECASE,
    ):
        name   = m.group(1).lower()
        months = [int(x.strip()) for x in m.group(2).split(",") if x.strip().isdigit()]
        hubs.setdefault(name, {})["months"] = months

    for m in re.finditer(
        r"""^\s*([a-z0-9_-]+)\.schedule\.status\s*[=:]\s*["']([^"']+)["']""",
        text, re.MULTILINE | re.IGNORECASE,
    ):
        hubs.setdefault(m.group(1).lower(), {})["status"] = m.group(2)

    scheduled = []
    for name, data in sorted(hubs.items()):
        if "months" not in data or month not in data["months"]:
            continue
        if (data.get("status") or "active").lower() != "active":
            continue
        if name in SPECIAL_HUBS:
            continue
        scheduled.append(name)

    # Skip hubs already ingested this month (e.g. run manually or via single-hub GHA).
    # Propagate S3 errors — a failed lookup should abort, not silently treat the hub as un-run.
    year = datetime.now().year
    already_done = []
    for h in scheduled:
        try:
            if already_ran_this_month(h, month, year):
                already_done.append(h)
        except RuntimeError as e:
            sys.exit(f"[bad] S3 check failed for hub '{h}': {e}")
    if already_done:
        warn(f"Skipping {len(already_done)} hub(s) already ingested this month: {', '.join(already_done)}")

    return [h for h in scheduled if h not in already_done]

# ── batch launch ──────────────────────────────────────────────────────────────

def build_batch_script(hubs, batch_log, gha_actor=""):
    """Return a bash script that runs ingest.sh for each hub sequentially.

    The script writes its own log via >> redirection; the nohup wrapper in
    fire_batch() redirects to /dev/null to avoid double-writing the same file.
    """
    hub_list = " ".join(hubs)
    actor_export = f'export GHA_ACTOR="{gha_actor}"' if gha_actor else ""
    triggered_by = f" via GHA (triggered by {gha_actor})" if gha_actor else ""
    return f"""\
#!/bin/bash
# Monthly batch — generated by run_monthly_batch.py
# Hubs: {hub_list}
set -uo pipefail
source /home/ec2-user/ingestion3/scripts/common.sh
{actor_export}
HUBS=({hub_list})
BATCH_LOG="{batch_log}"
SCRIPTS_DIR="/home/ec2-user/ingestion3/scripts"
FAILED=()

_log() {{ echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ')  $*" >> "$BATCH_LOG"; }}

_log "▶ Monthly batch started{triggered_by} — ${{#HUBS[@]}} hubs: {hub_list}"
slack_notify ":calendar: *Monthly batch started*{triggered_by} — ${{#HUBS[@]}} hubs: {hub_list}"

for HUB in "${{HUBS[@]}}"; do
    _log "▶ Starting $HUB"
    bash "$SCRIPTS_DIR/ingest.sh" "$HUB"
    RC=$?
    if [ $RC -eq 0 ]; then
        _log "✓ $HUB complete"
    else
        _log "✗ $HUB FAILED (exit $RC)"
        FAILED+=("$HUB")
    fi
done

if [ ${{#FAILED[@]}} -gt 0 ]; then
    _log "✗ Batch finished with failures: ${{FAILED[*]}}"
    slack_notify ":x: *Monthly batch finished with failures* — failed: ${{FAILED[*]}}"
    exit 1
else
    _log "✓ Batch complete — all ${{#HUBS[@]}} hubs ingested"
    slack_notify ":tada: *Monthly batch complete* — all ${{#HUBS[@]}} hubs ingested"
fi
"""


LOCK_PATH = "/tmp/monthly-batch.lock"


def fire_batch(hubs, batch_log):
    """Write the batch script to EC2 and run it as a nohup background job.

    Uses mktemp under /home/ec2-user (mode 0700, owned by ec2-user) so the
    script file is never world-readable and each dispatch gets a unique path.
    A flock probe fast-fails if another batch is already running; the nohup
    wrapper also uses flock -n so a second dispatch that slips through the
    probe's TOCTOU window fails immediately rather than waiting.  The script
    cleans itself up after execution.
    The script logs via _log() → $BATCH_LOG directly; nohup goes to /dev/null
    to avoid mixing ingest.sh stdout into the structured batch log.
    """
    script = build_batch_script(hubs, batch_log, gha_actor=GHA_ACTOR)
    encoded = base64.b64encode(script.encode()).decode()

    # 1. mktemp: unique path, restricted permissions, owned by ec2-user.
    # 2. Probe: fast-fail if lock is held (synchronous error visible via SSM).
    # 3. Launch: nohup wrapper holds the lock for the script's full duration
    #    with flock -n (fail-fast if probe's TOCTOU window allowed a race).
    #    rm -f inside the wrapper cleans up the script after it finishes.
    cmd = (
        f"SCRIPT=$(mktemp /home/ec2-user/monthly-batch-XXXXXX.sh) && "
        f"chmod 0700 \"$SCRIPT\" && "
        f"echo {encoded} | base64 -d > \"$SCRIPT\" && "
        f"flock -n {LOCK_PATH} true || "
        f"  {{ echo 'ERROR: a monthly batch is already running; try again later'; rm -f \"$SCRIPT\"; exit 1; }} && "
        f"nohup bash -c \"flock -n {LOCK_PATH} bash \\\"$SCRIPT\\\"; rm -f \\\"$SCRIPT\\\"\" > /dev/null 2>&1 </dev/null & "
        f"echo \"Batch PID=$!\""
    )
    out = ssm_run(cmd, timeout_seconds=30)
    if "ERROR:" in out:
        sys.exit(f"[bad] {out.strip()}")
    pid_match = re.search(r"PID=(\d+)", out)
    pid = pid_match.group(1) if pid_match else "unknown"
    ok(f"Batch script launched on EC2 (PID {pid})")
    info(f"Log:  {batch_log}")
    info(f"Tail: ssh ec2-user@<box> tail -f {batch_log}")
    info(f"Or:   python3 ingest_python_scripts/check_ingest.py <hub>")

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fire this month's standard hub ingests as a sequential batch on EC2."
    )
    parser.add_argument("--month", type=int, help="Month 1-12 (default: current month)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print hub list without launching")
    args = parser.parse_args()

    month = args.month if args.month is not None else datetime.now().month
    if not (1 <= month <= 12):
        sys.exit(f"Invalid month: {month}")

    if not INSTANCE_ID:
        sys.exit("INGEST_INSTANCE_ID is not set — export it or add it to .env")

    header(f"Monthly batch — month {month}")

    hubs = get_monthly_hubs(month)

    if not hubs:
        info("No active standard hubs scheduled for this month.")
        sys.exit(0)

    info(f"\n  {len(hubs)} hub(s) queued:")
    for h in hubs:
        print(f"    • {h}", flush=True)
    print(flush=True)

    if args.dry_run:
        info("Dry run — not launching.")
        sys.exit(0)

    batch_log = f"{LOG_DIR}/monthly-batch-{datetime.utcnow().strftime('%Y%m')}.log"
    fire_batch(hubs, batch_log)

    ok("Batch is running on EC2. GHA step is complete.")
    info("Slack will notify as each hub finishes.")


if __name__ == "__main__":
    main()
