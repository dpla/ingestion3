#!/usr/bin/env python3
"""
DPLA NARA Ingest Status Checker

Shows the current state of a running or recently completed NARA ingest:
  - Whether nara-ingest.sh is running on EC2 and how long it's been going
  - Tail of the most recent log file
  - Disk usage of NARA data directories

Usage:
    python3 check_nara.py
    python3 check_nara.py --log /home/ec2-user/nara-ingest-202605.log
    python3 check_nara.py --tail 50
"""

import argparse
import base64
import json
import subprocess
import sys
import time

# ---------- config ----------
REGION      = "us-east-1"
INSTANCE_ID = "i-0a0def8581efef783"
LOG_DIR     = "/home/ec2-user"
DPLA_DATA   = "/home/ec2-user/dpla/data"  # matches common.sh: ${DPLA_DATA:-$HOME/dpla/data}
NARA_DATA   = f"{DPLA_DATA}/nara"


# ---------- AWS / SSM helpers ----------

def aws(args, check=True):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=60, poll_seconds=5):
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
        "--region", REGION,
        "--query", "Command.CommandId",
        "--output", "text",
    ])
    deadline = time.time() + timeout_seconds
    while True:
        time.sleep(poll_seconds)
        status = aws(["ssm", "get-command-invocation",
                      "--command-id", cmd_id, "--instance-id", INSTANCE_ID,
                      "--region", REGION,
                      "--query", "Status", "--output", "text"])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    out = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--region", REGION,
               "--query", "StandardOutputContent", "--output", "text"])
    err = aws(["ssm", "get-command-invocation", "--command-id", cmd_id,
               "--instance-id", INSTANCE_ID, "--region", REGION,
               "--query", "StandardErrorContent", "--output", "text"])
    if status != "Success":
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return out


def section(title):
    print()
    print("─" * 70)
    print(f"  {title}")
    print("─" * 70)


def main():
    parser = argparse.ArgumentParser(description="Check NARA ingest status on EC2")
    parser.add_argument("--log",  help="Specific log file to tail (default: most recent)")
    parser.add_argument("--tail", type=int, default=30, help="Lines to tail (default: 30)")
    args = parser.parse_args()

    print("\nNARA INGEST STATUS CHECK")
    print(f"EC2: {INSTANCE_ID}\n")
    print("Querying EC2 via SSM...")

    check_cmd = f"""
# ── Running processes ──────────────────────────────────────────────────
echo "=== PROCESSES ==="
NARA_PROCS=$(pgrep -af 'nara-ingest.sh|NaraMergeUtil|HarvestEntry|IngestRemap' 2>/dev/null || true)
if [ -n "$NARA_PROCS" ]; then
    echo "$NARA_PROCS"
    # Show elapsed time for main process
    MAIN_PID=$(pgrep -f 'nara-ingest.sh' | head -1 || true)
    if [ -n "$MAIN_PID" ]; then
        ELAPSED=$(ps -o etime= -p "$MAIN_PID" 2>/dev/null | tr -d ' ' || echo unknown)
        echo "nara-ingest.sh elapsed: $ELAPSED"
    fi
else
    echo "(no nara-ingest.sh or sbt processes running)"
fi

# ── Most recent log ────────────────────────────────────────────────────
echo ""
echo "=== LOG ==="
LOG_PATH="{args.log if args.log else ""}"
if [ -z "$LOG_PATH" ]; then
    LOG_PATH=$(ls -t {LOG_DIR}/nara-ingest-*.log 2>/dev/null | head -1 || true)
fi
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
    echo "File: $LOG_PATH"
    echo "Size: $(wc -l < "$LOG_PATH") lines"
    echo "---"
    tail -{args.tail} "$LOG_PATH"
else
    echo "(no nara-ingest-*.log found in {LOG_DIR})"
fi

# ── Disk usage ─────────────────────────────────────────────────────────
echo ""
echo "=== DISK USAGE ==="
for d in harvest delta/*/harvest delta originalRecords; do
    full="{NARA_DATA}/$d"
    if [ -e "$full" ]; then
        SIZE=$(du -sh "$full" 2>/dev/null | cut -f1)
        echo "$SIZE  $full"
    fi
done

# Latest merged harvest
echo ""
echo "=== LATEST MERGED HARVEST ==="
LATEST=$(ls -td {NARA_DATA}/harvest/*-nara-OriginalRecord.avro 2>/dev/null | head -1 || true)
if [ -n "$LATEST" ]; then
    echo "$LATEST"
    if [ -f "$LATEST/_LOGS/_SUMMARY.txt" ]; then
        echo "--- Merge summary ---"
        cat "$LATEST/_LOGS/_SUMMARY.txt"
    fi
else
    echo "(none found)"
fi
"""

    try:
        out = ssm_run(check_cmd, timeout_seconds=60, poll_seconds=5)
    except RuntimeError as e:
        print(f"\n  [BAD] SSM failed:\n{e}")
        sys.exit(1)

    # Pretty-print sections
    current_section = None
    for line in out.splitlines():
        if line.startswith("=== ") and line.endswith(" ==="):
            title = line[4:-4].title()
            section(title)
        else:
            print(f"  {line}")

    print()


if __name__ == "__main__":
    main()
