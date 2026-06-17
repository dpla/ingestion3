#!/usr/bin/env python3
"""
DPLA NARA Ingest Status Checker

Shows the current state of a running or recently completed NARA ingest:
  - Whether nara-ingest.sh / NaraMergeUtil / IngestRemap is running and for how long
  - Which stage is currently active (harvest, merge, mapping, enrichment, jsonl)
  - Latest merged harvest and its summary
  - Disk usage of key NARA data directories
  - Tail of the most recent log file

Usage:
    python3 check_nara.py
    python3 check_nara.py --month 202604
    python3 check_nara.py --tail 50
    python3 check_nara.py --watch        # refresh every 30s
    python3 check_nara.py --watch 60     # custom interval
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
REGION      = "us-east-1"
def _load_dotenv():
    cfg = {}
    env_file = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..", ".env")
    )
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = os.path.expanduser(v.strip().strip('"').strip("'"))
    creds = cfg.get("AWS_SHARED_CREDENTIALS_FILE")
    if creds:
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", creds)
    return cfg

_env = _load_dotenv()
INSTANCE_ID = _env.get("INGEST_INSTANCE_ID", "")
LOG_DIR     = "/home/ec2-user"
DATA_ROOT   = "/home/ec2-user/data"
NARA_DATA   = f"{DATA_ROOT}/nara"

STAGE_KEYWORDS = {
    "jsonl":        ["JsonlEntry", "jsonl complete", ":white_check_mark: jsonl"],
    "enrichment":   ["EnrichEntry", "enrichment complete", ":white_check_mark: enrichment"],
    "mapping":      ["MappingEntry", "IngestRemap", "mapping complete", ":white_check_mark: mapping"],
    "merge":        ["NaraMergeUtil", "Merge complete", "merge complete"],
    "harvest":      ["HarvestEntry", "FileHarvester", "harvest complete", "Preprocessing"],
    "delete-gate":  ["zero-delete gate", "halted at zero-delete gate", "Zero-delete gate triggered"],
}

ALL_STAGE_REGEX = (
    "NaraMergeUtil|Merge complete|"
    "HarvestEntry|FileHarvester|Preprocessing|"
    "MappingEntry|IngestRemap|"
    "EnrichEntry|JsonlEntry|"
    "harvest complete|merge complete|mapping complete|enrichment complete|jsonl complete|"
    "zero-delete gate|halted at zero-delete gate"
)


# ---------- AWS / SSM helpers ----------
def aws(args, check=True):
    profile = [] if any(a.startswith("--profile") or a.startswith("-p") for a in args) else ["--profile", "dpla"]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=60, poll_seconds=4):
    encoded = base64.b64encode(shell_cmd.encode("utf-8")).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params = json.dumps({"commands": [wrapped]})
    cmd_id = aws([
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
        status = aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--region", REGION,
            "--query", "Status",
            "--output", "text",
        ])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM command timed out after {timeout_seconds}s")
    output = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--region", REGION,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])
    if status != "Success":
        err = aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--region", REGION,
            "--query", "StandardErrorContent",
            "--output", "text",
        ])
        raise RuntimeError(f"SSM status={status}\nSTDOUT:\n{output}\nSTDERR:\n{err}")
    return output


# ---------- bash payload ----------
def build_status_script(month_filter="", tail_lines=30):
    log_glob = (
        f"{LOG_DIR}/nara-ingest-{month_filter}*.log"
        if month_filter else
        f"{LOG_DIR}/nara-ingest-*.log"
    )
    return f"""
echo "===PROCESS==="
PIDS=$(pgrep -af 'nara-ingest.sh|NaraMergeUtil|IngestRemap' 2>/dev/null || true)
if [ -z "$PIDS" ]; then
  echo "(none)"
else
  echo "$PIDS"
  MAIN_PID=$(pgrep -f 'nara-ingest.sh' | head -1 || true)
  if [ -n "$MAIN_PID" ]; then
    ELAPSED=$(ps -o etime= -p "$MAIN_PID" 2>/dev/null | tr -d ' ' || echo unknown)
    echo "nara-ingest.sh elapsed: $ELAPSED"
  fi
fi

echo "===LOGINFO==="
LOG_PATH=$(ls -t {log_glob} 2>/dev/null | head -1 || true)
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
  echo "path=$LOG_PATH"
  echo "lines=$(wc -l < "$LOG_PATH")"
  echo "mtime=$(stat -c '%y' "$LOG_PATH" | cut -d'.' -f1)"
else
  echo "(no log)"
fi

echo "===STAGE_RECENT==="
LOG_PATH=$(ls -t {log_glob} 2>/dev/null | head -1 || true)
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
  tail -200 "$LOG_PATH" 2>/dev/null | grep -E "{ALL_STAGE_REGEX}" | tail -10
fi

echo "===MERGE_SUMMARY==="
LATEST=$(ls -td {NARA_DATA}/harvest/*-nara-OriginalRecord.avro 2>/dev/null | head -1 || true)
if [ -n "$LATEST" ]; then
  echo "path=$LATEST"
  if [ -f "$LATEST/_LOGS/_SUMMARY.txt" ]; then
    cat "$LATEST/_LOGS/_SUMMARY.txt"
  fi
else
  echo "(none)"
fi

echo "===DISK==="
for d in harvest originalRecords; do
  full="{NARA_DATA}/$d"
  if [ -e "$full" ]; then
    SIZE=$(du -sh "$full" 2>/dev/null | cut -f1)
    echo "$SIZE  $full"
  fi
done

echo "===LOG_TAIL==="
LOG_PATH=$(ls -t {log_glob} 2>/dev/null | head -1 || true)
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
  tail -{tail_lines} "$LOG_PATH"
fi
""".strip()


# ---------- parsers ----------
def parse_sections(out):
    sections = {}
    current = None
    buf = []
    for line in out.splitlines():
        m = re.match(r"^===(\w+)===$", line.strip())
        if m:
            if current is not None:
                sections[current] = "\n".join(buf).rstrip()
            current = m.group(1)
            buf = []
        else:
            buf.append(line)
    if current is not None:
        sections[current] = "\n".join(buf).rstrip()
    return sections


def detect_current_stage(stage_recent):
    for line in reversed(stage_recent.splitlines()):
        for stage, keywords in STAGE_KEYWORDS.items():
            for kw in keywords:
                if kw in line:
                    return stage
    return None


def parse_process_lines(proc_text):
    rows = []
    elapsed = None
    for ln in proc_text.splitlines():
        ln = ln.strip()
        if not ln or ln == "(none)":
            continue
        m = re.match(r"nara-ingest\.sh elapsed: (.+)", ln)
        if m:
            elapsed = m.group(1)
            continue
        m = re.match(r"^\s*(\d+)\s+(.*)$", ln)
        if not m:
            continue
        pid, rest = m.group(1), m.group(2)
        if "IngestRemap" in rest:
            script = "IngestRemap (Spark)"
        elif "NaraMergeUtil" in rest:
            script = "NaraMergeUtil (Spark)"
        elif "nara-ingest.sh" in rest:
            script = "nara-ingest.sh"
        else:
            script = rest[:80]
        rows.append({"pid": pid, "script": script})
    return rows, elapsed


# ---------- color ----------
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
DIM    = "\033[2m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
USE_COLOR = sys.stdout.isatty()


def c(color, text):
    return f"{color}{text}{RESET}" if USE_COLOR else text


# ---------- render ----------
def render(sections):
    lines = []
    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  NARA ingest status   (instance {INSTANCE_ID})")
    lines.append(c(DIM, "=" * 70))

    # PROCESS
    proc_text = sections.get("PROCESS", "").strip()
    is_running = bool(proc_text and proc_text != "(none)")
    lines.append("")
    lines.append("PROCESS")
    if not is_running:
        lines.append("  " + c(YELLOW, "(no NARA ingest process running)"))
    else:
        rows, elapsed = parse_process_lines(proc_text)
        if rows:
            primary = rows[0]
            etime_str = f"   (running for {elapsed})" if elapsed else ""
            lines.append(f"  Script:  {c(GREEN, primary['script'])}")
            lines.append(f"  PID:     {primary['pid']}{etime_str}")
            if len(rows) > 1:
                lines.append(c(DIM, f"  + {len(rows) - 1} subprocess(es)"))
        else:
            lines.append("  " + c(GREEN, proc_text.splitlines()[0]))

    # CURRENT STAGE
    stage_recent = sections.get("STAGE_RECENT", "")
    current_stage = detect_current_stage(stage_recent)
    lines.append("")
    lines.append("CURRENT STAGE")
    if not is_running:
        lines.append(c(DIM, "  (no process running — see SUMMARY)"))
    elif not current_stage:
        lines.append(c(YELLOW, "  Could not infer current stage from log."))
    else:
        lines.append(f"  Stage:    {c(YELLOW, current_stage)}")
        if current_stage in ("harvest", "merge"):
            lines.append(c(DIM, "  Progress: (file-based — no offset to parse)"))
        else:
            lines.append(c(DIM, "  Progress: (not derivable for Spark stages from log alone)"))

    # LATEST MERGED HARVEST
    merge_text = sections.get("MERGE_SUMMARY", "").strip()
    lines.append("")
    lines.append("LATEST MERGED HARVEST")
    if not merge_text or merge_text == "(none)":
        lines.append(c(DIM, "  (none found)"))
    else:
        for ln in merge_text.splitlines():
            if ln.startswith("path="):
                lines.append(c(DIM, f"  {ln.split('=', 1)[1]}"))
            else:
                lines.append(f"  {ln}")

    # DISK USAGE
    disk_text = sections.get("DISK", "").strip()
    lines.append("")
    lines.append("DISK USAGE")
    if not disk_text:
        lines.append(c(DIM, "  (no data directories found)"))
    else:
        for ln in disk_text.splitlines():
            if ln.strip():
                lines.append(f"  {ln}")

    # LOG TAIL
    log_tail = sections.get("LOG_TAIL", "").rstrip()
    log_info = sections.get("LOGINFO", "")
    log_path = next(
        (ln.split("=", 1)[1] for ln in log_info.splitlines() if ln.startswith("path=")),
        None,
    )
    lines.append("")
    lines.append("RECENT LOG")
    if log_path:
        lines.append(c(DIM, f"  {log_path}"))
    if log_tail and log_tail.strip() != "(no log)":
        for ln in log_tail.splitlines():
            lines.append(f"  {ln}")
    else:
        lines.append(c(DIM, "  (no log file found)"))

    # SUMMARY
    summary_text = derive_summary(is_running, current_stage, sections)
    lines.append("")
    lines.append("SUMMARY: " + summary_text)

    # If the process halted at the zero-delete gate, show resume instructions
    log_tail = sections.get("LOG_TAIL", "")
    if not is_running and re.search(r"zero-delete gate|halted at zero-delete gate", log_tail, re.IGNORECASE):
        lines.append("")
        lines.append(c(YELLOW, "  ZERO-DELETE GATE — action required:"))
        lines.append("  1. Review the merge summary above for valid/invalid delete counts.")
        lines.append("  2. Check the delivery: were NAC_DESC_Deletes_*.xml files present?")
        lines.append("  3. If 0 deletes is expected for this delivery, resume with:")
        lines.append(c(DIM, "       python3 nara/launch_nara.py --skip-to-pipeline --skip-delete-check"))
        lines.append("  4. Otherwise, investigate before re-running.")

    lines.append("")
    return "\n".join(lines)


def derive_summary(is_running, current_stage, sections):
    if is_running:
        if current_stage == "delete-gate":
            return c(YELLOW, "running — waiting at zero-delete gate (operator action needed)")
        if current_stage:
            return c(YELLOW, f"running — currently in {current_stage}")
        return c(YELLOW, "running — stage unclear")

    log_tail = sections.get("LOG_TAIL", "")

    # Zero-delete gate halt: process exited non-zero after the gate fired
    if re.search(r"zero-delete gate|halted at zero-delete gate", log_tail, re.IGNORECASE):
        return c(YELLOW, "HALTED — zero-delete gate (see below for resume instructions)")

    if re.search(r"\[SUCCESS\].*jsonl|jsonl complete|:white_check_mark: jsonl", log_tail, re.IGNORECASE):
        return c(GREEN, "complete — all stages done")
    merge_text = sections.get("MERGE_SUMMARY", "").strip()
    if merge_text and merge_text != "(none)":
        return c(YELLOW, "process exited — merge done, pipeline status unclear (check log)")
    return c(RED, "process exited — check log for errors")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Check NARA ingest status on EC2")
    parser.add_argument("--month", help="Filter to a specific month (YYYYMM)")
    parser.add_argument("--tail",  type=int, default=30, help="Log lines to show (default: 30)")
    parser.add_argument(
        "--watch", nargs="?", const=30, type=int, default=None,
        help="Re-run every N seconds (default 30 if --watch given without a value)",
    )
    args = parser.parse_args()

    script = build_status_script(month_filter=args.month or "", tail_lines=args.tail)

    def one_pass():
        try:
            out = ssm_run(script, timeout_seconds=60, poll_seconds=4)
        except RuntimeError as e:
            print(f"\n  [ERROR] {e}\n")
            return
        sections = parse_sections(out)
        print(render(sections))

    if args.watch is None:
        one_pass()
        return

    interval = args.watch
    try:
        while True:
            os.system("clear" if os.name == "posix" else "cls")
            print(time.strftime("Last refresh: %Y-%m-%d %H:%M:%S"))
            one_pass()
            print(c(DIM, f"(refreshing every {interval}s — Ctrl+C to exit)"))
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
