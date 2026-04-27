#!/usr/bin/env python3
"""
Check on a running (or recently finished) DPLA hub ingest.

Reports four things in one shot, via a single SSM round-trip to the box:
  1. Whether the ingest.sh process is alive, and how long it's been running.
  2. Per-stage status (harvest, mapping, enrichment, jsonl) — looks for the
     latest timestamped output dir and the _SUCCESS marker inside it.
  3. Record counts from each completed stage's _MANIFEST.
  4. Tail of the ingest log, with a separate "errors only" filter.

Usage:
    python3 check_ingest.py                  # prompts for hub
    python3 check_ingest.py bpl              # checks bpl
    python3 check_ingest.py bpl --watch      # re-runs every 30 seconds
    python3 check_ingest.py bpl --watch 60   # custom interval
    python3 check_ingest.py bpl --tail 100   # show more log lines

Requirements: aws CLI authenticated, IAM perms for ssm:SendCommand and
ssm:GetCommandInvocation against the ingest EC2.
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
INSTANCE_ID = "i-0a0def8581efef783"
DATA_ROOT = "/home/ec2-user/data"
STAGES = ("harvest", "mapping", "enrichment", "jsonl")
HUB_RE = re.compile(r"^[a-z0-9_-]+$")

# ---------- AWS CLI wrappers (same pattern as prechecks.py) ----------
def aws(args):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


def ssm_run(shell_cmd, timeout_seconds=60, poll_seconds=4):
    """Send a shell command to the EC2 instance via SSM (as ec2-user, login shell)
    and return its stdout. Uses base64 to avoid quoting traps."""
    encoded = base64.b64encode(shell_cmd.encode("utf-8")).decode("ascii")
    wrapped = f"sudo -u ec2-user bash -lc 'echo {encoded} | base64 -d | bash -l'"
    params = json.dumps({"commands": [wrapped]})

    cmd_id = aws([
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
        status = aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--query", "Status",
            "--output", "text",
        ])
        if status not in ("Pending", "InProgress", "Delayed"):
            break
        if time.time() > deadline:
            raise RuntimeError(f"SSM command {cmd_id} timed out after {timeout_seconds}s")

    output = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])

    if status != "Success":
        err = aws([
            "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE_ID,
            "--query", "StandardErrorContent",
            "--output", "text",
        ])
        raise RuntimeError(f"SSM command ended with status {status}.\nSTDOUT:\n{output}\nSTDERR:\n{err}")
    return output


# ---------- the bash payload ----------
def build_status_script(hub: str, tail_lines: int) -> str:
    """One bash script that prints all status info, sectioned with === markers."""
    return f"""
HUB="{hub}"
LOG="{DATA_ROOT}/${{HUB}}-ingest.log"

echo "===PROCESS==="
PIDS=$(pgrep -f "ingest.sh ${{HUB}}" || true)
if [ -z "$PIDS" ]; then
  echo "(no ingest.sh process running for ${{HUB}})"
else
  for p in $PIDS; do
    ps -o pid=,etime=,cmd= -p $p
  done
fi

echo "===STAGES==="
for stage in {' '.join(STAGES)}; do
  STAGE_DIR="{DATA_ROOT}/${{HUB}}/${{stage}}"
  LATEST=$(ls -1dt ${{STAGE_DIR}}/*/ 2>/dev/null | head -1 | sed 's:/$::')
  if [ -z "$LATEST" ]; then
    echo "${{stage}}|NOT_STARTED||"
    continue
  fi
  MTIME=$(stat -c '%y' "$LATEST" 2>/dev/null | cut -d'.' -f1)
  MANIFEST=""
  if [ -f "$LATEST/_MANIFEST" ]; then
    MANIFEST=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')
  fi
  if [ -f "$LATEST/_SUCCESS" ]; then
    echo "${{stage}}|DONE|${{MTIME}}|${{MANIFEST}}"
  else
    echo "${{stage}}|IN_PROGRESS|${{MTIME}}|"
  fi
done

echo "===LOG==="
if [ -f "$LOG" ]; then
  SIZE=$(stat -c '%s' "$LOG")
  MTIME=$(stat -c '%y' "$LOG" | cut -d'.' -f1)
  LINES=$(wc -l < "$LOG")
  echo "size=${{SIZE}} bytes   mtime=${{MTIME}}   lines=${{LINES}}"
  echo "---tail-{tail_lines}---"
  tail -{tail_lines} "$LOG"
  echo "---errors---"
  grep -E -i 'error|exception|failed|:x:|FATAL' "$LOG" | tail -10 || echo "(no error lines found)"
else
  echo "(no log file at ${{LOG}})"
fi
""".strip()


# ---------- pretty-printers ----------
GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"; DIM = "\033[2m"; RESET = "\033[0m"
USE_COLOR = sys.stdout.isatty()

def c(color, text):
    return f"{color}{text}{RESET}" if USE_COLOR else text


def parse_sections(out: str) -> dict:
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


def render(hub: str, sections: dict) -> str:
    lines = []
    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  Ingest status: {c(GREEN, hub)}   (instance {INSTANCE_ID})")
    lines.append(c(DIM, "=" * 70))

    # Process
    proc = sections.get("PROCESS", "").strip()
    lines.append("")
    lines.append("PROCESS")
    if proc.startswith("(no ingest"):
        lines.append("  " + c(YELLOW, proc))
    else:
        for ln in proc.splitlines():
            lines.append("  " + c(GREEN, ln))

    # Stages
    lines.append("")
    lines.append("STAGES")
    stage_data = sections.get("STAGES", "").strip().splitlines()
    for ln in stage_data:
        parts = ln.split("|", 3)
        if len(parts) < 4:
            lines.append("  " + ln)
            continue
        stage, status, mtime, manifest = parts
        if status == "DONE":
            tag = c(GREEN, "DONE       ")
        elif status == "IN_PROGRESS":
            tag = c(YELLOW, "IN PROGRESS")
        else:
            tag = c(DIM, "not started")
        extra = []
        if mtime:
            extra.append(f"mtime {mtime}")
        if manifest:
            extra.append(manifest.strip())
        suffix = "   " + "   ".join(extra) if extra else ""
        lines.append(f"  {stage:<11} {tag}{suffix}")

    # Log
    lines.append("")
    lines.append("LOG")
    log = sections.get("LOG", "")
    for ln in log.splitlines():
        if ln.startswith("---"):
            lines.append("  " + c(DIM, ln))
        elif "error" in ln.lower() or "exception" in ln.lower() or ":x:" in ln.lower() or "fatal" in ln.lower():
            lines.append("  " + c(RED, ln))
        elif ln.startswith("size="):
            lines.append("  " + c(DIM, ln))
        else:
            lines.append("  " + ln)

    # Top-line summary
    summary = derive_summary(sections)
    lines.append("")
    lines.append(f"SUMMARY: {summary}")
    lines.append("")
    return "\n".join(lines)


def derive_summary(sections: dict) -> str:
    proc = sections.get("PROCESS", "").strip()
    is_running = not proc.startswith("(no ingest")

    stage_status = {}
    for ln in sections.get("STAGES", "").splitlines():
        parts = ln.split("|", 3)
        if len(parts) >= 2:
            stage_status[parts[0]] = parts[1]

    log = sections.get("LOG", "").lower()
    has_errors = bool(re.search(r"\b(error|exception|failed|fatal)\b", log)) or ":x:" in log

    if is_running:
        in_progress_stage = next((s for s in STAGES if stage_status.get(s) == "IN_PROGRESS"), None)
        if in_progress_stage:
            return c(YELLOW, f"running — currently in {in_progress_stage}")
        return c(YELLOW, "running")

    # Not running. Did it finish?
    if all(stage_status.get(s) == "DONE" for s in STAGES):
        if has_errors:
            return c(YELLOW, "all stages DONE but error lines were found in log — check log section")
        return c(GREEN, "complete — all stages DONE")
    if has_errors:
        return c(RED, "process is gone, not all stages complete, errors in log — likely failed")
    return c(YELLOW, "process is gone, not all stages complete — possibly failed or never started")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Check on a DPLA hub ingest.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. bpl). Prompts if omitted.")
    parser.add_argument("--tail", type=int, default=30, help="Number of log lines to show (default 30).")
    parser.add_argument(
        "--watch", nargs="?", const=30, type=int, default=None,
        help="Re-run every N seconds (default 30 if --watch is given without a value).",
    )
    args = parser.parse_args()

    hub = args.hub
    if not hub:
        try:
            hub = input("Hub: ").strip().lower()
        except EOFError:
            sys.exit("No hub provided.")
    if not HUB_RE.match(hub):
        sys.exit(f"Invalid hub name: {hub!r}")

    script = build_status_script(hub, args.tail)

    def one_pass():
        try:
            out = ssm_run(script)
        except RuntimeError as e:
            print(f"\n[ERROR] {e}\n")
            return
        sections = parse_sections(out)
        print(render(hub, sections))

    if args.watch is None:
        one_pass()
        return

    # Watch mode.
    interval = args.watch
    try:
        while True:
            # Clear screen for a tidy refresh.
            os.system("clear" if os.name == "posix" else "cls")
            print(time.strftime("Last refresh: %Y-%m-%d %H:%M:%S"))
            one_pass()
            print(c(DIM, f"(refreshing every {interval}s — Ctrl+C to exit)"))
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()