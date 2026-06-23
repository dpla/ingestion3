#!/usr/bin/env python3
"""
Check on a running (or recently finished) Smithsonian ingest.

Reports the things that matter while you're waiting:
  - Whether harvest.sh or ingest.sh is still alive, and for how long.
  - Which stages have completed (with record counts).
  - Which stage is currently running, when it started, and (for harvest)
    how far through it we are.

Usage:
    python3 check_status_smithsonian.py            # one-shot
    python3 check_status_smithsonian.py --watch    # refresh every 30s
    python3 check_status_smithsonian.py --watch 60 # custom interval
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

# ---------- config ----------
DATA_ROOT    = "/home/ec2-user/data/smithsonian"
HARVEST_LOG  = "/home/ec2-user/data/si-harvest.log"
PIPELINE_LOG = "/home/ec2-user/data/smithsonian-ingest.log"

STAGES = ("harvest", "mapping", "enrichment", "jsonl")

STAGE_KEYWORDS = {
    "jsonl":      ["JsonlEntry", "jsonl complete"],
    "enrichment": ["EnrichEntry", "enrichment complete"],
    "mapping":    ["MappingEntry", "IngestRemap", "mapping complete"],
    "harvest":    ["SiFileHarvester", "Harvested"],
}
PIPELINE_STAGE_REGEX = "MappingEntry|IngestRemap|EnrichEntry|JsonlEntry|mapping complete|enrichment complete|jsonl complete"


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
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")


# ---------- AWS / SSM ----------
def aws(args):
    profile = [] if any(a.startswith("--profile") for a in args) else ["--profile", AWS_PROFILE]
    result = subprocess.run(["aws"] + profile + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args)} failed:\n{result.stderr.strip()}")
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
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    output = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])
    if status != "Success":
        try:
            stderr = aws([
                "ssm", "get-command-invocation",
                "--command-id", cmd_id,
                "--instance-id", INSTANCE_ID,
                "--query", "StandardErrorContent",
                "--output", "text",
            ])
        except Exception:
            stderr = "(could not fetch stderr)"
        raise RuntimeError(f"SSM ended with status {status}.\nStdout:\n{output}\nStderr:\n{stderr}")
    return output


# ---------- bash payload ----------
def build_status_script():
    return f"""
echo "===PROCESS==="
HARVEST_PIDS=$(pgrep -f 'harvest\\.sh smithsonian' 2>/dev/null || true)
PIPELINE_PIDS=$(pgrep -f 'ingest\\.sh smithsonian' 2>/dev/null || true)
if [ -n "$HARVEST_PIDS" ]; then
  for p in $HARVEST_PIDS; do ps -o pid=,etime=,cmd= -p $p 2>/dev/null; done
elif [ -n "$PIPELINE_PIDS" ]; then
  for p in $PIPELINE_PIDS; do ps -o pid=,etime=,cmd= -p $p 2>/dev/null; done
else
  echo "(none)"
fi

echo "===LOGINFO==="
HARVEST_RUNNING=$(pgrep -f 'harvest\\.sh smithsonian' 2>/dev/null || true)
PIPELINE_RUNNING=$(pgrep -f 'ingest\\.sh smithsonian' 2>/dev/null || true)
if [ -n "$HARVEST_RUNNING" ]; then
  ACTIVE_LOG="{HARVEST_LOG}"
elif [ -n "$PIPELINE_RUNNING" ]; then
  ACTIVE_LOG="{PIPELINE_LOG}"
else
  ACTIVE_LOG=$(ls -t {HARVEST_LOG} {PIPELINE_LOG} 2>/dev/null | head -1)
fi
if [ -n "$ACTIVE_LOG" ] && [ -f "$ACTIVE_LOG" ]; then
  echo "log=$ACTIVE_LOG"
  echo "mtime=$(stat -c '%y' "$ACTIVE_LOG" 2>/dev/null | cut -d'.' -f1)"
fi
echo "ec2_now=$(date '+%H:%M:%S')"

echo "===STAGE_FIRST==="
HLOG="{HARVEST_LOG}"
PLOG="{PIPELINE_LOG}"
[ -f "$HLOG" ] && grep -m1 -E 'SiFileHarvester|Harvested' "$HLOG" 2>/dev/null | grep -oE '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' | head -1
echo "---"
[ -f "$PLOG" ] && grep -m1 -E 'MappingEntry|IngestRemap' "$PLOG" 2>/dev/null | grep -oE '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' | head -1
echo "---"
[ -f "$PLOG" ] && grep -m1 -E 'EnrichEntry' "$PLOG" 2>/dev/null | grep -oE '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' | head -1
echo "---"
[ -f "$PLOG" ] && grep -m1 -E 'JsonlEntry' "$PLOG" 2>/dev/null | grep -oE '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' | head -1
echo "---"

echo "===STAGE_RECENT==="
PLOG="{PIPELINE_LOG}"
[ -f "$PLOG" ] && tail -200 "$PLOG" 2>/dev/null | grep -E "{PIPELINE_STAGE_REGEX}" | tail -10

echo "===PROGRESS_LINES==="
HLOG="{HARVEST_LOG}"
[ -f "$HLOG" ] && tail -100 "$HLOG" 2>/dev/null | grep -E 'SiFileHarvester.*Harvested' | tail -5

echo "===STAGES_DONE==="
CUTOFF=$(date -d '48 hours ago' +%s)
for stage in harvest mapping enrichment jsonl; do
  STAGE_DIR="{DATA_ROOT}/$stage"
  LATEST=$(ls -1dt $STAGE_DIR/*/ 2>/dev/null | head -1 | sed 's:/$::')
  if [ -n "$LATEST" ] && [ -f "$LATEST/_SUCCESS" ]; then
    SUCCESS_EPOCH=$(stat -c '%Y' "$LATEST/_SUCCESS" 2>/dev/null || echo 0)
    if [ "$SUCCESS_EPOCH" -lt "$CUTOFF" ]; then continue; fi
    MTIME=$(stat -c '%y' "$LATEST/_SUCCESS" 2>/dev/null | cut -d'.' -f1)
    MANIFEST=""
    [ -f "$LATEST/_MANIFEST" ] && MANIFEST=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')
    echo "$stage|$MTIME|$MANIFEST"
  fi
done
""".strip()


# ---------- parsers ----------
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


def detect_current_stage(sections: dict, is_harvest_running: bool, is_pipeline_running: bool) -> str | None:
    if is_harvest_running:
        return "harvest"
    if is_pipeline_running:
        stage_recent = sections.get("STAGE_RECENT", "")
        for line in reversed(stage_recent.splitlines()):
            for stage, keywords in STAGE_KEYWORDS.items():
                if stage == "harvest":
                    continue
                for kw in keywords:
                    if kw in line:
                        return stage
    return None


def parse_first_stage_timestamps(stage_first: str) -> dict:
    timestamps = {}
    chunks = stage_first.split("---")
    ts_re = re.compile(r"^\d{2}:\d{2}:\d{2}$")
    for stage, chunk in zip(STAGES, chunks):
        ts = chunk.strip()
        if ts_re.match(ts):
            timestamps[stage] = ts
    return timestamps


def hms_to_seconds(hms: str) -> int:
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def fmt_duration(seconds) -> str:
    if seconds is None or seconds < 0:
        return "?"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"


def stage_runtime_seconds(stage_first_ts, now_ref):
    if not stage_first_ts or not now_ref:
        return None
    try:
        ref_dt = datetime.strptime(now_ref, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    try:
        stage_t = datetime.strptime(stage_first_ts, "%H:%M:%S").time()
    except ValueError:
        return None
    stage_dt = datetime.combine(ref_dt.date(), stage_t)
    delta = (ref_dt - stage_dt).total_seconds()
    if delta < 0:
        delta += 86400
    return int(delta)


def parse_process_lines(proc_text: str):
    rows = []
    for ln in proc_text.splitlines():
        ln = ln.strip()
        if not ln or ln == "(none)":
            continue
        m = re.match(r"^\s*(\d+)\s+(\S+)\s+(.*)$", ln)
        if not m:
            continue
        pid, etime, cmd = m.group(1), m.group(2), m.group(3)
        script_match = re.search(r"/([^/\s]+\.sh)(\s+.*)?$", cmd)
        if script_match:
            name = script_match.group(1)
            tail = (script_match.group(2) or "").strip()
            script = f"{name} {tail}".strip()
        else:
            script = cmd
        rows.append({"pid": pid, "etime": etime, "script": script})
    return rows


def parse_si_harvest_progress(progress_lines: str):
    """Parse 'SiFileHarvester - Harvested 73.2% (409,678 / 559,554)' lines."""
    best = None
    for line in progress_lines.splitlines():
        m = re.search(r"Harvested\s+([\d.]+)%\s+\((\d[\d,]*)\s*/\s*(\d[\d,]*)\)", line)
        if m:
            pct = float(m.group(1))
            current = int(m.group(2).replace(",", ""))
            total = int(m.group(3).replace(",", ""))
            best = (pct, current, total)
    return best  # returns (pct, current, total) or None


# ---------- color ----------
GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"
DIM = "\033[2m"; BOLD = "\033[1m"; RESET = "\033[0m"
USE_COLOR = sys.stdout.isatty()
def c(color, text): return f"{color}{text}{RESET}" if USE_COLOR else text


# ---------- render ----------
def render(sections: dict) -> str:
    lines = []
    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  Smithsonian ingest status   (instance {INSTANCE_ID})")
    lines.append(c(DIM, "=" * 70))

    # PROCESS
    proc_text = sections.get("PROCESS", "").strip()
    proc_rows = parse_process_lines(proc_text)
    is_harvest_running = any("harvest.sh" in r["script"] for r in proc_rows)
    is_pipeline_running = any("ingest.sh" in r["script"] for r in proc_rows)
    is_running = bool(proc_rows)

    lines.append("")
    lines.append("PROCESS")
    if not is_running:
        lines.append("  " + c(YELLOW, "(no smithsonian process running)"))
    else:
        primary = proc_rows[0]
        lines.append(f"  Script:  {c(GREEN, primary['script'])}")
        lines.append(f"  PID:     {primary['pid']}   (running for {primary['etime']})")
        if len(proc_rows) > 1:
            lines.append(c(DIM, f"  + {len(proc_rows) - 1} subprocess(es)"))

    # COMPLETED STAGES
    lines.append("")
    lines.append("COMPLETED STAGES (this run)")
    done_lines = [ln for ln in sections.get("STAGES_DONE", "").splitlines() if ln.strip()]
    if not done_lines:
        lines.append(c(DIM, "  (none yet this run)"))
    else:
        for ln in done_lines:
            parts = ln.split("|", 2)
            if len(parts) == 3:
                stage, mtime, manifest = parts
                manifest = re.sub(
                    r"(\d[\d,]*)",
                    lambda m: f"{int(m.group(1).replace(',', '')):,}",
                    manifest.strip(),
                    count=1,
                )
                lines.append(f"  {c(GREEN, stage):<22} done {mtime}   {manifest}")
            else:
                lines.append(f"  {ln}")

    # CURRENT STAGE
    stage_first_ts = parse_first_stage_timestamps(sections.get("STAGE_FIRST", ""))
    loginfo_lines = sections.get("LOGINFO", "").splitlines()
    log_mtime = next((ln.split("=", 1)[1] for ln in loginfo_lines if ln.startswith("mtime=")), None)
    ec2_now   = next((ln.split("=", 1)[1] for ln in loginfo_lines if ln.startswith("ec2_now=")), None)
    now_ref   = f"2000-01-01 {ec2_now}" if ec2_now else log_mtime

    lines.append("")
    lines.append("CURRENT STAGE")
    if not is_running:
        lines.append(c(DIM, "  (no process running — see SUMMARY)"))
    else:
        current_stage = detect_current_stage(sections, is_harvest_running, is_pipeline_running)
        if not current_stage:
            lines.append(c(YELLOW, "  Could not infer current stage from log."))
        else:
            stage_start = stage_first_ts.get(current_stage)
            runtime_s = stage_runtime_seconds(stage_start, now_ref)
            lines.append(f"  Stage:     {c(YELLOW, current_stage)}")
            if stage_start:
                lines.append(f"  Started:   {stage_start}   (running for {fmt_duration(runtime_s)})")
            else:
                lines.append("  Started:   (no stage-start marker found in log)")

            if current_stage == "harvest":
                progress = parse_si_harvest_progress(sections.get("PROGRESS_LINES", ""))
                if progress:
                    pct, current, total = progress
                    lines.append(
                        f"  Progress:  {current:,} / {total:,} files "
                        f"({c(BOLD, f'{pct:.1f}%')})"
                    )
                else:
                    lines.append("  Progress:  (no progress lines in recent log)")
            else:
                lines.append(c(DIM, "  Progress:  (Spark stage — not derivable from log)"))

    # SUMMARY
    lines.append("")
    lines.append("SUMMARY: " + derive_summary(is_running, is_harvest_running, is_pipeline_running, sections))
    lines.append("")
    return "\n".join(lines)


def derive_summary(is_running, is_harvest_running, is_pipeline_running, sections):
    done_count = len([ln for ln in sections.get("STAGES_DONE", "").splitlines() if ln.strip()])
    if is_running:
        if is_harvest_running:
            progress = parse_si_harvest_progress(sections.get("PROGRESS_LINES", ""))
            if progress:
                pct, current, total = progress
                return c(YELLOW, f"running — harvest at {pct:.1f}% ({current:,} / {total:,})")
            return c(YELLOW, "running — harvest in progress")
        if is_pipeline_running:
            stage_recent = sections.get("STAGE_RECENT", "")
            for line in reversed(stage_recent.splitlines()):
                for stage, keywords in STAGE_KEYWORDS.items():
                    if stage == "harvest":
                        continue
                    for kw in keywords:
                        if kw in line:
                            return c(YELLOW, f"running — currently in {stage}")
            return c(YELLOW, "running — pipeline in progress")
        return c(YELLOW, "running — stage unclear")

    if done_count == len(STAGES):
        return c(GREEN, "complete — all stages DONE")
    return c(RED, f"process is gone, only {done_count}/{len(STAGES)} stages complete in last 48h — likely failed or still launching")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Check on a Smithsonian ingest.")
    parser.add_argument(
        "--watch", nargs="?", const=30, type=int, default=None,
        help="Re-run every N seconds (default 30 if --watch is given without a value).",
    )
    args = parser.parse_args()

    script = build_status_script()

    def one_pass():
        try:
            out = ssm_run(script)
        except RuntimeError as e:
            print(f"\n[ERROR] {e}\n")
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
