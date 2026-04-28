#!/usr/bin/env python3
"""
Check on a running (or recently finished) DPLA hub ingest.

Reports the things that actually matter while you're waiting on a long ingest:
  - What kind of harvest this is (type + endpoint, looked up from i3.conf).
  - Whether ingest.sh is still alive, and its overall etime.
  - Which stages have already completed (with record counts from _MANIFEST).
  - Which stage is *currently running*, and:
      - when that stage started
      - how far through it we are (% complete, where derivable)
      - current pacing (records/sec)
      - ETA to end of the stage

Stage detection uses log content, not directory existence — Spark stages only
materialize their output dir when they finish, so the directory-existence
heuristic in the previous version reported "not started" for in-flight stages.

Usage:
    python3 run/check_ingest.py                  # prompts for hub
    python3 run/check_ingest.py bpl              # one-shot
    python3 run/check_ingest.py bpl --watch      # refresh every 30s
    python3 run/check_ingest.py bpl --watch 60   # custom interval

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
from datetime import datetime, timedelta

# ---------- config ----------
INSTANCE_ID = "i-0a0def8581efef783"
DATA_ROOT = "/home/ec2-user/data"
STAGES = ("harvest", "mapping", "enrichment", "jsonl")
HUB_RE = re.compile(r"^[a-z0-9_-]+$")
LOG_TAIL_LINES = 400  # how many recent log lines to pull back for analysis

CONF_PATH = os.environ.get("I3_CONF") or os.path.expanduser(
    "~/repos/ingestion3-conf/i3.conf"
)

# Stage indicators — first matching keyword in a log line wins.
# Ordered most-specific first (jsonl/enrichment before mapping before harvest)
# so "MappingEntry" doesn't accidentally claim a line that's about JsonlEntry.
STAGE_KEYWORDS = {
    "jsonl":      ["JsonlEntry", "jsonl complete", ":white_check_mark: jsonl"],
    "enrichment": ["EnrichEntry", "enrichment complete", ":white_check_mark: enrichment"],
    "mapping":    ["MappingEntry", "IngestRemap", "mapping complete", ":white_check_mark: mapping"],
    "harvest":    ["OaiMultiPageResponseBuilder", "HarvestEntry",
                   "OaiHarvester", "ApiHarvester", "FileHarvester",
                   "harvest complete", ":white_check_mark: harvest"],
}

# ---------- AWS / SSM helpers ----------
def aws(args):
    result = subprocess.run(["aws"] + args, capture_output=True, text=True)
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


# ---------- HOCON conf lookup (copied from prechecks.py) ----------
def lookup_hub_in_conf(hub, conf_path=CONF_PATH):
    if not os.path.exists(conf_path):
        return None, None
    with open(conf_path, "r", encoding="utf-8") as f:
        text = f.read()
    text = re.sub(r"(?m)^\s*(#|//).*$", "", text)
    text = re.sub(r"(?<!:)//[^\n]*", "", text)

    endpoint = None
    harvest_type = None

    block_match = re.search(rf"(?ms)^\s*{re.escape(hub)}\s*(?:=|:)?\s*\{{(.*)", text)
    if block_match:
        depth = 1
        start = block_match.start(1)
        i = start
        while i < len(text) and depth > 0:
            if text[i] == "{": depth += 1
            elif text[i] == "}": depth -= 1
            i += 1
        block = text[start:i - 1] if depth == 0 else text[start:]
        ep_match = re.search(r"""endpoint\s*[=:]\s*["']([^"']+)["']""", block)
        if ep_match: endpoint = ep_match.group(1)
        type_match = re.search(r"""type\s*[=:]\s*["']?([A-Za-z]+)["']?""", block)
        if type_match: harvest_type = type_match.group(1).lower()

    if endpoint is None:
        ep_match = re.search(rf"""{re.escape(hub)}\.harvest\.endpoint\s*[=:]\s*["']([^"']+)["']""", text)
        if ep_match: endpoint = ep_match.group(1)
    if harvest_type is None:
        type_match = re.search(rf"""{re.escape(hub)}\.harvest\.type\s*[=:]\s*["']?([A-Za-z]+)["']?""", text)
        if type_match: harvest_type = type_match.group(1).lower()
    return endpoint, harvest_type


# ---------- bash payload ----------
def build_status_script(hub: str) -> str:
    return f"""
HUB="{hub}"
LOG="{DATA_ROOT}/${{HUB}}-ingest.log"

echo "===PROCESS==="
PIDS=$(pgrep -f "ingest.sh ${{HUB}}" || true)
if [ -z "$PIDS" ]; then
  echo "(none)"
else
  # The main ingest.sh has the smallest etime-relative-to-the-others; just dump them all.
  for p in $PIDS; do
    ps -o pid=,etime=,cmd= -p $p
  done
fi

echo "===LOGINFO==="
if [ -f "$LOG" ]; then
  echo "size=$(stat -c '%s' "$LOG")"
  echo "mtime=$(stat -c '%y' "$LOG" | cut -d'.' -f1)"
  echo "lines=$(wc -l < "$LOG")"
else
  echo "(no log file)"
fi

echo "===STAGE_FIRST==="
# First-occurrence timestamps for each stage, used to compute stage runtime.
if [ -f "$LOG" ]; then
  for kw in "OaiMultiPageResponseBuilder|HarvestEntry|OaiHarvester|ApiHarvester|FileHarvester" \\
            "MappingEntry|IngestRemap" \\
            "EnrichEntry" \\
            "JsonlEntry"; do
    grep -m1 -E "$kw" "$LOG" 2>/dev/null | grep -oE '^[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' | head -1
    echo "---"
  done
fi

echo "===LOGTAIL==="
if [ -f "$LOG" ]; then
  tail -{LOG_TAIL_LINES} "$LOG"
fi

echo "===STAGES_DONE==="
for stage in {' '.join(STAGES)}; do
  STAGE_DIR="{DATA_ROOT}/${{HUB}}/${{stage}}"
  LATEST=$(ls -1dt ${{STAGE_DIR}}/*/ 2>/dev/null | head -1 | sed 's:/$::')
  if [ -n "$LATEST" ] && [ -f "$LATEST/_SUCCESS" ]; then
    MTIME=$(stat -c '%y' "$LATEST/_SUCCESS" 2>/dev/null | cut -d'.' -f1)
    MANIFEST=""
    if [ -f "$LATEST/_MANIFEST" ]; then
      MANIFEST=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')
    fi
    echo "${{stage}}|${{MTIME}}|${{MANIFEST}}"
  fi
done
""".strip()


# ---------- parsers / analyzers ----------
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


def detect_current_stage(log_tail: str) -> str | None:
    """Walk log lines from newest to oldest; first matching keyword wins."""
    for line in reversed(log_tail.splitlines()):
        for stage, keywords in STAGE_KEYWORDS.items():
            for kw in keywords:
                if kw in line:
                    return stage
    return None


def parse_first_stage_timestamps(stage_first: str) -> dict:
    """STAGE_FIRST section is 4 entries (harvest, mapping, enrichment, jsonl)
    each followed by '---'. Each entry may be empty if the stage hasn't started."""
    timestamps = {}
    chunks = stage_first.split("---")
    for stage, chunk in zip(STAGES, chunks):
        chunk = chunk.strip()
        if chunk and re.match(r"^\d{2}:\d{2}:\d{2}$", chunk):
            timestamps[stage] = chunk
    return timestamps


def extract_log_timestamp(line: str) -> str | None:
    m = re.match(r"^(\d{2}:\d{2}:\d{2})", line)
    return m.group(1) if m else None


def hms_to_seconds(hms: str) -> int:
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def fmt_duration(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return "?"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"


def stage_runtime_seconds(stage_first_ts: str, log_mtime: str) -> int | None:
    """Both are HH:MM:SS strings (stage_first_ts) and 'YYYY-MM-DD HH:MM:SS' (log_mtime).
    Compute (log_mtime - first_ts), handling day-rollover."""
    if not stage_first_ts or not log_mtime:
        return None
    try:
        # log_mtime: "2026-04-27 18:56:40"
        log_dt = datetime.strptime(log_mtime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    # stage_first_ts is just HH:MM:SS — assume same day as log_mtime.
    try:
        stage_t = datetime.strptime(stage_first_ts, "%H:%M:%S").time()
    except ValueError:
        return None
    stage_dt = datetime.combine(log_dt.date(), stage_t)
    delta = (log_dt - stage_dt).total_seconds()
    if delta < 0:
        # Stage started yesterday relative to log mtime.
        delta += 86400
    return int(delta)


# --- harvest progress + pacing (OAI-style resumption tokens) ---
TOKEN_RE = re.compile(r"t\((\d+)\):(\d+)")

def parse_harvest_progress(log_tail: str):
    """Return (current_offset, total_records) from the most recent OAI resumption token.
    Returns (None, None) if no token found (e.g., non-OAI harvest)."""
    for line in reversed(log_tail.splitlines()):
        m = TOKEN_RE.search(line)
        if m:
            return int(m.group(2)), int(m.group(1))
    return None, None


def parse_harvest_pacing(log_tail: str, n_points: int = 30):
    """Compute records/sec from the last N log lines that contain both a timestamp
    and a resumption token offset. Returns records-per-second (float) or None."""
    points = []
    for line in log_tail.splitlines():
        ts = extract_log_timestamp(line)
        m = TOKEN_RE.search(line)
        if ts and m:
            points.append((hms_to_seconds(ts), int(m.group(2))))
    if len(points) < 2:
        return None
    points = points[-n_points:]
    t0, o0 = points[0]
    t1, o1 = points[-1]
    dt = t1 - t0
    if dt < 0:
        dt += 86400  # day rollover
    if dt <= 0:
        return None
    return (o1 - o0) / dt


# ---------- color helpers ----------
GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"; DIM = "\033[2m"; BOLD = "\033[1m"; RESET = "\033[0m"
USE_COLOR = sys.stdout.isatty()
def c(color, text): return f"{color}{text}{RESET}" if USE_COLOR else text


# ---------- render ----------
def render(hub, harvest_type, endpoint, sections):
    lines = []
    lines.append("")
    lines.append(c(DIM, "=" * 70))
    lines.append(f"  Ingest status: {c(BOLD + GREEN, hub)}   (instance {INSTANCE_ID})")
    lines.append(c(DIM, "=" * 70))

    # CONFIG
    lines.append("")
    lines.append("CONFIG")
    lines.append(f"  Type:     {harvest_type or '(unknown — not in conf)'}")
    lines.append(f"  Endpoint: {endpoint or '(unknown — not in conf)'}")

    # PROCESS
    proc = sections.get("PROCESS", "").strip()
    is_running = proc and proc != "(none)"
    lines.append("")
    lines.append("PROCESS")
    if not is_running:
        lines.append("  " + c(YELLOW, "(no ingest.sh process running for this hub)"))
    else:
        for ln in proc.splitlines():
            lines.append("  " + c(GREEN, ln.strip()))

    # COMPLETED STAGES
    lines.append("")
    lines.append("COMPLETED STAGES")
    done_lines = [ln for ln in sections.get("STAGES_DONE", "").splitlines() if ln.strip()]
    if not done_lines:
        lines.append(c(DIM, "  (none yet)"))
    else:
        for ln in done_lines:
            parts = ln.split("|", 2)
            if len(parts) == 3:
                stage, mtime, manifest = parts
                lines.append(f"  {c(GREEN, stage):<22} done {mtime}   {manifest.strip()}")
            else:
                lines.append(f"  {ln}")

    # CURRENT STAGE — only meaningful if process is running.
    log_tail = sections.get("LOGTAIL", "")
    stage_first_ts = parse_first_stage_timestamps(sections.get("STAGE_FIRST", ""))
    log_mtime = next(
        (ln.split("=", 1)[1] for ln in sections.get("LOGINFO", "").splitlines() if ln.startswith("mtime=")),
        None,
    )

    lines.append("")
    lines.append("CURRENT STAGE")
    if not is_running:
        lines.append(c(DIM, "  (no process running — see SUMMARY)"))
    else:
        current_stage = detect_current_stage(log_tail)
        if not current_stage:
            lines.append(c(YELLOW, "  Could not infer current stage from log."))
        else:
            stage_start = stage_first_ts.get(current_stage)
            runtime_s = stage_runtime_seconds(stage_start, log_mtime)
            lines.append(f"  Stage:     {c(YELLOW, current_stage)}")
            if stage_start:
                lines.append(f"  Started:   {stage_start}   (running for {fmt_duration(runtime_s)})")
            else:
                lines.append(f"  Started:   (no clear stage-start marker in log)")

            # Progress + pacing — currently only computable for OAI harvests.
            if current_stage == "harvest":
                current, total = parse_harvest_progress(log_tail)
                pacing_rps = parse_harvest_pacing(log_tail)
                if current is not None and total:
                    pct = (current / total) * 100
                    lines.append(
                        f"  Progress:  {current:,} / {total:,} records "
                        f"({c(BOLD, f'{pct:.1f}%')})"
                    )
                else:
                    lines.append("  Progress:  (no resumption token in recent log — non-OAI or just started)")
                if pacing_rps:
                    rph = pacing_rps * 3600
                    lines.append(f"  Pacing:    ~{pacing_rps:.1f} records/sec  (~{rph:,.0f}/hour)")
                    if current is not None and total:
                        remaining = total - current
                        eta_s = remaining / pacing_rps if pacing_rps > 0 else None
                        lines.append(f"  ETA:       ~{fmt_duration(eta_s)} remaining for this stage")
                else:
                    lines.append("  Pacing:    (insufficient log data to compute)")
            else:
                # mapping / enrichment / jsonl: Spark progress isn't easily parsed from
                # the high-level log. We could grep for "X tasks completed" but it's noisy.
                lines.append(c(DIM, "  Progress:  (not derivable for Spark stages from the log alone)"))

    # SUMMARY
    lines.append("")
    lines.append("SUMMARY: " + derive_summary(is_running, sections, log_tail))
    lines.append("")
    return "\n".join(lines)


def derive_summary(is_running, sections, log_tail):
    done_count = len([ln for ln in sections.get("STAGES_DONE", "").splitlines() if ln.strip()])
    if is_running:
        stage = detect_current_stage(log_tail)
        if stage == "harvest":
            current, total = parse_harvest_progress(log_tail)
            pacing = parse_harvest_pacing(log_tail)
            if current and total and pacing:
                pct = (current / total) * 100
                eta = fmt_duration((total - current) / pacing)
                return c(YELLOW, f"running — harvest at {pct:.1f}%, ETA ~{eta}")
            return c(YELLOW, f"running — currently in {stage}")
        if stage:
            return c(YELLOW, f"running — currently in {stage}")
        return c(YELLOW, "running — stage unclear")

    # Not running.
    if done_count == len(STAGES):
        return c(GREEN, "complete — all stages DONE")
    return c(RED, f"process is gone, only {done_count}/{len(STAGES)} stages complete — likely failed")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Check on a DPLA hub ingest.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. bpl). Prompts if omitted.")
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

    harvest_type, endpoint = None, None
    looked_up_endpoint, looked_up_type = lookup_hub_in_conf(hub)
    if looked_up_endpoint or looked_up_type:
        endpoint, harvest_type = looked_up_endpoint, looked_up_type

    script = build_status_script(hub)

    def one_pass():
        try:
            out = ssm_run(script)
        except RuntimeError as e:
            print(f"\n[ERROR] {e}\n")
            return
        sections = parse_sections(out)
        print(render(hub, harvest_type, endpoint, sections))

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