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

Handles multiple harvest types:
  - oai            — OAI-PMH with offset encoded in resumption token
                     (e.g. BPL via Digital Commonwealth: ...t(1485939):838750)
  - localoai       — OAI-PMH with offset as separate fields in OaiRequestInfo
                     (e.g. p2p: ...Some(502000),Some(1211330)))
  - api            — REST API harvests (progress not derivable from log)
  - file / nara    — file-based harvests (progress not meaningful)

Usage:
    python3 check_ingest.py                  # prompts for hub
    python3 check_ingest.py p2p              # one-shot
    python3 check_ingest.py p2p --watch      # refresh every 30s
    python3 check_ingest.py p2p --watch 60   # custom interval
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
INSTANCE_ID = "i-0a0def8581efef783"
DATA_ROOT = "/home/ec2-user/data"
STAGES = ("harvest", "mapping", "enrichment", "jsonl")
HUB_RE = re.compile(r"^[a-z0-9_-]+$")

CONF_PATH = os.environ.get("I3_CONF") or os.path.expanduser(
    "~/Documents/Repos/ingestion3-conf/i3.conf"
)

# Stage indicators — first matching keyword wins. Ordered most-specific first
# (jsonl/enrichment before mapping before harvest) so e.g. "MappingEntry"
# doesn't accidentally claim a line that's about JsonlEntry.
STAGE_KEYWORDS = {
    "jsonl":      ["JsonlEntry", "jsonl complete", ":white_check_mark: jsonl"],
    "enrichment": ["EnrichEntry", "enrichment complete", ":white_check_mark: enrichment"],
    "mapping":    ["MappingEntry", "IngestRemap", "mapping complete", ":white_check_mark: mapping"],
    "harvest":    ["OaiMultiPageResponseBuilder", "OaiRequestInfo", "HarvestEntry",
                   "OaiHarvester", "ApiHarvester", "FileHarvester",
                   "harvest complete", ":white_check_mark: harvest"],
}
# Flat regex used by bash grep — covers all stages.
ALL_STAGE_REGEX = (
    "OaiMultiPageResponseBuilder|OaiRequestInfo|HarvestEntry|"
    "OaiHarvester|ApiHarvester|FileHarvester|"
    "MappingEntry|IngestRemap|EnrichEntry|JsonlEntry|"
    "harvest complete|mapping complete|enrichment complete|jsonl complete"
)

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


# ---------- HOCON conf lookup ----------
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
        type_match = re.search(r"""type\s*[=:]\s*["']?([A-Za-z._]+)["']?""", block)
        if type_match: harvest_type = type_match.group(1).lower()

    if endpoint is None:
        ep_match = re.search(rf"""{re.escape(hub)}\.harvest\.endpoint\s*[=:]\s*["']([^"']+)["']""", text)
        if ep_match: endpoint = ep_match.group(1)
    if harvest_type is None:
        type_match = re.search(rf"""{re.escape(hub)}\.harvest\.type\s*[=:]\s*["']?([A-Za-z._]+)["']?""", text)
        if type_match: harvest_type = type_match.group(1).lower()
    return endpoint, harvest_type


# ---------- bash payload ----------
def build_status_script(hub: str) -> str:
    """Compact bash payload — sends only the targeted lines we need.
    Designed to stay well under SSM's 24KB output limit."""
    return f"""
HUB="{hub}"
LOG="{DATA_ROOT}/${{HUB}}-ingest.log"

echo "===PROCESS==="
PIDS=$(pgrep -f "ingest\\.sh ${{HUB}}|${{HUB}}-ingest\\.sh|${{HUB}}.*ingest" || true)
if [ -z "$PIDS" ]; then
  echo "(none)"
else
  for p in $PIDS; do ps -o pid=,etime=,cmd= -p $p; done
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
# First matching log line for each stage (whole line — Python extracts timestamp).
if [ -f "$LOG" ]; then
  for kw in "OaiMultiPageResponseBuilder|OaiRequestInfo|HarvestEntry|OaiHarvester|ApiHarvester|FileHarvester|harvest started|:arrow_forward: .* harvest" \\
            "MappingEntry|IngestRemap|mapping started|:arrow_forward: .* mapping" \\
            "EnrichEntry|enrichment started|:arrow_forward: .* enrichment" \\
            "JsonlEntry|jsonl started|:arrow_forward: .* jsonl"; do
    grep -m1 -E "$kw" "$LOG" 2>/dev/null
    echo "---ENDFIRSTSTAGE---"
  done
fi

echo "===STAGE_RECENT==="
# Last 10 lines containing any stage indicator — used to detect current stage.
if [ -f "$LOG" ]; then
  tail -200 "$LOG" 2>/dev/null | grep -E "{ALL_STAGE_REGEX}" | tail -10
fi

echo "===PROGRESS_LINES==="
# Last 30 OAI request lines — used for progress + pacing parsing.
if [ -f "$LOG" ]; then
  tail -300 "$LOG" 2>/dev/null | grep -E "OaiRequestInfo|Loading page" | tail -30
fi

echo "===STAGES_DONE==="
# Determine current run start epoch so we can filter out output dirs from
# previous runs. Strategy (in priority order):
#   1. ps -o lstart on the running ingest process — exact and reliable.
#   2. Earliest YYYYMMDD_HHMMSS dir name across all stage output dirs — works
#      after the process exits (e.g. run just finished, checking results).
RUN_START_EPOCH=0
INGEST_PID=$(pgrep -f "ingest\\.sh ${{HUB}}" | head -1 || true)
if [ -n "$INGEST_PID" ]; then
  LSTART=$(ps -o lstart= -p "$INGEST_PID" 2>/dev/null | xargs || true)
  if [ -n "$LSTART" ]; then
    RUN_START_EPOCH=$(date -d "$LSTART" +%s 2>/dev/null || echo 0)
  fi
fi
if [ "$RUN_START_EPOCH" -eq 0 ]; then
  # Process is gone — find the earliest completed stage dir from this run by
  # looking at the most recent dir across all stages and taking the minimum.
  for stage in {' '.join(STAGES)}; do
    LATEST=$(ls -1dt {DATA_ROOT}/${{HUB}}/${{stage}}/*/ 2>/dev/null | head -1 | sed 's:/$::')
    if [ -n "$LATEST" ] && [ -f "$LATEST/_SUCCESS" ]; then
      DNAME=$(basename "$LATEST")
      DTS=$(echo "$DNAME" | grep -oE '^[0-9]{{8}}_[0-9]{{6}}' | head -1)
      if [ -n "$DTS" ]; then
        PARSED=$(date -d "${{DTS:0:8}} ${{DTS:9:2}}:${{DTS:11:2}}:${{DTS:13:2}}" +%s 2>/dev/null || echo 0)
        if [ "$PARSED" -gt 0 ] && ([ "$RUN_START_EPOCH" -eq 0 ] || [ "$PARSED" -lt "$RUN_START_EPOCH" ]); then
          RUN_START_EPOCH=$PARSED
        fi
      fi
    fi
  done
fi
for stage in {' '.join(STAGES)}; do
  STAGE_DIR="{DATA_ROOT}/${{HUB}}/${{stage}}"
  LATEST=$(ls -1dt ${{STAGE_DIR}}/*/ 2>/dev/null | head -1 | sed 's:/$::')
  if [ -n "$LATEST" ] && [ -f "$LATEST/_SUCCESS" ]; then
    SUCCESS_EPOCH=$(stat -c '%Y' "$LATEST/_SUCCESS" 2>/dev/null || echo 0)
    # Skip if this _SUCCESS predates the current run's log start.
    if [ "$RUN_START_EPOCH" -gt 0 ] && [ "$SUCCESS_EPOCH" -lt "$RUN_START_EPOCH" ]; then continue; fi
    MTIME=$(stat -c '%y' "$LATEST/_SUCCESS" 2>/dev/null | cut -d'.' -f1)
    MANIFEST=""
    if [ -f "$LATEST/_MANIFEST" ]; then
      MANIFEST=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')
    fi
    echo "${{stage}}|${{MTIME}}|${{MANIFEST}}"
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


def detect_current_stage(stage_recent: str) -> str | None:
    """Walk the recent stage-indicator lines from newest to oldest;
    first matching keyword wins."""
    for line in reversed(stage_recent.splitlines()):
        for stage, keywords in STAGE_KEYWORDS.items():
            for kw in keywords:
                if kw in line:
                    return stage
    return None


def parse_first_stage_timestamps(stage_first: str) -> dict:
    """STAGE_FIRST has 4 entries (harvest, mapping, enrichment, jsonl)
    separated by '---ENDFIRSTSTAGE---'. Each entry is a full log line;
    we extract the first HH:MM:SS we find anywhere on that line."""
    timestamps = {}
    chunks = stage_first.split("---ENDFIRSTSTAGE---")
    ts_re = re.compile(r"\b(\d{2}:\d{2}:\d{2})\b")
    for stage, chunk in zip(STAGES, chunks):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = ts_re.search(chunk)
        if m:
            timestamps[stage] = m.group(1)
    return timestamps


def extract_log_timestamp(line: str) -> str | None:
    m = re.search(r"\b(\d{2}:\d{2}:\d{2})\b", line)
    return m.group(1) if m else None


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


def stage_runtime_seconds(stage_first_ts, log_mtime):
    if not stage_first_ts or not log_mtime:
        return None
    try:
        log_dt = datetime.strptime(log_mtime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    try:
        stage_t = datetime.strptime(stage_first_ts, "%H:%M:%S").time()
    except ValueError:
        return None
    stage_dt = datetime.combine(log_dt.date(), stage_t)
    delta = (log_dt - stage_dt).total_seconds()
    if delta < 0:
        delta += 86400
    return int(delta)


# ---- progress + pacing — handles multiple OAI log shapes ----
PROGRESS_TRAILING_PAIR_RE = re.compile(r"Some\((\d+)\)\s*,\s*Some\((\d+)\)\s*\)\s*$")
PROGRESS_TOKEN_RE = re.compile(r"t\((\d+)\):(\d+)")


def parse_oai_progress_from_line(line: str):
    """Return (current, total) from a single OaiRequestInfo line."""
    m = PROGRESS_TRAILING_PAIR_RE.search(line)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = PROGRESS_TOKEN_RE.search(line)
    if m:
        return int(m.group(2)), int(m.group(1))
    return None, None


def parse_harvest_progress(progress_lines: str):
    """Return (current, total) from the most recent line that has progress info."""
    for line in reversed(progress_lines.splitlines()):
        c, t = parse_oai_progress_from_line(line)
        if c is not None and t is not None:
            return c, t
    return None, None


def parse_harvest_pacing(progress_lines: str, n_points: int = 30):
    """Compute records/sec from progression of offsets across recent lines."""
    points = []
    for line in progress_lines.splitlines():
        ts = extract_log_timestamp(line)
        c, _ = parse_oai_progress_from_line(line)
        if ts and c is not None:
            points.append((hms_to_seconds(ts), c))
    if len(points) < 2:
        return None
    points = points[-n_points:]
    t0, o0 = points[0]
    t1, o1 = points[-1]
    dt = t1 - t0
    if dt < 0:
        dt += 86400
    if dt <= 0:
        return None
    return (o1 - o0) / dt


def earliest_timestamp_in_lines(lines: str):
    earliest = None
    earliest_secs = None
    for line in lines.splitlines():
        ts = extract_log_timestamp(line)
        if ts:
            secs = hms_to_seconds(ts)
            if earliest_secs is None or secs < earliest_secs:
                earliest = ts
                earliest_secs = secs
    return earliest


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
        rows.append({"pid": pid, "etime": etime, "cmd": cmd, "script": script})
    return rows


# ---------- color ----------
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
        lines.append("  " + c(YELLOW, "(no ingest process running for this hub)"))
    else:
        rows = parse_process_lines(proc)
        if rows:
            primary = rows[0]
            lines.append(f"  Script:  {c(GREEN, primary['script'])}")
            lines.append(f"  PID:     {primary['pid']}   (running for {primary['etime']})")
            if len(rows) > 1:
                lines.append(c(DIM, f"  + {len(rows) - 1} subprocess(es)"))
        else:
            for ln in proc.splitlines():
                lines.append("  " + c(GREEN, ln.strip()))

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
                lines.append(f"  {c(GREEN, stage):<22} done {mtime}   {manifest.strip()}")
            else:
                lines.append(f"  {ln}")

    # CURRENT STAGE
    progress_lines = sections.get("PROGRESS_LINES", "")
    stage_recent = sections.get("STAGE_RECENT", "")
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
        current_stage = detect_current_stage(stage_recent)
        if not current_stage:
            lines.append(c(YELLOW, "  Could not infer current stage from log."))
        else:
            stage_start = stage_first_ts.get(current_stage)
            stage_start_was_inferred = False
            if not stage_start:
                stage_start = earliest_timestamp_in_lines(progress_lines)
                if stage_start:
                    stage_start_was_inferred = True

            runtime_s = stage_runtime_seconds(stage_start, log_mtime)
            lines.append(f"  Stage:     {c(YELLOW, current_stage)}")
            if stage_start:
                suffix = " (approx — based on recent log window)" if stage_start_was_inferred else ""
                lines.append(f"  Started:   {stage_start}   (running for {fmt_duration(runtime_s)}){suffix}")
            else:
                lines.append("  Started:   (no clear stage-start marker in log)")

            if current_stage == "harvest":
                ht = (harvest_type or "").lower()
                if ht in ("oai", "localoai") or ht == "" or "oai" in ht:
                    current, total = parse_harvest_progress(progress_lines)
                    pacing_rps = parse_harvest_pacing(progress_lines)
                    if current is not None and total:
                        pct = (current / total) * 100
                        lines.append(
                            f"  Progress:  {current:,} / {total:,} records "
                            f"({c(BOLD, f'{pct:.1f}%')})"
                        )
                    else:
                        lines.append("  Progress:  (no offset/total in recent log lines)")
                    if pacing_rps:
                        rph = pacing_rps * 3600
                        lines.append(f"  Pacing:    ~{pacing_rps:.1f} records/sec  (~{rph:,.0f}/hour)")
                        if current is not None and total:
                            remaining = total - current
                            eta_s = remaining / pacing_rps if pacing_rps > 0 else None
                            lines.append(f"  ETA:       ~{fmt_duration(eta_s)} remaining for this stage")
                    else:
                        lines.append("  Pacing:    (insufficient log data to compute)")
                elif ht == "api":
                    lines.append(c(DIM, "  Progress:  (API harvest — progress not derivable from log)"))
                elif ht.startswith("file") or "nara" in ht:
                    lines.append(c(DIM, "  Progress:  (file-based harvest — progress not meaningful)"))
                else:
                    lines.append(c(DIM, f"  Progress:  (harvest type '{ht}' — no progress parser)"))
            else:
                lines.append(c(DIM, "  Progress:  (not derivable for Spark stages from the log alone)"))

    # SUMMARY
    lines.append("")
    lines.append("SUMMARY: " + derive_summary(is_running, sections, harvest_type))
    lines.append("")
    return "\n".join(lines)


def derive_summary(is_running, sections, harvest_type):
    done_count = len([ln for ln in sections.get("STAGES_DONE", "").splitlines() if ln.strip()])
    progress_lines = sections.get("PROGRESS_LINES", "")
    stage_recent = sections.get("STAGE_RECENT", "")

    if is_running:
        stage = detect_current_stage(stage_recent)
        if stage == "harvest":
            ht = (harvest_type or "").lower()
            if ht in ("oai", "localoai") or "oai" in ht or ht == "":
                current, total = parse_harvest_progress(progress_lines)
                pacing = parse_harvest_pacing(progress_lines)
                if current and total and pacing:
                    pct = (current / total) * 100
                    eta = fmt_duration((total - current) / pacing)
                    return c(YELLOW, f"running — harvest at {pct:.1f}%, ETA ~{eta}")
            return c(YELLOW, f"running — currently in {stage}")
        if stage:
            return c(YELLOW, f"running — currently in {stage}")
        return c(YELLOW, "running — stage unclear")

    if done_count == len(STAGES):
        return c(GREEN, "complete — all stages DONE today")
    return c(RED, f"process is gone, only {done_count}/{len(STAGES)} stages complete today — likely failed")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Check on a DPLA hub ingest.")
    parser.add_argument("hub", nargs="?", help="Hub name (e.g. p2p). Prompts if omitted.")
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

    endpoint, harvest_type = lookup_hub_in_conf(hub)
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