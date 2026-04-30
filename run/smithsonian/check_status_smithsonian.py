#!/usr/bin/env python3
"""
Smithsonian ingest status — works regardless of which stage is currently
running. Walks the full Smithsonian workflow and reports the state of each:

    Stage 1: Download from s3://dpla-hub-si/<DATE>/
    Stage 2: Preprocessing (fix-si.sh — gunzip recompress + xmll split)
    Stage 3: Conf endpoint matches the latest delivery
    Stage 4: Harvest (harvest.sh smithsonian)
    Stage 5: Pipeline (mapping → enrichment → jsonl via ingest.sh --skip-harvest)
    Stage 6: S3 sync (jsonl in s3://dpla-master-dataset/smithsonian/jsonl/)

For each stage, prints one of:
    [DONE] / [RUNNING] / [PENDING] / [SKIPPED]

Plus a "current stage" summary with relevant log tail and a "next action"
suggestion so you know what to do next.

Usage:
    python3 check_status_smithsonian.py            # auto-detect latest delivery
    python3 check_status_smithsonian.py --date 2026-04-03   # specific delivery
    python3 check_status_smithsonian.py --watch    # refresh every 60s
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
AWS_PROFILE = os.environ.get("AWS_PROFILE", "dpla")

S3_DELIVERY_BUCKET = "dpla-hub-si"
S3_OUTPUT_BUCKET = "dpla-master-dataset"
HUB_S3_NAME = "smithsonian"

DATA_ROOT = "/home/ec2-user/data/smithsonian"
HARVEST_LOG = "/home/ec2-user/data/si-harvest.log"
PIPELINE_LOG = "/home/ec2-user/data/smithsonian-ingest.log"
CONF_PATH_BOX = "/home/ec2-user/ingestion3-conf/i3.conf"

S3_DATE_RE = re.compile(r"PRE\s+(\d{4}-\d{2}-\d{2})/")
S3_FOLDER_DATE_RE = re.compile(r"PRE\s+(\d{8})[-_]")


# ---------- AWS / SSM ----------
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
            raise RuntimeError(f"SSM timed out after {timeout_seconds}s")
    output = aws([
        "ssm", "get-command-invocation",
        "--command-id", cmd_id,
        "--instance-id", INSTANCE_ID,
        "--query", "StandardOutputContent",
        "--output", "text",
    ])
    if status != "Success":
        raise RuntimeError(f"SSM ended with status {status}.\nSTDOUT:\n{output}")
    return output


# ---------- helpers ----------
def aws_s3_ls(s3_path):
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", s3_path, "--profile", AWS_PROFILE],
            capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    return result.stdout if result.returncode == 0 else ""


def find_latest_ia_delivery():
    out = aws_s3_ls(f"s3://{S3_DELIVERY_BUCKET}/")
    dates = [m.group(1) for m in (S3_DATE_RE.search(line) for line in out.splitlines()) if m]
    return sorted(dates)[-1] if dates else None


def s3_jsonl_folders():
    out = aws_s3_ls(f"s3://{S3_OUTPUT_BUCKET}/{HUB_S3_NAME}/jsonl/")
    return [m.group(1) for line in out.splitlines() for m in [S3_FOLDER_DATE_RE.search(line)] if m]


def parse_kv(text):
    """Parse simple key=value lines into a dict."""
    out = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def parse_sections(out):
    sections = {}
    current = None
    buf = []
    for line in out.splitlines():
        m = re.match(r"^===(\w+)===$", line.strip())
        if m:
            if current:
                sections[current] = "\n".join(buf).rstrip()
            current = m.group(1)
            buf = []
        else:
            buf.append(line)
    if current:
        sections[current] = "\n".join(buf).rstrip()
    return sections


# ---------- bash payload ----------
def build_status_script(date):
    """Single SSM call that gathers state for all six stages."""
    download_dir = f"{DATA_ROOT}/originalRecords/{date}"
    return f"""
echo "===DOWNLOAD==="
DIR="{download_dir}"
if [ -d "$DIR" ]; then
  echo "exists=yes"
  echo "xml_gz_count=$(find "$DIR" -maxdepth 1 -name '*.xml.gz' -type f 2>/dev/null | wc -l)"
  echo "size=$(du -sh "$DIR" 2>/dev/null | cut -f1)"
  echo "mtime=$(stat -c '%y' "$DIR" 2>/dev/null | cut -d'.' -f1)"
else
  echo "exists=no"
fi

echo "===PREPROCESS==="
if [ -d "$DIR/original_backup" ]; then
  echo "preprocessed=yes"
  echo "backup_count=$(find "$DIR/original_backup" -maxdepth 1 -name '*.xml.gz' -type f 2>/dev/null | wc -l)"
  echo "preprocess_mtime=$(stat -c '%y' "$DIR/original_backup" 2>/dev/null | cut -d'.' -f1)"
else
  echo "preprocessed=no"
fi

echo "===CONF==="
grep '^smithsonian\\.harvest\\.endpoint' {CONF_PATH_BOX} 2>/dev/null || echo "(no smithsonian endpoint in conf)"

echo "===PROCESSES==="
pgrep -af 'fix-si\\.sh|harvest\\.sh smithsonian|ingest\\.sh smithsonian|s3 sync.*dpla-hub-si' || echo "(no smithsonian processes)"

echo "===HARVEST==="
LATEST=$(ls -1dt {DATA_ROOT}/harvest/*/ 2>/dev/null | head -1 | sed 's:/$::')
if [ -n "$LATEST" ]; then
  echo "path=$LATEST"
  echo "mtime=$(stat -c '%y' "$LATEST" 2>/dev/null | cut -d'.' -f1)"
  echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"
  [ -f "$LATEST/_MANIFEST" ] && echo "manifest=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')"
else
  echo "exists=no"
fi

echo "===MAPPING==="
LATEST=$(ls -1dt {DATA_ROOT}/mapping/*/ 2>/dev/null | head -1 | sed 's:/$::')
if [ -n "$LATEST" ]; then
  echo "path=$LATEST"
  echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"
  echo "mtime=$(stat -c '%y' "$LATEST" 2>/dev/null | cut -d'.' -f1)"
else
  echo "exists=no"
fi

echo "===ENRICHMENT==="
LATEST=$(ls -1dt {DATA_ROOT}/enrichment/*/ 2>/dev/null | head -1 | sed 's:/$::')
if [ -n "$LATEST" ]; then
  echo "path=$LATEST"
  echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"
  echo "mtime=$(stat -c '%y' "$LATEST" 2>/dev/null | cut -d'.' -f1)"
else
  echo "exists=no"
fi

echo "===JSONL==="
LATEST=$(ls -1dt {DATA_ROOT}/jsonl/*/ 2>/dev/null | head -1 | sed 's:/$::')
if [ -n "$LATEST" ]; then
  echo "path=$LATEST"
  echo "success=$([ -f "$LATEST/_SUCCESS" ] && echo yes || echo no)"
  echo "mtime=$(stat -c '%y' "$LATEST" 2>/dev/null | cut -d'.' -f1)"
  [ -f "$LATEST/_MANIFEST" ] && echo "manifest=$(grep -i 'record count' "$LATEST/_MANIFEST" 2>/dev/null | head -1 | tr -d '\\n')"
else
  echo "exists=no"
fi

echo "===HARVEST_LOG==="
[ -f {HARVEST_LOG} ] && tail -15 {HARVEST_LOG} 2>/dev/null || echo "(no harvest log)"

echo "===PIPELINE_LOG==="
[ -f {PIPELINE_LOG} ] && tail -15 {PIPELINE_LOG} 2>/dev/null || echo "(no pipeline log)"
""".strip()


# ---------- stage analysis ----------
DONE = "[DONE]"
RUNNING = "[RUNNING]"
PENDING = "[PENDING]"
WARN = "[WARN]"
STALE = "[STALE]"   # output exists but is from a previous run, not the current one


def parse_dir_timestamp(path):
    """Extract YYYYMMDDHHMMSS from a Spark output dir name.
    Dir names look like '20260429_191232-smithsonian-OriginalRecord.avro'.
    Returns int(YYYYMMDDHHMMSS) for easy comparison, or None."""
    if not path:
        return None
    name = os.path.basename(path)
    m = re.match(r"^(\d{8})_(\d{6})", name)
    return int(m.group(1) + m.group(2)) if m else None


def parse_record_count(manifest_str):
    """Pull the integer out of a manifest string like 'Record count: 7832993'."""
    if not manifest_str:
        return None
    m = re.search(r"(\d[\d,]*)", manifest_str)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


# Smithsonian historically lands in the millions of records. Anything below
# this is almost certainly a partial/failed harvest, not a real one.
SMITHSONIAN_MIN_PLAUSIBLE_RECORDS = 100_000


def analyze(date, sections):
    """Return a list of {stage, status, detail, current} dicts."""
    download = parse_kv(sections.get("DOWNLOAD", ""))
    preprocess = parse_kv(sections.get("PREPROCESS", ""))
    conf = sections.get("CONF", "").strip()
    procs = sections.get("PROCESSES", "").strip()
    harvest = parse_kv(sections.get("HARVEST", ""))
    mapping = parse_kv(sections.get("MAPPING", ""))
    enrichment = parse_kv(sections.get("ENRICHMENT", ""))
    jsonl = parse_kv(sections.get("JSONL", ""))

    # Detect which processes are currently running.
    running_download = "s3 sync" in procs and "dpla-hub-si" in procs
    running_preprocess = "fix-si.sh" in procs
    running_harvest = "harvest.sh smithsonian" in procs
    running_pipeline = "ingest.sh smithsonian" in procs

    stages = []

    # Stage 1: Download
    if running_download:
        s = RUNNING
        d = "aws s3 sync still in flight"
    elif download.get("exists") == "yes":
        s = DONE
        d = f"{download.get('xml_gz_count', '?')} files, {download.get('size', '?')} (mtime {download.get('mtime', '?')})"
    else:
        s = PENDING
        d = f"no folder at {DATA_ROOT}/originalRecords/{date}"
    stages.append({"name": f"Stage 1/6: Download from s3://dpla-hub-si/{date}/", "status": s, "detail": d})

    # Stage 2: Preprocess
    if running_preprocess:
        s = RUNNING
        d = "fix-si.sh in flight (NMNHBOTANY can take ~6 min)"
    elif preprocess.get("preprocessed") == "yes":
        s = DONE
        d = f"original_backup/ has {preprocess.get('backup_count', '?')} files (mtime {preprocess.get('preprocess_mtime', '?')})"
    elif download.get("exists") == "yes":
        s = PENDING
        d = "download done, fix-si.sh not yet run"
    else:
        s = PENDING
        d = "blocked on Stage 1"
    stages.append({"name": "Stage 2/6: Preprocessing (fix-si.sh)", "status": s, "detail": d})

    # Stage 3: Conf endpoint
    expected_path_substring = f"/{date}/"
    if expected_path_substring in conf:
        s = DONE
        d = conf
    elif conf and "smithsonian" in conf:
        s = WARN
        d = f"conf endpoint doesn't reference {date}: {conf}"
    else:
        s = PENDING
        d = "conf endpoint not detected"
    stages.append({"name": f"Stage 3/6: Conf endpoint references {date}", "status": s, "detail": d})

    # Stage 4: Harvest
    harvest_ts = parse_dir_timestamp(harvest.get("path", ""))
    harvest_records = parse_record_count(harvest.get("manifest", ""))
    if running_harvest:
        s = RUNNING
        d = "harvest.sh smithsonian in flight"
    elif harvest.get("success") == "yes":
        if harvest_records is not None and harvest_records < SMITHSONIAN_MIN_PLAUSIBLE_RECORDS:
            s = WARN
            d = (f"_SUCCESS present but only {harvest_records:,} records — likely a stale or "
                 f"failed run; expected millions. Path: {harvest.get('path', '?')}")
        else:
            s = DONE
            d = f"{harvest.get('path', '?')} (mtime {harvest.get('mtime', '?')})"
            if "manifest" in harvest:
                d += f" — {harvest['manifest']}"
    elif harvest.get("exists") != "no":
        s = WARN
        d = f"output dir exists but no _SUCCESS: {harvest.get('path', '?')}"
    else:
        s = PENDING
        d = "no harvest output yet"
    stages.append({"name": "Stage 4/6: Harvest", "status": s, "detail": d})

    # Stage 5: Pipeline (mapping + enrichment + jsonl)
    # Cross-reference timestamps with the harvest run — any sub-stage output
    # whose timestamp predates the current harvest is from a previous ingest
    # and should NOT be counted as done for this delivery.
    def stage_belongs_to_current_run(stage_dict):
        """True if this stage's output is from the current harvest run (or
        later). False if it's older (a previous run's output)."""
        if stage_dict.get("success") != "yes":
            return False
        if harvest_ts is None:
            # No harvest yet — there's no "current run" to belong to.
            return False
        ts = parse_dir_timestamp(stage_dict.get("path", ""))
        return ts is not None and ts >= harvest_ts

    sub_done_current = sum(
        1 for x in (mapping, enrichment, jsonl) if stage_belongs_to_current_run(x)
    )
    sub_done_total = sum(1 for x in (mapping, enrichment, jsonl) if x.get("success") == "yes")
    sub_stale = sub_done_total - sub_done_current

    if running_pipeline:
        s = RUNNING
        d = f"ingest.sh smithsonian --skip-harvest in flight ({sub_done_current}/3 substages from current run)"
    elif sub_done_current == 3:
        s = DONE
        records = jsonl.get("manifest", "")
        d = f"all 3 substages complete{' — ' + records if records else ''}"
    elif sub_done_current > 0:
        s = WARN
        d = f"only {sub_done_current}/3 substages from current run done"
    elif sub_stale > 0:
        # All "done" sub-stages are from a previous run — for THIS delivery,
        # the pipeline hasn't started yet.
        s = STALE
        d = (f"{sub_stale} sub-stage(s) have _SUCCESS markers but they're from a "
             f"previous run (predate the current harvest). Pipeline NOT yet done for {date}.")
    elif harvest.get("success") == "yes":
        s = PENDING
        d = "harvest complete, pipeline not yet run"
    else:
        s = PENDING
        d = "blocked on Stage 4"
    stages.append({"name": "Stage 5/6: Pipeline (mapping/enrichment/jsonl)", "status": s, "detail": d})

    # Stage 6: S3 sync — must be from current harvest's date or later, not
    # just any folder dated within the same calendar month.
    harvest_date_str = str(harvest_ts)[:8] if harvest_ts else None  # e.g. "20260429"
    s3_folders = s3_jsonl_folders()
    target_yyyymm = date.replace("-", "")[:6]
    if harvest_date_str:
        matching_current = [f for f in s3_folders if f >= harvest_date_str]
        matching_month = [f for f in s3_folders if f.startswith(target_yyyymm)]
    else:
        matching_current = []
        matching_month = [f for f in s3_folders if f.startswith(target_yyyymm)]

    if matching_current:
        s = DONE
        d = f"jsonl folder in S3 dated {sorted(matching_current)[-1]} (>= harvest {harvest_date_str})"
    elif matching_month and stage_belongs_to_current_run(jsonl):
        # Local jsonl is current and matches this month — assume the S3 folder
        # we see is for the current run even if its timestamp is earlier.
        s = DONE
        d = f"jsonl folder in S3 for {target_yyyymm} (date {sorted(matching_month)[-1]})"
    elif matching_month:
        s = STALE
        d = (f"jsonl folder in S3 dated {sorted(matching_month)[-1]} is from this month but "
             f"PRECEDES the current harvest {harvest_date_str}. S3 sync NOT yet done for {date}.")
    elif sub_done_current == 3:
        s = WARN
        d = "local pipeline complete but S3 doesn't show a folder yet"
    else:
        s = PENDING
        d = "blocked on Stage 5"
    stages.append({"name": "Stage 6/6: S3 sync to dpla-master-dataset", "status": s, "detail": d})

    return stages, {
        "running_download": running_download,
        "running_preprocess": running_preprocess,
        "running_harvest": running_harvest,
        "running_pipeline": running_pipeline,
    }


# ---------- next-action suggester ----------
def suggest_next_action(stages, running):
    """Return a one-paragraph suggestion of what to do next."""
    if any(running.values()):
        which = next(k for k, v in running.items() if v)
        if which == "running_download":
            return "Wait for the S3 download to finish. Re-check in 5–15 min."
        if which == "running_preprocess":
            return "Wait for fix-si.sh to finish. NMNHBOTANY alone takes ~6 min, total ~10–15 min."
        if which == "running_harvest":
            return ("Wait for harvest.sh to finish. Smithsonian harvest is typically 3–4 hours. "
                    "Check the harvest log periodically.")
        if which == "running_pipeline":
            return ("Wait for ingest.sh to finish. Mapping/enrichment/jsonl + S3 sync is typically "
                    "1–2 hours. Watch the pipeline log.")

    # Nothing running — find the first pending stage.
    for stage in stages:
        if stage["status"] == PENDING:
            num = stage["name"].split(":")[0]
            if "1/6" in num:
                return "Stage 1: kick off the download (aws s3 sync from dpla-hub-si)."
            if "2/6" in num:
                return "Stage 2: run fix-si.sh on the downloaded folder."
            if "3/6" in num:
                return "Stage 3: update smithsonian.harvest.endpoint in i3.conf, push, pull on box."
            if "4/6" in num:
                return "Stage 4: kick off the harvest (./scripts/harvest.sh smithsonian)."
            if "5/6" in num:
                return "Stage 5: kick off the pipeline (./scripts/ingest.sh smithsonian --skip-harvest)."
            if "6/6" in num:
                return ("Stage 6: ingest.sh should have synced to S3. If it didn't, check the "
                        "pipeline log for S3 errors and consider a manual aws s3 sync.")

    # Look for STALE first — that means we have outputs from a previous run
    # but the current run's pipeline/S3 hasn't been done yet.
    for stage in stages:
        if stage["status"] == STALE:
            num = stage["name"].split(":")[0]
            if "5/6" in num:
                return ("Stage 5: previous run's pipeline outputs are present but predate the "
                        "current harvest. Run ./scripts/ingest.sh smithsonian --skip-harvest to "
                        "produce fresh mapping/enrichment/jsonl for this delivery.")
            if "6/6" in num:
                return ("Stage 6: S3 has an earlier folder but not one matching the current "
                        "harvest. After the pipeline finishes, the S3 sync should land a fresh "
                        "folder dated >= the current harvest.")

    if any(s["status"] == WARN for s in stages):
        return "There are warnings — review the [WARN] lines above."
    return "All six stages complete. Smithsonian is done for this delivery."


# ---------- render ----------
def render(date, stages, running, sections):
    out = []
    out.append("")
    out.append("=" * 70)
    out.append(f"  Smithsonian Ingest Status — delivery {date}")
    out.append("=" * 70)
    out.append("")
    for st in stages:
        out.append(f"  {st['status']:<10} {st['name']}")
        out.append(f"             {st['detail']}")
        out.append("")

    # Running-stage detail (log tail)
    if running.get("running_harvest"):
        out.append("HARVEST LOG (last 15 lines):")
        out.append(sections.get("HARVEST_LOG", "").rstrip())
        out.append("")
    elif running.get("running_pipeline"):
        out.append("PIPELINE LOG (last 15 lines):")
        out.append(sections.get("PIPELINE_LOG", "").rstrip())
        out.append("")

    out.append("NEXT ACTION:")
    out.append(f"  {suggest_next_action(stages, running)}")
    out.append("")
    return "\n".join(out)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Smithsonian ingest status checker")
    parser.add_argument("--date", help="Delivery date YYYY-MM-DD. Default: latest in s3://dpla-hub-si/.")
    parser.add_argument(
        "--watch", nargs="?", const=60, type=int, default=None,
        help="Refresh every N seconds (default 60).",
    )
    args = parser.parse_args()

    date = args.date
    if not date:
        date = find_latest_ia_delivery()
        if not date:
            sys.exit(f"Could not find any deliveries in s3://{S3_DELIVERY_BUCKET}/")
        print(f"(auto-detected latest delivery: {date})")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        sys.exit(f"Invalid --date {date}. Use YYYY-MM-DD.")

    script = build_status_script(date)

    def one_pass():
        try:
            out = ssm_run(script)
        except RuntimeError as e:
            print(f"[ERROR] {e}")
            return
        sections = parse_sections(out)
        stages, running = analyze(date, sections)
        print(render(date, stages, running, sections))

    if args.watch is None:
        one_pass()
        return

    interval = args.watch
    try:
        while True:
            os.system("clear" if os.name == "posix" else "cls")
            print(time.strftime("Last refresh: %Y-%m-%d %H:%M:%S"))
            one_pass()
            print(f"(refreshing every {interval}s — Ctrl+C to exit)")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()