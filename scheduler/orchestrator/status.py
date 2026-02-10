#!/usr/bin/env python3
"""Fast, file-based ingest status reader.

Reads per-hub ``.status`` JSON files written by the orchestrator and
formats them as a human-readable table.  No subprocess calls — status
is available instantly.

Usage (standalone)::

    python3 -m scheduler.orchestrator.status            # table
    python3 -m scheduler.orchestrator.status --json     # raw JSON
    python3 -m scheduler.orchestrator.status wisconsin  # single hub
    python3 -m scheduler.orchestrator.status --watch    # auto-refresh

Usage (from agent / Python)::

    from scheduler.orchestrator.status import get_status_table
    print(get_status_table())              # formatted string
    print(get_status_table("wisconsin"))   # single hub
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional


# Default location — matches IngestState.status_dir
DEFAULT_STATUS_DIR = Path("logs/status")

# Stage display names and icons
STAGE_DISPLAY = {
    "pending": ("Pending", "⏳"),
    "preparing": ("Preparing", "📥"),
    "harvesting": ("Harvesting", "🌾"),
    "mapping": ("Mapping", "🗺️"),
    "enriching": ("Enriching", "✨"),
    "jsonl_export": ("JSONL Export", "📦"),
    "remapping": ("Remapping", "🔄"),
    "syncing": ("S3 Sync", "☁️"),
    "complete": ("Complete", "✅"),
    "failed": ("Failed", "❌"),
    "skipped": ("Skipped", "⏭️"),
}


def _fmt_duration(seconds: int | None) -> str:
    """Format seconds as a compact human-readable string."""
    if seconds is None:
        return "—"
    if seconds < 0:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s:02d}s"
    else:
        h, remainder = divmod(seconds, 3600)
        m = remainder // 60
        return f"{h}h {m:02d}m"


def _fmt_time(iso: str | None) -> str:
    """Format an ISO timestamp as a short local time string."""
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%H:%M:%S")
    except ValueError:
        return iso[:19]


def _fmt_date_time(iso: str | None) -> str:
    """Format an ISO timestamp as date + time."""
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return iso[:16]


def _recompute_total_elapsed(data: dict) -> int | None:
    """Re-compute total elapsed from started_at for active hubs, or use the
    stored value for completed/failed hubs."""
    status = data.get("status", "")
    terminal = {"complete", "failed", "skipped"}
    if status in terminal:
        # Use stored value (or compat fallback)
        return data.get("total_elapsed_seconds") or data.get("duration_seconds")
    started = data.get("started_at")
    if not started:
        return data.get("total_elapsed_seconds") or data.get("duration_seconds")
    try:
        start = datetime.fromisoformat(started)
        return max(0, int((datetime.now() - start).total_seconds()))
    except ValueError:
        return data.get("total_elapsed_seconds") or data.get("duration_seconds")


def _recompute_time_in_stage(data: dict) -> int | None:
    """Re-compute time_in_stage_seconds from stage_started_at so the value
    is always fresh, even if the file was written minutes ago."""
    stage_started = data.get("stage_started_at")
    if not stage_started:
        return data.get("time_in_stage_seconds")
    try:
        start = datetime.fromisoformat(stage_started)
        return max(0, int((datetime.now() - start).total_seconds()))
    except ValueError:
        return data.get("time_in_stage_seconds")


def read_status_files(
    status_dir: Path = DEFAULT_STATUS_DIR,
    hubs: list[str] | None = None,
) -> list[dict]:
    """Read all (or selected) .status files and return parsed dicts."""
    if not status_dir.exists():
        return []

    results = []
    for status_file in sorted(status_dir.glob("*.status")):
        hub_name = status_file.stem
        if hubs and hub_name not in hubs:
            continue
        try:
            data = json.loads(status_file.read_text())
            # Recompute live timing fields
            data["time_in_stage_seconds"] = _recompute_time_in_stage(data)
            # Recompute total elapsed live for active hubs
            data["total_elapsed_seconds"] = _recompute_total_elapsed(data)
            results.append(data)
        except (json.JSONDecodeError, OSError):
            results.append({"hub": hub_name, "status": "unknown", "error": "unreadable"})

    return results


def get_status_table(
    *hubs: str,
    status_dir: Path = DEFAULT_STATUS_DIR,
    verbose: bool = False,
) -> str:
    """Return a formatted status table as a string.

    This is the primary interface for agents and scripts.
    """
    hub_list = list(hubs) if hubs else None
    entries = read_status_files(status_dir, hub_list)

    if not entries:
        return "No status files found. Is an ingest running?"

    lines: list[str] = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append("")
    lines.append(f"{'═' * 72}")
    lines.append(f"  INGEST STATUS — {now_str}")
    lines.append(f"{'═' * 72}")

    # Sort: active stages first, then complete, then failed, then pending
    def sort_key(d: dict) -> tuple:
        status = d.get("status", "")
        active = {"preparing", "harvesting", "mapping", "enriching",
                  "jsonl_export", "remapping", "syncing"}
        if status in active:
            return (0, d.get("hub", ""))
        elif status == "complete":
            return (1, d.get("hub", ""))
        elif status == "failed":
            return (2, d.get("hub", ""))
        else:
            return (3, d.get("hub", ""))

    entries.sort(key=sort_key)

    for data in entries:
        hub = data.get("hub", "?")
        status = data.get("status", "unknown")
        label, icon = STAGE_DISPLAY.get(status, (status, "•"))

        stage_started = _fmt_time(data.get("stage_started_at"))
        time_in_stage = _fmt_duration(data.get("time_in_stage_seconds"))
        total_elapsed = _fmt_duration(data.get("total_elapsed_seconds"))

        # Progress indicator (e.g. "3/6")
        stage_idx = data.get("stage_index")
        total_stages = data.get("total_stages", 6)
        progress = f"{stage_idx}/{total_stages}" if stage_idx else ""

        # Records
        records = data.get("harvest_records")
        records_str = f"{records:,}" if records else ""

        lines.append("")
        lines.append(f"  {icon}  {hub}")
        lines.append(f"  {'─' * 40}")
        lines.append(f"     Stage:          {label}" +
                      (f"  ({progress})" if progress else ""))
        lines.append(f"     Stage started:  {stage_started}")
        lines.append(f"     Time in stage:  {time_in_stage}")
        lines.append(f"     Total elapsed:  {total_elapsed}")

        if records_str:
            lines.append(f"     Records:        {records_str}")

        if data.get("error"):
            err = data["error"]
            if len(err) > 60:
                err = err[:57] + "..."
            lines.append(f"     Error:          {err}")

        if verbose:
            # Show stage history
            history = data.get("stage_history", [])
            if history:
                lines.append(f"     History:")
                for sr in history:
                    s_label = STAGE_DISPLAY.get(sr["stage"], (sr["stage"], ""))[0]
                    s_dur = _fmt_duration(sr.get("duration_seconds"))
                    s_start = _fmt_time(sr.get("started_at"))
                    ended = "running" if not sr.get("ended_at") else s_dur
                    lines.append(f"       {s_label:16s} {s_start} → {ended}")

    lines.append("")
    lines.append(f"{'═' * 72}")

    return "\n".join(lines)


def get_status_json(
    *hubs: str,
    status_dir: Path = DEFAULT_STATUS_DIR,
) -> list[dict]:
    """Return status data as a list of dicts (for agent consumption)."""
    hub_list = list(hubs) if hubs else None
    return read_status_files(status_dir, hub_list)


def main():
    parser = argparse.ArgumentParser(
        description="DPLA ingest status (reads .status files, no subprocess calls)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python3 -m scheduler.orchestrator.status                # table view
    python3 -m scheduler.orchestrator.status --json         # raw JSON
    python3 -m scheduler.orchestrator.status wisconsin      # single hub
    python3 -m scheduler.orchestrator.status -v             # with stage history
    python3 -m scheduler.orchestrator.status --watch        # auto-refresh every 30s
    python3 -m scheduler.orchestrator.status --watch 10     # auto-refresh every 10s
""",
    )
    parser.add_argument("hubs", nargs="*", help="Specific hubs to show")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show stage history")
    parser.add_argument("--watch", nargs="?", type=int, const=30, default=None,
                        metavar="SECONDS",
                        help="Auto-refresh (default every 30s)")
    parser.add_argument("--status-dir", type=Path, default=DEFAULT_STATUS_DIR,
                        help="Path to status directory")

    args = parser.parse_args()

    if args.json:
        data = get_status_json(*args.hubs, status_dir=args.status_dir)
        print(json.dumps(data, indent=2))
        return

    if args.watch is not None:
        try:
            while True:
                # Clear screen
                print("\033[2J\033[H", end="")
                print(get_status_table(
                    *args.hubs,
                    status_dir=args.status_dir,
                    verbose=args.verbose,
                ))
                print(f"\n  (refreshing every {args.watch}s — Ctrl-C to stop)")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print()
            return
    else:
        print(get_status_table(
            *args.hubs,
            status_dir=args.status_dir,
            verbose=args.verbose,
        ))


if __name__ == "__main__":
    main()
