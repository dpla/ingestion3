#!/usr/bin/env python3
"""Write per-hub .status files for manual ingest runs (harvest.sh, ingest.sh, remap.sh).

The ingest-status script reads these files; they use the same JSON format as
the orchestrator's IngestState. Manual scripts call this so ingest-status.sh
works when running multiple ingests via scripts (not the orchestrator).

Usage:
    python -m scheduler.orchestrator.write_status <hub> <status> [options]
    python -m scheduler.orchestrator.write_status mwdl harvesting
    python -m scheduler.orchestrator.write_status mwdl remapping --records=50000
    python -m scheduler.orchestrator.write_status mwdl complete
    python -m scheduler.orchestrator.write_status mwdl failed --error="Harvest timeout"
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


def write_status(
    hub: str,
    status: str,
    *,
    status_dir: Path | None = None,
    started_at: str | None = None,
    stage_started_at: str | None = None,
    harvest_records: int | None = None,
    error: str | None = None,
) -> None:
    """Write a per-hub status file compatible with status.py."""
    if status_dir is None:
        i3_home = Path(__file__).resolve().parents[2]
        status_dir = i3_home / "logs" / "status"
    status_dir = Path(status_dir)
    status_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now().isoformat()
    data = {
        "hub": hub,
        "run_id": "manual",
        "status": status,
        "started_at": started_at or now,
        "stage_started_at": stage_started_at or now,
        "updated_at": now,
    }
    if harvest_records is not None:
        data["harvest_records"] = harvest_records
    if error:
        data["error"] = error

    path = status_dir / f"{hub}.status"
    try:
        path.write_text(json.dumps(data, indent=2))
    except OSError:
        pass  # Non-fatal


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write per-hub status file for manual ingest runs"
    )
    parser.add_argument("hub", help="Hub/provider name")
    parser.add_argument(
        "status",
        help="Status: preparing, harvesting, remapping, syncing, complete, failed",
    )
    parser.add_argument("--status-dir", type=Path, help="Override status directory")
    parser.add_argument("--error", help="Error message (for failed status)")
    parser.add_argument("--records", type=int, help="Harvest record count")
    parser.add_argument(
        "--started-at",
        help="ISO timestamp when ingest started (default: now)",
    )
    parser.add_argument(
        "--stage-started-at",
        help="ISO timestamp when current stage started (default: now)",
    )
    args = parser.parse_args()

    write_status(
        args.hub,
        args.status,
        status_dir=args.status_dir,
        started_at=args.started_at,
        stage_started_at=args.stage_started_at,
        harvest_records=args.records,
        error=args.error,
    )


if __name__ == "__main__":
    main()
