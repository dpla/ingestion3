"""State management for tracking ingest runs.

Provides per-hub, per-stage timing so that status can be read
instantly from JSON files without running any subprocess commands.
"""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, Any


class HubStatus(str, Enum):
    PENDING = "pending"
    PREPARING = "preparing"
    HARVESTING = "harvesting"
    MAPPING = "mapping"
    ENRICHING = "enriching"
    JSONL_EXPORT = "jsonl_export"
    REMAPPING = "remapping"  # legacy: combined mapping+enrichment+jsonl
    SYNCING = "syncing"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"


# Ordered stages for display and progress tracking
STAGE_ORDER: list[HubStatus] = [
    HubStatus.PREPARING,
    HubStatus.HARVESTING,
    HubStatus.MAPPING,
    HubStatus.ENRICHING,
    HubStatus.JSONL_EXPORT,
    HubStatus.SYNCING,
]


@dataclass
class StageRecord:
    """Timing record for a single pipeline stage."""
    stage: str
    started_at: str
    ended_at: Optional[str] = None
    duration_seconds: Optional[int] = None

    def to_dict(self) -> dict:
        d = {'stage': self.stage, 'started_at': self.started_at}
        if self.ended_at:
            d['ended_at'] = self.ended_at
        if self.duration_seconds is not None:
            d['duration_seconds'] = self.duration_seconds
        return d

    @classmethod
    def from_dict(cls, data: dict) -> 'StageRecord':
        return cls(
            stage=data['stage'],
            started_at=data['started_at'],
            ended_at=data.get('ended_at'),
            duration_seconds=data.get('duration_seconds'),
        )


@dataclass
class HubState:
    """State for a single hub in a run."""
    hub_name: str
    status: HubStatus = HubStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[int] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    diagnosis: Optional[dict] = None
    retries: int = 0
    harvest_records: Optional[int] = None
    failure_stage: Optional[str] = None  # harvest, mapping, sync, anomaly, unknown
    # Per-stage timing
    stage_started_at: Optional[str] = None
    stage_history: list[StageRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d['status'] = self.status.value
        d['stage_history'] = [sr.to_dict() for sr in self.stage_history]
        return d

    @classmethod
    def from_dict(cls, data: dict) -> 'HubState':
        data['status'] = HubStatus(data.get('status', 'pending'))
        if 'failure_stage' not in data:
            data['failure_stage'] = None
        if 'stage_started_at' not in data:
            data['stage_started_at'] = None
        raw_history = data.pop('stage_history', [])
        hub = cls(**data)
        hub.stage_history = [StageRecord.from_dict(sr) for sr in raw_history]
        return hub


@dataclass
class RunState:
    """State for a complete ingest run."""
    run_id: str
    month: int
    year: int
    started_at: str
    completed_at: Optional[str] = None
    status: str = "running"
    hubs: dict[str, HubState] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'run_id': self.run_id,
            'month': self.month,
            'year': self.year,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'status': self.status,
            'hubs': {k: v.to_dict() for k, v in self.hubs.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'RunState':
        hubs = {k: HubState.from_dict(v) for k, v in data.get('hubs', {}).items()}
        return cls(
            run_id=data['run_id'],
            month=data['month'],
            year=data['year'],
            started_at=data['started_at'],
            completed_at=data.get('completed_at'),
            status=data.get('status', 'running'),
            hubs=hubs,
        )


class IngestState:
    """Manages persistent state for ingest runs.

    In addition to the main state file (orchestrator_state.json), writes
    per-hub status files to ``<logs_dir>/status/<hub>.status`` (JSON) for
    easy external monitoring::

        cat logs/status/nara.status | python -m json.tool
    """

    def __init__(self, state_file: Path):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        # Per-hub status dir sits next to the state file
        self.status_dir = self.state_file.parent / "status"
        self.status_dir.mkdir(parents=True, exist_ok=True)
        self._state: dict[str, RunState] = {}
        self._load()

    def _load(self):
        """Load state from file."""
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text())
                self._state = {
                    run_id: RunState.from_dict(run_data)
                    for run_id, run_data in data.get('runs', {}).items()
                }
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Could not load state file: {e}")
                self._state = {}

    def _save(self):
        """Save state to file."""
        data = {
            'runs': {run_id: run.to_dict() for run_id, run in self._state.items()},
            'last_updated': datetime.now().isoformat(),
        }
        self.state_file.write_text(json.dumps(data, indent=2, default=str))

    def _write_hub_status_file(self, run_id: str, hub_name: str):
        """Write a per-hub status file for external monitoring.

        File: ``<status_dir>/<hub>.status`` (JSON)

        Designed to be self-contained: an external reader (``status.py``,
        an agent, or ``cat``) can get a full picture from this file alone
        without running any subprocess commands.
        """
        run = self._state.get(run_id)
        if not run:
            return
        hub = run.hubs.get(hub_name)
        if not hub:
            return

        now = datetime.now()

        # Compute time-in-current-stage (live, computed at write time)
        time_in_stage: Optional[int] = None
        if hub.stage_started_at:
            try:
                stage_start = datetime.fromisoformat(hub.stage_started_at)
                time_in_stage = int((now - stage_start).total_seconds())
            except ValueError:
                pass

        # Compute total elapsed if still running
        total_elapsed: Optional[int] = hub.duration_seconds
        if total_elapsed is None and hub.started_at:
            try:
                total_elapsed = int(
                    (now - datetime.fromisoformat(hub.started_at)).total_seconds()
                )
            except ValueError:
                pass

        # Stage progress: which stage out of how many
        stage_index: Optional[int] = None
        total_stages = len(STAGE_ORDER)
        try:
            stage_index = STAGE_ORDER.index(hub.status) + 1
        except ValueError:
            pass  # terminal states (complete/failed) or pending

        status_data = {
            'hub': hub_name,
            'run_id': run_id,
            'status': hub.status.value,
            # Hub-level timing
            'started_at': hub.started_at,
            'completed_at': hub.completed_at,
            'total_elapsed_seconds': total_elapsed,
            # Current stage timing
            'stage_started_at': hub.stage_started_at,
            'time_in_stage_seconds': time_in_stage,
            # Progress indicator (e.g. stage 3 of 6)
            'stage_index': stage_index,
            'total_stages': total_stages,
            # Stage history (completed + current)
            'stage_history': [sr.to_dict() for sr in hub.stage_history],
            # Counts
            'harvest_records': hub.harvest_records,
            # Error info
            'error': hub.error,
            'failure_stage': hub.failure_stage,
            'retries': hub.retries,
            # Bookkeeping
            'updated_at': now.isoformat(),
        }

        status_file = self.status_dir / f"{hub_name}.status"
        try:
            status_file.write_text(json.dumps(status_data, indent=2, default=str))
        except OSError:
            pass  # Non-fatal: don't break the pipeline for a status file

    def start_run(self, month: int, hubs: list[str]) -> str:
        """Start a new ingest run."""
        now = datetime.now()
        run_id = f"{now.year}{now.month:02d}{now.day:02d}_{now.hour:02d}{now.minute:02d}{now.second:02d}"

        run = RunState(
            run_id=run_id,
            month=month,
            year=now.year,
            started_at=now.isoformat(),
            hubs={hub: HubState(hub_name=hub) for hub in hubs},
        )

        self._state[run_id] = run
        self._save()

        return run_id

    def get_run(self, run_id: str) -> Optional[RunState]:
        """Get a specific run."""
        return self._state.get(run_id)

    def get_latest_run(self) -> Optional[RunState]:
        """Get the most recent run."""
        if not self._state:
            return None
        latest_id = max(self._state.keys())
        return self._state[latest_id]

    def update_hub(
        self,
        run_id: str,
        hub_name: str,
        status: HubStatus,
        error: Optional[str] = None,
        error_type: Optional[str] = None,
        diagnosis: Optional[dict] = None,
        retries: Optional[int] = None,
        harvest_records: Optional[int] = None,
        failure_stage: Optional[str] = None,
    ):
        """Update status for a hub in a run.

        Automatically tracks per-stage timing: when the status changes to a
        new pipeline stage, the previous stage is closed (end time + duration
        recorded) and the new stage is opened.

        Args:
            failure_stage: For failed hubs, the stage at which failure occurred:
                           'harvest', 'mapping', 'sync', 'anomaly', or 'unknown'
        """
        run = self._state.get(run_id)
        if not run:
            raise ValueError(f"Run not found: {run_id}")

        hub = run.hubs.get(hub_name)
        if not hub:
            hub = HubState(hub_name=hub_name)
            run.hubs[hub_name] = hub

        now = datetime.now()
        previous_status = hub.status

        # ---- Stage transition bookkeeping ----
        if status != previous_status:
            # Close the previous stage record (if one is open)
            if hub.stage_history and hub.stage_history[-1].ended_at is None:
                prev = hub.stage_history[-1]
                prev.ended_at = now.isoformat()
                start = datetime.fromisoformat(prev.started_at)
                prev.duration_seconds = int((now - start).total_seconds())

            # Open a new stage record for active pipeline stages
            active_stages = {
                HubStatus.PREPARING, HubStatus.HARVESTING, HubStatus.MAPPING,
                HubStatus.ENRICHING, HubStatus.JSONL_EXPORT, HubStatus.REMAPPING,
                HubStatus.SYNCING,
            }
            if status in active_stages:
                hub.stage_started_at = now.isoformat()
                hub.stage_history.append(StageRecord(
                    stage=status.value,
                    started_at=now.isoformat(),
                ))
            else:
                hub.stage_started_at = None

        hub.status = status

        if status == HubStatus.PREPARING and not hub.started_at:
            hub.started_at = now.isoformat()

        if status in (HubStatus.COMPLETE, HubStatus.FAILED, HubStatus.SKIPPED):
            hub.completed_at = now.isoformat()
            if hub.started_at:
                start = datetime.fromisoformat(hub.started_at)
                hub.duration_seconds = int((now - start).total_seconds())

        if error is not None:
            hub.error = error
        if error_type is not None:
            hub.error_type = error_type
        if diagnosis is not None:
            hub.diagnosis = diagnosis
        if retries is not None:
            hub.retries = retries
        if harvest_records is not None:
            hub.harvest_records = harvest_records
        if failure_stage is not None:
            hub.failure_stage = failure_stage

        self._save()
        self._write_hub_status_file(run_id, hub_name)

    def complete_run(self, run_id: str, status: str = "complete"):
        """Mark a run as complete."""
        run = self._state.get(run_id)
        if run:
            run.status = status
            run.completed_at = datetime.now().isoformat()
            self._save()

    def get_failed_hubs(self, run_id: str) -> list[str]:
        """Get list of failed hubs for a run."""
        run = self._state.get(run_id)
        if not run:
            return []
        return [
            hub_name for hub_name, hub in run.hubs.items()
            if hub.status == HubStatus.FAILED
        ]

    def get_failure_report(self, run_id: str) -> dict:
        """Generate a failure report for a run."""
        run = self._state.get(run_id)
        if not run:
            return {}

        failed_hubs = self.get_failed_hubs(run_id)

        return {
            'run_id': run_id,
            'month': run.month,
            'year': run.year,
            'timestamp': datetime.now().isoformat(),
            'failed_hubs': failed_hubs,
            'failures': {
                hub_name: {
                    'error': run.hubs[hub_name].error,
                    'error_type': run.hubs[hub_name].error_type,
                    'diagnosis': run.hubs[hub_name].diagnosis,
                    'retries': run.hubs[hub_name].retries,
                    'failure_stage': run.hubs[hub_name].failure_stage,
                }
                for hub_name in failed_hubs
            },
            'summary': {
                'total': len(run.hubs),
                'complete': sum(1 for h in run.hubs.values() if h.status == HubStatus.COMPLETE),
                'failed': len(failed_hubs),
                'skipped': sum(1 for h in run.hubs.values() if h.status == HubStatus.SKIPPED),
            }
        }

    def get_summary(self, run_id: str) -> dict:
        """Get summary for a run."""
        run = self._state.get(run_id)
        if not run:
            return {}

        hubs_summary = {}
        for hub_name, hub in run.hubs.items():
            hub_info = {
                'status': hub.status.value,
                'duration': hub.duration_seconds,
                'records': hub.harvest_records,
            }
            # Include failure info for failed hubs
            if hub.status == HubStatus.FAILED:
                hub_info['error'] = hub.error
                hub_info['error_type'] = hub.error_type
                hub_info['failure_stage'] = hub.failure_stage
            hubs_summary[hub_name] = hub_info

        return {
            'run_id': run_id,
            'month': run.month,
            'year': run.year,
            'started_at': run.started_at,
            'completed_at': run.completed_at,
            'status': run.status,
            'hubs': hubs_summary,
            'totals': {
                'complete': sum(1 for h in run.hubs.values() if h.status == HubStatus.COMPLETE),
                'failed': sum(1 for h in run.hubs.values() if h.status == HubStatus.FAILED),
                'total': len(run.hubs),
            }
        }
