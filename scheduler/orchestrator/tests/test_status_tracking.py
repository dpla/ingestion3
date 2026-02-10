"""Tests for file-based status tracking."""

import json
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from scheduler.orchestrator.state import IngestState, HubStatus, HubState, RunState


@pytest.fixture
def temp_state_dir(tmp_path):
    """Create a temporary state file path."""
    return tmp_path / "orchestrator_state.json"


@pytest.fixture
def state(temp_state_dir):
    """Create an IngestState instance with temp directory."""
    return IngestState(temp_state_dir)


class TestFileBasedStatusTracking:
    """Test per-hub .status files are written alongside main state."""

    def test_status_dir_created(self, state):
        """Status directory should be created on init."""
        assert state.status_dir.exists()
        assert state.status_dir.is_dir()

    def test_hub_status_file_written_on_update(self, state):
        """Updating a hub should write a .status file."""
        run_id = state.start_run(month=2, hubs=["maryland"])
        state.update_hub(run_id, "maryland", HubStatus.HARVESTING)

        status_file = state.status_dir / "maryland.status"
        assert status_file.exists()

        data = json.loads(status_file.read_text())
        assert data["hub"] == "maryland"
        assert data["status"] == "harvesting"
        assert data["run_id"] == run_id

    def test_status_file_updates_on_each_state_change(self, state):
        """Status file should be overwritten on each update."""
        run_id = state.start_run(month=2, hubs=["nara"])

        state.update_hub(run_id, "nara", HubStatus.PREPARING)
        data1 = json.loads((state.status_dir / "nara.status").read_text())
        assert data1["status"] == "preparing"

        state.update_hub(run_id, "nara", HubStatus.HARVESTING)
        data2 = json.loads((state.status_dir / "nara.status").read_text())
        assert data2["status"] == "harvesting"

        state.update_hub(run_id, "nara", HubStatus.COMPLETE)
        data3 = json.loads((state.status_dir / "nara.status").read_text())
        assert data3["status"] == "complete"
        assert data3["completed_at"] is not None

    def test_status_file_includes_error_info(self, state):
        """Failed hub status file should include error details."""
        run_id = state.start_run(month=2, hubs=["test-hub"])
        state.update_hub(
            run_id, "test-hub", HubStatus.FAILED,
            error="Harvest timeout",
            failure_stage="harvest",
        )

        data = json.loads((state.status_dir / "test-hub.status").read_text())
        assert data["status"] == "failed"
        assert data["error"] == "Harvest timeout"
        assert data["failure_stage"] == "harvest"

    def test_multiple_hub_status_files(self, state):
        """Multiple hubs should each have their own status file."""
        hubs = ["maryland", "virginia", "michigan"]
        run_id = state.start_run(month=2, hubs=hubs)

        for hub in hubs:
            state.update_hub(run_id, hub, HubStatus.PREPARING)

        for hub in hubs:
            status_file = state.status_dir / f"{hub}.status"
            assert status_file.exists(), f"Missing status file for {hub}"

    def test_status_file_has_updated_at_timestamp(self, state):
        """Status file should include an updated_at timestamp."""
        run_id = state.start_run(month=2, hubs=["maryland"])
        state.update_hub(run_id, "maryland", HubStatus.HARVESTING)

        data = json.loads((state.status_dir / "maryland.status").read_text())
        assert "updated_at" in data
        # Should be a valid ISO timestamp
        datetime.fromisoformat(data["updated_at"])


class TestIngestStateCore:
    """Test core IngestState functionality."""

    def test_start_run_creates_state(self, state):
        """start_run should create a run with pending hubs."""
        run_id = state.start_run(month=3, hubs=["maryland", "virginia"])
        run = state.get_run(run_id)

        assert run is not None
        assert run.month == 3
        assert len(run.hubs) == 2
        assert run.hubs["maryland"].status == HubStatus.PENDING

    def test_get_latest_run(self, state):
        """get_latest_run should return the most recent run."""
        run_id1 = state.start_run(month=1, hubs=["a"])
        run_id2 = state.start_run(month=2, hubs=["b"])

        latest = state.get_latest_run()
        assert latest.run_id == run_id2

    def test_get_failed_hubs(self, state):
        """get_failed_hubs should return only failed hub names."""
        run_id = state.start_run(month=2, hubs=["a", "b", "c"])
        state.update_hub(run_id, "a", HubStatus.COMPLETE)
        state.update_hub(run_id, "b", HubStatus.FAILED, error="timeout")
        state.update_hub(run_id, "c", HubStatus.COMPLETE)

        failed = state.get_failed_hubs(run_id)
        assert failed == ["b"]

    def test_complete_run(self, state):
        """complete_run should set status and completed_at."""
        run_id = state.start_run(month=2, hubs=["a"])
        state.complete_run(run_id, "complete")

        run = state.get_run(run_id)
        assert run.status == "complete"
        assert run.completed_at is not None

    def test_state_persists_to_file(self, temp_state_dir):
        """State should be loadable from file after save."""
        state1 = IngestState(temp_state_dir)
        run_id = state1.start_run(month=2, hubs=["maryland"])
        state1.update_hub(run_id, "maryland", HubStatus.COMPLETE)

        # Create a new IngestState from the same file
        state2 = IngestState(temp_state_dir)
        run = state2.get_run(run_id)
        assert run is not None
        assert run.hubs["maryland"].status == HubStatus.COMPLETE
