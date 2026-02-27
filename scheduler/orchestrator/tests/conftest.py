"""Pytest fixtures for orchestrator tests."""

import os
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


@pytest.fixture
def mock_config():
    """Create a mock Config object."""
    from scheduler.orchestrator.config import Config, HubConfig

    config = MagicMock(spec=Config)
    config.data_dir = Path("/tmp/dpla-test-data")
    config.logs_dir = Path("/tmp/dpla-test-logs")
    config.i3_home = Path("/tmp/dpla-test-home")
    config.aws_profile = "dpla"
    config.slack_webhook = "https://hooks.slack.com/services/TEST/FAKE/WEBHOOK"
    config.slack_tech_webhook = "https://hooks.slack.com/services/TEST/FAKE/TECH"
    config.slack_alert_user_id = "U0TESTFAKE01"
    config.aliases_enabled = True
    config.github_token = None

    # S3 methods
    config.get_s3_dest_bucket.return_value = "dpla-master-dataset"
    config.resolve_s3_prefix.side_effect = lambda hub: {
        "hathi": "hathitrust",
        "tn": "tennessee",
        "hathitrust": "hathitrust",
        "tennessee": "tennessee",
    }.get(hub, hub)
    config.get_s3_prefix.side_effect = config.resolve_s3_prefix.side_effect

    # Hub configs
    def get_hub_config(hub_name):
        hub = MagicMock(spec=HubConfig)
        hub.name = hub_name
        hub.provider = hub_name.upper()
        hub.email = f"contact@{hub_name}.example.com" if hub_name != "no-email-hub" else ""
        hub.harvest_type = "oai"
        return hub

    config.get_hub_config.side_effect = get_hub_config

    return config


@pytest.fixture
def sample_summary():
    """Create a sample run summary for testing."""
    return {
        'run_id': f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'month': datetime.now().month,
        'year': datetime.now().year,
        'started_at': datetime.now().isoformat(),
        'completed_at': datetime.now().isoformat(),
        'status': 'partial',
        'hubs': {
            'maryland': {
                'status': 'complete',
                'duration': 1234,
                'records': 10000,
            },
            'wisconsin': {
                'status': 'complete',
                'duration': 567,
                'records': 5000,
            },
            'failed-hub': {
                'status': 'failed',
                'duration': 123,
                'records': None,
                'error': 'Harvest timeout after 5 retries',
                'error_type': 'timeout',
                'failure_stage': 'harvest',
            },
            'no-email-hub': {
                'status': 'complete',
                'duration': 890,
                'records': 7500,
            },
        },
        'totals': {
            'complete': 3,
            'failed': 1,
            'total': 4,
        }
    }


@pytest.fixture
def sample_anomaly_report():
    """Create a sample anomaly report for testing."""
    from scheduler.orchestrator.anomaly_detector import (
        AnomalyReport, Anomaly, AnomalyType, IngestCounts
    )

    return AnomalyReport(
        hub="test-hub",
        current_mapping=IngestCounts(
            hub="test-hub",
            timestamp="20260203_120000",
            attempted=10000,
            successful=9500,
            failed=500,
        ),
        baseline_mapping=IngestCounts(
            hub="test-hub",
            timestamp="20260103_120000",
            attempted=10500,
            successful=10200,
            failed=300,
        ),
        anomalies=[
            Anomaly(
                hub="test-hub",
                anomaly_type=AnomalyType.HARVEST_DROP,
                severity='warning',
                message="Harvest records dropped from 10,500 to 10,000 (-4.8%)",
                current_value=10000,
                baseline_value=10500,
                threshold=0.15,
                percent_change=-4.8,
            )
        ]
    )


@pytest.fixture
def notifier(mock_config):
    """Create a Notifier instance with mock config."""
    from scheduler.orchestrator.notifications import Notifier

    # Ensure test directories exist
    mock_config.data_dir.mkdir(parents=True, exist_ok=True)
    mock_config.logs_dir.mkdir(parents=True, exist_ok=True)
    (mock_config.data_dir / "escalations").mkdir(parents=True, exist_ok=True)

    return Notifier(mock_config)


# Markers for special tests
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "dry_run_slack: mark test to actually send to Slack (use with caution)"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring external services"
    )
