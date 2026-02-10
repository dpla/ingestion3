"""Tests for notification functionality."""

import os
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestNotificationFormatting:
    """Test notification message formatting."""

    def test_format_completed_hub_line(self, notifier, mock_config):
        """Test formatting a completed hub line for Slack."""
        info = {
            'status': 'complete',
            'duration': 1234,
            'records': 10000,
            'harvest_count': 10500,
            'mapping_attempted': 10500,
            'mapping_successful': 10000,
            'mapping_failed': 500,
            'issues_summary': '50 errors, 100 warnings',
            'summary_available': True,
            's3_paths': MagicMock(summary_url='https://example.com/summary'),
        }

        line = notifier._format_completed_hub_line('maryland', info)

        assert 'maryland' in line
        assert '10,500 harvested' in line
        assert '10,000/10,500 mapped' in line
        assert '500 failed' in line
        assert '50 errors' in line
        assert 'Summary' in line

    def test_format_completed_hub_no_summary(self, notifier, mock_config):
        """Test formatting when summary is unavailable."""
        info = {
            'status': 'complete',
            'duration': 1234,
            'records': 10000,
            'mapping_successful': 9500,
            'summary_available': False,
        }

        line = notifier._format_completed_hub_line('test-hub', info)

        assert 'test-hub' in line
        assert 'summary unavailable' in line

    def test_format_failed_hub_line(self, notifier, mock_config):
        """Test formatting a failed hub line for Slack."""
        info = {
            'status': 'failed',
            'failure_stage': 'harvest',
            'error': 'Timeout after 5 retries',
        }

        line = notifier._format_failed_hub_line('failed-hub', info)

        assert 'failed-hub' in line
        assert 'harvest' in line
        assert 'Timeout' in line


class TestCompletionNotification:
    """Test completion notification building."""

    def test_build_completion_message(self, notifier, sample_summary, mock_config):
        """Test building the full Slack completion message."""
        # Mock the config methods
        with patch.object(notifier, '_enrich_hub_summary', return_value=sample_summary['hubs']):
            with patch.object(notifier, '_write_email_drafts', return_value=(Path('/tmp/drafts'), [])):
                with patch.object(notifier, '_send_slack') as mock_slack:
                    notifier.send_completion_notification(
                        run_id=sample_summary['run_id'],
                        summary=sample_summary,
                        test_prefix="[TEST]",
                        write_drafts=False
                    )

                    # Verify Slack was called
                    assert mock_slack.called
                    payload = mock_slack.call_args[0][0]
                    text = payload['text']

                    # Check for test prefix
                    assert '[TEST]' in text

                    # Check for run info
                    assert sample_summary['run_id'] in text
                    assert '3/4 successful' in text

    def test_completion_message_truncation(self, notifier, mock_config):
        """Test that very long messages are truncated."""
        # Create a summary with many hubs
        many_hubs = {
            f'hub-{i}': {'status': 'complete', 'duration': i * 100, 'records': i * 1000}
            for i in range(100)
        }

        summary = {
            'run_id': 'TEST_truncation',
            'hubs': many_hubs,
            'totals': {'complete': 100, 'failed': 0, 'total': 100},
        }

        with patch.object(notifier, '_enrich_hub_summary', return_value=many_hubs):
            with patch.object(notifier, '_write_email_drafts', return_value=(None, [])):
                with patch.object(notifier, '_send_slack') as mock_slack:
                    notifier.send_completion_notification(
                        run_id=summary['run_id'],
                        summary=summary,
                        test_prefix="[TEST]",
                        write_drafts=False
                    )

                    # Verify message is truncated
                    payload = mock_slack.call_args[0][0]
                    text = payload['text']
                    assert len(text) <= 35050  # MAX_SLACK_TEXT_LENGTH + buffer


class TestAnomalyAlert:
    """Test anomaly alert notifications."""

    def test_anomaly_alert_warning(self, notifier, sample_anomaly_report, mock_config):
        """Test sending a warning-level anomaly alert."""
        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.send_anomaly_alert(
                hub="test-hub",
                report=sample_anomaly_report,
                test_prefix="[TEST]"
            )

            assert mock_slack.called
            payload = mock_slack.call_args[0][0]
            text = payload['text']

            assert '[TEST]' in text
            assert 'WARNING' in text
            assert 'test-hub' in text
            assert 'Harvest records dropped' in text

    def test_anomaly_alert_critical(self, notifier, mock_config):
        """Test sending a critical-level anomaly alert."""
        from scheduler.orchestrator.anomaly_detector import (
            AnomalyReport, Anomaly, AnomalyType, IngestCounts
        )

        critical_report = AnomalyReport(
            hub="critical-hub",
            current_mapping=IngestCounts(
                hub="critical-hub",
                timestamp="20260203_120000",
                attempted=5000,
                successful=2000,
                failed=3000,
            ),
            baseline_mapping=IngestCounts(
                hub="critical-hub",
                timestamp="20260103_120000",
                attempted=10000,
                successful=9800,
                failed=200,
            ),
            anomalies=[
                Anomaly(
                    hub="critical-hub",
                    anomaly_type=AnomalyType.OUTPUT_DROP,
                    severity='critical',
                    message="Mapped records dropped by 80%",
                    current_value=2000,
                    baseline_value=9800,
                    threshold=0.30,
                    percent_change=-79.6,
                )
            ]
        )

        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.send_anomaly_alert(
                hub="critical-hub",
                report=critical_report,
                test_prefix="[TEST]"
            )

            payload = mock_slack.call_args[0][0]
            text = payload['text']

            assert 'ERROR' in text
            assert 'Sync HALTED' in text

    def test_no_alert_without_anomalies(self, notifier, mock_config):
        """Test that no alert is sent when there are no anomalies."""
        from scheduler.orchestrator.anomaly_detector import AnomalyReport

        empty_report = AnomalyReport(hub="clean-hub", anomalies=[])

        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.send_anomaly_alert(
                hub="clean-hub",
                report=empty_report,
                test_prefix="[TEST]"
            )

            # Should not send anything
            assert not mock_slack.called


class TestEmailDrafts:
    """Test email draft generation."""

    def test_write_email_drafts(self, notifier, mock_config, tmp_path):
        """Test writing email draft files."""
        mock_config.logs_dir = tmp_path

        enriched_hubs = {
            'maryland': {
                'status': 'complete',
                'mapping_successful': 10000,
                'summary_available': True,
            },
            'no-email-hub': {
                'status': 'complete',
                'mapping_successful': 5000,
            },
        }

        draft_dir, hubs_without_email = notifier._write_email_drafts(
            run_id="TEST_run",
            enriched_hubs=enriched_hubs,
            test_prefix="[TEST]"
        )

        # Should have written draft for maryland
        assert draft_dir is not None
        assert draft_dir.exists()

        # Should report no-email-hub as missing email
        assert 'no-email-hub' in hubs_without_email

        # Check draft file exists
        draft_files = list(draft_dir.glob("*.draft.txt"))
        assert len(draft_files) >= 1

    def test_draft_content_format(self, notifier, mock_config, tmp_path):
        """Test the format of draft email content."""
        mock_config.logs_dir = tmp_path

        info = {
            'status': 'complete',
            'mapping_attempted': 10000,
            'mapping_successful': 9500,
            'mapping_failed': 500,
            'summary_available': False,
            's3_paths': MagicMock(summary_url='https://example.com/summary', logs_url=None),
        }

        content = notifier._build_email_draft(
            hub_name='maryland',
            hub_email='contact@maryland.org',
            info=info,
            test_prefix='[TEST]'
        )

        # Check required sections
        assert 'To: scott@dp.la' in content  # Test recipient
        assert 'Subject:' in content
        assert '[TEST]' in content
        assert 'DPLA Ingest Summary' in content
        assert 'Bleep bloop' in content  # Matches Emailer.scala


@pytest.mark.dry_run_slack
class TestDryRunSlack:
    """Tests that actually send to Slack. Only run when explicitly requested."""

    @pytest.mark.skipif(
        not os.environ.get('SLACK_WEBHOOK'),
        reason="SLACK_WEBHOOK not set"
    )
    def test_send_test_notification(self, sample_summary):
        """Send a real test notification to Slack."""
        from scheduler.orchestrator.config import load_config
        from scheduler.orchestrator.notifications import Notifier

        config = load_config()
        notifier = Notifier(config)

        notifier.send_completion_notification(
            run_id=sample_summary['run_id'],
            summary=sample_summary,
            test_prefix="[PYTEST-TEST]",
            write_drafts=False
        )

        print("Test notification sent - check #tech-alerts in Slack")

    @pytest.mark.skipif(
        not os.environ.get('SLACK_WEBHOOK'),
        reason="SLACK_WEBHOOK not set"
    )
    def test_send_test_anomaly_alert(self, sample_anomaly_report):
        """Send a real test anomaly alert to Slack."""
        from scheduler.orchestrator.config import load_config
        from scheduler.orchestrator.notifications import Notifier

        config = load_config()
        notifier = Notifier(config)

        notifier.send_anomaly_alert(
            hub="pytest-test-hub",
            report=sample_anomaly_report,
            test_prefix="[PYTEST-TEST]"
        )

        print("Test anomaly alert sent - check #tech-alerts in Slack")
