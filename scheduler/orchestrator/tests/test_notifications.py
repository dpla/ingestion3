"""Tests for notification functionality."""

import os
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestNotificationFormatting:
    """Test notification message formatting."""

    def test_format_completed_hub_lines(self, notifier, mock_config):
        """Test formatting a completed hub block for Slack."""
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
        }

        lines = notifier._format_completed_hub_lines('maryland', info)
        block = "\n".join(lines)

        assert '`maryland`' in block
        assert 'Harvested: `10,500`' in block
        assert 'Mapped:' in block
        assert '- Successful: `10,000`' in block
        assert '- Failed: `500`' in block

    def test_format_completed_hub_no_summary(self, notifier, mock_config):
        """Test formatting when summary is unavailable."""
        info = {
            'status': 'complete',
            'duration': 1234,
            'records': 10000,
            'mapping_successful': 9500,
            'summary_available': False,
        }

        lines = notifier._format_completed_hub_lines('test-hub', info)
        block = "\n".join(lines)

        assert 'test-hub' in block
        assert '9,500' in block

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
                    assert 'DPLA Ingest Run Summary' in text

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

    def test_anomaly_alert_warning_format(self, notifier, sample_anomaly_report, mock_config):
        """Test warning anomaly alert uses bold percentages, dates, and Mapped sub-bullets."""
        mock_config.slack_webhook = "https://hooks.slack.com/test"
        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.send_anomaly_alert(
                hub="test-hub",
                report=sample_anomaly_report,
                test_prefix="[TEST]"
            )

            assert mock_slack.called
            payload = mock_slack.call_args[0][0]
            text = payload['text']

            # Header
            assert '[TEST]' in text
            assert 'WARNING' in text
            assert '`test-hub`' in text
            assert 'Sync proceeded' in text

            # Anomaly line: numbers in ticks, delta percentage bold
            assert '`10,500`' in text
            assert '`10,000`' in text
            assert '(:arrow_down: *-4.8%*)' in text

            # Current counts section uses date, not "Current"
            assert 'Current' not in text
            assert '*02-03-2026*' in text  # current timestamp 20260203
            assert 'Harvested: `10,000`' in text
            assert 'Mapped:' in text
            assert '- Successful: `9,500`' in text
            assert '- Failed: `500`' in text

            # Baseline counts section uses date + "(baseline)"
            assert '*01-03-2026* (baseline)' in text  # baseline timestamp 20260103
            assert 'Harvested: `10,500`' in text
            assert '- Successful: `10,200`' in text
            assert '- Failed: `300`' in text

    def test_anomaly_alert_critical_format(self, notifier, mock_config):
        """Test critical anomaly alert: HALTED, dates, non-delta percentages left alone."""
        from scheduler.orchestrator.anomaly_detector import (
            AnomalyReport, Anomaly, AnomalyType, IngestCounts
        )

        mock_config.slack_webhook = "https://hooks.slack.com/test"

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
                    message="Mapped records dropped from 9,800 to 2,000 (-79.6%)",
                    current_value=2000,
                    baseline_value=9800,
                    threshold=0.30,
                    percent_change=-79.6,
                ),
                Anomaly(
                    hub="critical-hub",
                    anomaly_type=AnomalyType.HIGH_FAILURE_RATE,
                    severity='critical',
                    message="Critical failure rate: 60% of records failed mapping",
                    current_value=0.60,
                    baseline_value=0.02,
                    threshold=0.40,
                    percent_change=60.0,
                ),
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

            # Delta percentage gets bold arrow treatment
            assert '(:arrow_down: *-79.6%*)' in text

            # Non-delta "60%" stays as-is (not wrapped in bold/arrow)
            assert '60% of records' in text

            # Dates instead of "Current" / "Baseline"
            assert 'Current' not in text
            assert 'Baseline' not in text.split('(baseline)')[0]  # only appears in label
            assert '*02-03-2026*' in text
            assert '*01-03-2026* (baseline)' in text

            # Mapped sub-bullet structure
            assert '- Successful: `2,000`' in text
            assert '- Failed: `3,000`' in text

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


class TestFormatCountsBlock:
    """Test the shared _format_counts_block helper."""

    def test_block_format(self):
        """Multi-line block with Harvested / Mapped sub-bullets."""
        from scheduler.orchestrator.notifications import Notifier
        result = Notifier._format_counts_block(10000, 9500, 500)
        assert result == (
            "Harvested: `10,000`\n"
            "Mapped:\n"
            "  - Successful: `9,500`\n"
            "  - Failed: `500`"
        )

    def test_block_format_no_harvest(self):
        """Block format with harvest omitted."""
        from scheduler.orchestrator.notifications import Notifier
        result = Notifier._format_counts_block(successful=9500, failed=500)
        assert result == (
            "Mapped:\n"
            "  - Successful: `9,500`\n"
            "  - Failed: `500`"
        )

    def test_inline_format(self):
        """Single-line inline format."""
        from scheduler.orchestrator.notifications import Notifier
        result = Notifier._format_counts_block(96388, 91729, 4659, inline=True)
        assert result == "Harvested: `96,388` · Successful: `91,729` · Failed: `4,659`"

    def test_inline_format_no_harvest(self):
        """Inline format without harvest."""
        from scheduler.orchestrator.notifications import Notifier
        result = Notifier._format_counts_block(successful=91729, failed=4659, inline=True)
        assert result == "Successful: `91,729` · Failed: `4,659`"

    def test_inline_all_none(self):
        """Inline format with nothing returns fallback text."""
        from scheduler.orchestrator.notifications import Notifier
        result = Notifier._format_counts_block(inline=True)
        assert result == "counts unavailable"


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


class TestHubCompleteSuccess:
    """Test hub-complete success notifications (→ #tech with @here)."""

    def test_hub_complete_no_baseline(self, notifier, mock_config):
        """Test hub-complete when no baseline exists (first ingest)."""
        with patch.object(notifier, '_send_slack_tech') as mock_slack:
            with patch.object(notifier, '_fetch_baseline', return_value=None):
                notifier.send_hub_complete_success(
                    hub="new-hub",
                    harvest_count=5000,
                    mapping_attempted=5000,
                    mapping_successful=4800,
                    mapping_failed=200,
                    current_run_date="02-11-2026",
                )

                assert mock_slack.called
                payload = mock_slack.call_args[0][0]
                text = payload['text']

                assert '<!here>' in text
                assert 'new-hub' in text
                assert 're-ingested' in text
                assert 'Harvested: `5,000`' in text
                assert 'Successful: `4,800`' in text
                assert 'Failed: `200`' in text
                # No diff section
                assert 'vs' not in text

    def test_hub_complete_with_baseline_deltas_on_current(self, notifier, mock_config):
        """Test that deltas appear on the current run block, not the previous."""
        baseline = {
            "date": "01-15-2026",
            "successful": 121000,
            "failed": 92,
            "harvest_attempted": 122000,
        }

        with patch.object(notifier, '_send_slack_tech') as mock_slack:
            with patch.object(notifier, '_fetch_baseline', return_value=baseline):
                notifier.send_hub_complete_success(
                    hub="wisconsin",
                    harvest_count=125000,
                    mapping_attempted=125000,
                    mapping_successful=122500,
                    mapping_failed=85,
                    current_run_date="02-10-2026",
                )

                payload = mock_slack.call_args[0][0]
                text = payload['text']

                # Current block has deltas
                assert 'Harvested: `125,000`  (:arrow_up: `3,000`)' in text
                assert 'Successful: `122,500`  (:arrow_up: `1,500`)' in text
                assert 'Failed: `85`  (:arrow_down: `7`)' in text

                # Previous block has plain counts with harvest
                assert 'vs previous run on *01-15-2026*' in text
                assert 'Harvested: `122,000`' in text
                assert 'Successful: `121,000`' in text
                assert 'Failed: `92`' in text

    def test_hub_complete_zero_delta_no_arrow(self, notifier, mock_config):
        """Test that zero deltas produce no arrow emoji."""
        baseline = {
            "date": "01-15-2026",
            "successful": 10000,
            "failed": 500,
            "harvest_attempted": 10500,
        }

        with patch.object(notifier, '_send_slack_tech') as mock_slack:
            with patch.object(notifier, '_fetch_baseline', return_value=baseline):
                notifier.send_hub_complete_success(
                    hub="stable-hub",
                    harvest_count=10500,
                    mapping_successful=10000,
                    mapping_failed=500,
                    current_run_date="02-10-2026",
                )

                payload = mock_slack.call_args[0][0]
                text = payload['text']

                assert ':arrow_up:' not in text
                assert ':arrow_down:' not in text
                assert 'Harvested: `10,500`' in text
                assert 'Successful: `10,000`' in text

    def test_hub_complete_uses_tech_webhook(self, notifier, mock_config):
        """Test that hub-complete uses SLACK_TECH_WEBHOOK."""
        mock_config.slack_tech_webhook = "https://hooks.slack.com/tech"
        mock_config.slack_webhook = "https://hooks.slack.com/alerts"

        with patch('urllib.request.urlopen') as mock_urlopen:
            with patch.object(notifier, '_fetch_baseline', return_value=None):
                notifier.send_hub_complete_success(
                    hub="test-hub",
                    harvest_count=100,
                    mapping_successful=90,
                    mapping_failed=10,
                )

                # Verify it was called with the tech webhook URL
                if mock_urlopen.called:
                    req = mock_urlopen.call_args[0][0]
                    assert req.full_url == "https://hooks.slack.com/tech"

    def test_hub_complete_falls_back_to_slack_webhook(self, notifier, mock_config):
        """Test fallback to SLACK_WEBHOOK when SLACK_TECH_WEBHOOK is not set."""
        mock_config.slack_tech_webhook = None
        mock_config.slack_webhook = "https://hooks.slack.com/alerts"

        with patch('urllib.request.urlopen') as mock_urlopen:
            with patch.object(notifier, '_fetch_baseline', return_value=None):
                notifier.send_hub_complete_success(
                    hub="test-hub",
                    harvest_count=100,
                    mapping_successful=90,
                    mapping_failed=10,
                )

                if mock_urlopen.called:
                    req = mock_urlopen.call_args[0][0]
                    assert req.full_url == "https://hooks.slack.com/alerts"


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
