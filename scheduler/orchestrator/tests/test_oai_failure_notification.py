"""Tests for OAI harvest failure notification formatting."""

import os
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


def _make_oai_failure_report(run_id="TEST_oai_20260216"):
    """Build a sample failure_report with OAI harvest context."""
    return {
        'run_id': run_id,
        'month': 2,
        'year': 2026,
        'timestamp': datetime.now().isoformat(),
        'failed_hubs': ['indiana'],
        'failures': {
            'indiana': {
                'error': 'OAI harvest error at stage page_parse: '
                         'org.xml.sax.SAXParseException: Invalid character in entity reference',
                'error_type': 'oai_feed',
                'diagnosis': {
                    'error_type': 'oai_feed',
                    'description': 'Invalid XML in OAI response',
                    'suggested_fix': 'Check OAI feed for problematic set',
                    'context': {
                        'oai_set': 'problematic_set',
                        'resumption_token': 'abc123tokenXYZ',
                        'cursor': 500,
                        'complete_list_size': 1200,
                        'url': 'https://dpla.library.in.gov/oai?verb=ListRecords'
                               '&resumptionToken=abc123tokenXYZ',
                        'first_id': 'oai:dpla.library.in.gov:PALNI_herb-22274',
                        'last_id': 'oai:dpla.library.in.gov:PALNI_herb-22324',
                        'log_snippet': 'org.xml.sax.SAXParseException: '
                                       'Invalid character in entity reference',
                    },
                },
                'retries': 2,
            },
        },
        'summary': {'total': 1, 'failed': 1, 'complete': 0, 'skipped': 0},
    }


class TestOaiFailureSlackNotification:
    """Test that Slack notifications include OAI debug context."""

    def test_escalate_includes_oai_context_in_slack(self, notifier, mock_config):
        """Verify the Slack payload includes OAI set, token, and cursor."""
        mock_config.slack_webhook = "https://hooks.slack.com/test"
        mock_config.slack_alert_user_id = "U01234ABCD"
        failure_report = _make_oai_failure_report()

        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.escalate_failures(failure_report)

            assert mock_slack.called, "_send_slack was not called"
            payload = mock_slack.call_args[0][0]
            text = payload['text']

            # Should tag the user
            assert '<@U01234ABCD>' in text

            # Should include OAI debug context
            assert 'problematic_set' in text
            assert 'abc123tokenXYZ' in text
            assert '500' in text
            assert '1200' in text
            assert 'dpla.library.in.gov' in text

    def test_escalate_without_user_id(self, notifier, mock_config):
        """Verify no @mention when SLACK_ALERT_USER_ID is not set."""
        mock_config.slack_webhook = "https://hooks.slack.com/test"
        mock_config.slack_alert_user_id = None
        failure_report = _make_oai_failure_report()

        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.escalate_failures(failure_report)

            assert mock_slack.called, "_send_slack was not called"
            payload = mock_slack.call_args[0][0]
            text = payload['text']
            assert '<@' not in text

    def test_escalate_includes_error_summary(self, notifier, mock_config):
        """Verify the Slack payload includes the error message."""
        mock_config.slack_webhook = "https://hooks.slack.com/test"
        mock_config.slack_alert_user_id = None
        failure_report = _make_oai_failure_report()

        with patch.object(notifier, '_send_slack') as mock_slack:
            notifier.escalate_failures(failure_report)

            assert mock_slack.called, "_send_slack was not called"
            payload = mock_slack.call_args[0][0]
            text = payload['text']
            assert 'indiana' in text
            assert 'SAXParseException' in text


class TestOaiFailureMarkdown:
    """Test that markdown escalation files include OAI debug context."""

    def test_markdown_includes_oai_context(self, notifier, mock_config):
        """Verify the failure markdown has OAI debug fields."""
        failure_report = _make_oai_failure_report()

        with patch.object(notifier, '_send_slack'):
            md_file = notifier.escalate_failures(failure_report)

        content = md_file.read_text()

        assert 'OAI Debug Context' in content
        assert 'problematic_set' in content
        assert 'abc123tokenXYZ' in content
        assert '500' in content
        assert '1200' in content
        assert 'dpla.library.in.gov' in content
        assert 'PALNI_herb-22274' in content
        assert 'PALNI_herb-22324' in content

    def test_markdown_without_oai_context(self, notifier, mock_config):
        """Verify markdown still works for non-OAI failures."""
        failure_report = {
            'run_id': 'TEST_non_oai',
            'month': 2,
            'year': 2026,
            'timestamp': datetime.now().isoformat(),
            'failed_hubs': ['smithsonian'],
            'failures': {
                'smithsonian': {
                    'error': 'OutOfMemoryError',
                    'error_type': 'oom',
                    'diagnosis': {
                        'error_type': 'oom',
                        'description': 'Out of memory',
                        'suggested_fix': 'Run xmll preprocessing',
                        'context': {
                            'log_snippet': 'java.lang.OutOfMemoryError: Java heap space',
                        },
                    },
                    'retries': 0,
                },
            },
            'summary': {'total': 1, 'failed': 1, 'complete': 0, 'skipped': 0},
        }

        with patch.object(notifier, '_send_slack'):
            md_file = notifier.escalate_failures(failure_report)

        content = md_file.read_text()
        assert 'smithsonian' in content
        assert 'OutOfMemoryError' in content
        assert 'OAI Debug Context' not in content


class TestSampleOaiFailureMessages:
    """Print sample Slack and email messages for visual review (run with -s)."""

    def test_sample_slack_message(self, capsys):
        """Print a sample Slack message for OAI harvest failure."""
        # Simulate what OaiHarvestException.formatForSlack would produce
        slack_text = (
            ":x: *Harvest Failure: indiana*\n"
            "<@U01234ABCD> OAI harvest failure needs attention\n"
            "\n"
            "*OAI debug context*\n"
            "• Set: `problematic_set`\n"
            "• Resumption token: `abc123tokenXYZ`\n"
            "• Cursor: 500 / 1200\n"
            "• URL: https://dpla.library.in.gov/oai?verb=ListRecords"
            "&resumptionToken=abc123tokenXYZ\n"
            "• First ID: `oai:dpla.library.in.gov:PALNI_herb-22274`\n"
            "• Last ID: `oai:dpla.library.in.gov:PALNI_herb-22324`\n"
            "\n"
            "*Error*: SAXParseException: Invalid character in entity reference\n"
            "\n"
            "Review: `data/escalations/failures-TEST_oai_20260216.md`"
        )

        print("\n" + "=" * 60)
        print("  SAMPLE SLACK MESSAGE")
        print("=" * 60)
        print(slack_text)
        print("=" * 60)

    def test_sample_email_message(self, capsys):
        """Print a sample email body for OAI harvest failure."""
        email_body = (
            "Subject: [DPLA Ingest] Harvest failure: indiana\n"
            "\n"
            "DPLA OAI Harvest Failure\n"
            "\n"
            "Hub: indiana\n"
            "Stage: harvest\n"
            "Run ID: TEST_oai_20260216\n"
            "\n"
            "Error: OAI harvest error at stage page_parse\n"
            "\n"
            "OAI debug context:\n"
            "- Set: problematic_set\n"
            "- Resumption token: abc123tokenXYZ\n"
            "- Cursor: 500 / 1200\n"
            "- URL: https://dpla.library.in.gov/oai?verb=ListRecords"
            "&resumptionToken=abc123tokenXYZ\n"
            "- First ID: oai:dpla.library.in.gov:PALNI_herb-22274\n"
            "- Last ID: oai:dpla.library.in.gov:PALNI_herb-22324\n"
            "\n"
            "Full error: SAXParseException: Invalid character in entity reference\n"
            "\n"
            "---\n"
            "If you have questions, contact tech@dp.la."
        )

        print("\n" + "=" * 60)
        print("  SAMPLE EMAIL MESSAGE")
        print("=" * 60)
        print(email_body)
        print("=" * 60)
