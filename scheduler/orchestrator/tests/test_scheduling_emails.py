"""Tests for monthly scheduling email (scheduling_emails.py)."""

import pytest
from datetime import date
from pathlib import Path

from scheduler.orchestrator.scheduling_emails import (
    build_preview_and_payload,
    format_date_range,
    last_calendar_week_range,
    CC_ALWAYS,
)


class TestLastCalendarWeek:
    """Test last calendar week date range computation."""

    def test_february_2026(self):
        start, end = last_calendar_week_range(2026, 2)
        assert start == date(2026, 2, 23)  # Monday
        assert end == date(2026, 2, 28)  # Last day (Saturday)

    def test_january_2026(self):
        start, end = last_calendar_week_range(2026, 1)
        assert end == date(2026, 1, 31)
        assert start.weekday() == 0  # Monday
        assert start <= end

    def test_december_2025(self):
        start, end = last_calendar_week_range(2025, 12)
        assert end == date(2025, 12, 31)
        assert start == date(2025, 12, 29)  # Monday


class TestFormatDateRange:
    """Test date range formatting."""

    def test_february_range(self):
        start = date(2026, 2, 23)
        end = date(2026, 2, 28)
        s = format_date_range(start, end)
        assert "February" in s
        assert "23" in s
        assert "28" in s
        assert "2026" in s


@pytest.fixture
def scheduling_i3_conf(tmp_path):
    """Minimal i3.conf for scheduling email tests: two hubs for Feb, one with email one without."""
    conf = tmp_path / "i3.conf"
    conf.write_text('''
hub-a.provider = "Hub A Display"
hub-a.harvest.type = "oai"
hub-a.email = "contact-a@example.com"
hub-a.schedule.frequency = "monthly"
hub-a.schedule.months = [2, 5, 8, 11]
hub-a.schedule.status = "active"

hub-b.provider = "Hub B Display"
hub-b.harvest.type = "oai"
hub-b.schedule.frequency = "quarterly"
hub-b.schedule.months = [2, 5, 8, 11]
hub-b.schedule.status = "active"

hub-c.provider = "Hub C"
hub-c.harvest.type = "oai"
hub-c.email = "contact-c@example.com"
hub-c.schedule.frequency = "monthly"
hub-c.schedule.months = [1, 3, 4]
hub-c.schedule.status = "active"

empty-schedule.provider = "Empty Schedule"
empty-schedule.schedule.frequency = "monthly"
empty-schedule.schedule.months = []
empty-schedule.schedule.status = "active"
''')
    return conf


class TestBuildPreviewAndPayload:
    """Test build_preview_and_payload with i3.conf fixture."""

    def test_february_hubs_and_emails(self, scheduling_i3_conf):
        preview_lines, hub_entries, to_emails, subject, body = build_preview_and_payload(
            scheduling_i3_conf, month=2, year=2026
        )
        # hub-a and hub-b are scheduled for Feb; hub-c is not (only 1,3,4); empty-schedule has []
        assert len(hub_entries) == 2
        displays = [h[0] for h in hub_entries]
        assert any("hub-a" in d for d in displays)
        assert any("hub-b" in d for d in displays)
        # hub-a has email, hub-b has no email
        assert "contact-a@example.com" in to_emails
        assert len(to_emails) == 1
        # Preview shows hub-b with (no email...) note
        assert any("no email" in line for line in preview_lines)
        assert "Date range" in preview_lines[2]
        assert "February" in preview_lines[2]
        assert "2026" in preview_lines[2]

    def test_subject_and_body_contain_month_and_last_week(self, scheduling_i3_conf):
        _, _, _, subject, body = build_preview_and_payload(
            scheduling_i3_conf, month=2, year=2026
        )
        assert "February" in subject
        assert "2026" in subject
        assert "last calendar week" in body or "last week" in body.lower()
        assert "tech@dp.la" in body
        assert "Hub A" in body or "hub-a" in body
        assert "Hub B" in body or "hub-b" in body

    def test_january_only_hub_c(self, scheduling_i3_conf):
        preview_lines, hub_entries, to_emails, subject, body = build_preview_and_payload(
            scheduling_i3_conf, month=1, year=2026
        )
        # Only hub-c has month 1
        assert len(hub_entries) == 1
        assert "hub-c" in hub_entries[0][0] or "Hub C" in hub_entries[0][0]
        assert "contact-c@example.com" in to_emails

    def test_cc_constant(self):
        assert "ingest@dp.la" in CC_ALWAYS
        assert "dominic@dp.la" in CC_ALWAYS
