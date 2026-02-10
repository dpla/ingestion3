"""Tests for configuration parsing and hub scheduling."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scheduler.orchestrator.config import Config, HubConfig, ResourceBudget, load_config


@pytest.fixture
def sample_i3_conf(tmp_path):
    """Create a sample i3.conf for testing."""
    conf_content = '''
maryland.provider = "University of Maryland"
maryland.harvest.type = "oai"
maryland.harvest.endpoint = "https://example.com/oai"
maryland.email = "contact@umd.edu"
maryland.schedule.frequency = "monthly"
maryland.schedule.months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
maryland.schedule.status = "active"

virginia.provider = "Digital Virginias"
virginia.harvest.type = "oai"
virginia.schedule.frequency = "quarterly"
virginia.schedule.months = [1, 4, 7, 10]
virginia.schedule.status = "active"

nara.provider = "National Archives"
nara.harvest.type = "file"
nara.s3_destination = "nara"
nara.schedule.frequency = "as-needed"
nara.schedule.months = []
nara.schedule.status = "active"

paused-hub.provider = "Paused Hub"
paused-hub.harvest.type = "oai"
paused-hub.schedule.frequency = "monthly"
paused-hub.schedule.months = [1, 2, 3]
paused-hub.schedule.status = "on-hold"
'''
    conf_file = tmp_path / "i3.conf"
    conf_file.write_text(conf_content)
    return conf_file


@pytest.fixture
def config(sample_i3_conf, tmp_path):
    """Create a Config instance from sample i3.conf."""
    return Config(
        i3_conf_path=sample_i3_conf,
        i3_home=tmp_path / "ingestion3",
        data_dir=tmp_path / "data",
        logs_dir=tmp_path / "logs",
        state_file=tmp_path / "logs" / "state.json",
    )


class TestI3ConfParsing:
    """Test i3.conf file parsing."""

    def test_hub_names_detected(self, config):
        """All hubs with .provider entries should be detected."""
        hubs = config.get_all_hubs()
        assert "maryland" in hubs
        assert "virginia" in hubs
        assert "nara" in hubs
        assert "paused-hub" in hubs

    def test_hub_config_fields(self, config):
        """HubConfig should have correct field values."""
        md = config.get_hub_config("maryland")
        assert md is not None
        assert md.provider == "University of Maryland"
        assert md.harvest_type == "oai"
        assert md.email == "contact@umd.edu"
        assert md.schedule_frequency == "monthly"
        assert md.schedule_status == "active"

    def test_schedule_months_parsed(self, config):
        """Schedule months should be parsed as list of ints."""
        md = config.get_hub_config("maryland")
        assert md.schedule_months == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

        va = config.get_hub_config("virginia")
        assert va.schedule_months == [1, 4, 7, 10]

    def test_file_harvest_type(self, config):
        """File-based hubs should have harvest_type='file'."""
        nara = config.get_hub_config("nara")
        assert nara.harvest_type == "file"

    def test_nonexistent_hub_returns_none(self, config):
        """get_hub_config for unknown hub should return None."""
        assert config.get_hub_config("nonexistent") is None


class TestHubScheduling:
    """Test hub scheduling/filtering by month."""

    def test_scheduled_hubs_for_january(self, config):
        """January should include maryland and virginia (not nara or paused)."""
        hubs = config.get_scheduled_hubs(month=1)
        assert "maryland" in hubs
        assert "virginia" in hubs
        assert "nara" not in hubs  # as-needed frequency
        assert "paused-hub" not in hubs  # on-hold status

    def test_scheduled_hubs_for_february(self, config):
        """February should include maryland only (virginia is quarterly)."""
        hubs = config.get_scheduled_hubs(month=2)
        assert "maryland" in hubs
        assert "virginia" not in hubs

    def test_scheduled_hubs_sorted(self, config):
        """Scheduled hubs should be sorted alphabetically."""
        hubs = config.get_scheduled_hubs(month=1)
        assert hubs == sorted(hubs)

    def test_no_hubs_for_month_13(self, config):
        """Invalid month should return empty list."""
        hubs = config.get_scheduled_hubs(month=13)
        assert hubs == []


class TestS3Config:
    """Test S3 bucket configuration."""

    def test_s3_prefix_mapping(self, config):
        """Known S3 prefix mappings should work."""
        assert config.get_s3_prefix("hathi") == "hathitrust"
        assert config.get_s3_prefix("tn") == "tennessee"

    def test_s3_prefix_default(self, config):
        """Unknown hubs should use their own name as S3 prefix."""
        assert config.get_s3_prefix("maryland") == "maryland"

    def test_s3_dest_bucket(self, config):
        """Destination bucket should be the standard one."""
        assert config.get_s3_dest_bucket() == "dpla-master-dataset"
