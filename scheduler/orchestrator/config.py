"""Configuration management for DPLA Ingest Orchestrator."""

import os
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ResourceBudget:
    """Resource allocation for a single hub when running in parallel.

    Determines Spark cores and JVM heap per hub based on total available
    resources and the number of concurrent pipelines.
    """
    memory_gb: int       # JVM -Xmx per hub (e.g. 4)
    spark_cores: int     # Spark local[N] per hub (e.g. 2)
    spark_master: str    # Full Spark master string (e.g. "local[2]")
    sbt_opts: str        # Full SBT_OPTS string (e.g. "-Xms1g -Xmx4g -XX:+UseG1GC")

    @staticmethod
    def compute(parallel: int = 1, total_memory_gb: int = 0, total_cores: int = 0) -> 'ResourceBudget':
        """Compute per-hub resource budget based on parallelism.

        Args:
            parallel: Number of concurrent hub pipelines.
            total_memory_gb: Total available memory in GB (0 = auto-detect).
            total_cores: Total available CPU cores (0 = auto-detect).

        Returns:
            ResourceBudget for a single hub.

        Resource guidelines:
            - M3 Pro 18GB:  parallel=2 → local[2], 4g each
            - 32GB machine: parallel=3 → local[2], 6g each
            - 64GB+ machine: parallel=4 → local[4], 8g each
        """
        if total_cores == 0:
            total_cores = os.cpu_count() or 4
        if total_memory_gb == 0:
            # Conservative default: assume 16GB available for JVMs
            # (leave headroom for OS/other processes)
            total_memory_gb = 16

        if parallel <= 1:
            # Single pipeline: use defaults
            return ResourceBudget(
                memory_gb=8,
                spark_cores=4,
                spark_master="local[4]",
                sbt_opts="-Xms2g -Xmx8g -XX:+UseG1GC",
            )

        # Divide resources across parallel pipelines
        cores_per_hub = max(1, total_cores // parallel)
        memory_per_hub = max(2, total_memory_gb // parallel)

        # Cap at reasonable maximums
        cores_per_hub = min(cores_per_hub, 8)
        memory_per_hub = min(memory_per_hub, 8)

        return ResourceBudget(
            memory_gb=memory_per_hub,
            spark_cores=cores_per_hub,
            spark_master=f"local[{cores_per_hub}]",
            sbt_opts=f"-Xms1g -Xmx{memory_per_hub}g -XX:+UseG1GC",
        )


@dataclass
class HubConfig:
    """Configuration for a single hub."""
    name: str
    provider: str = ""
    harvest_type: str = "oai"
    harvest_endpoint: str = ""
    s3_destination: str = ""
    email: str = ""
    schedule_frequency: str = ""
    schedule_months: list[int] = field(default_factory=list)
    schedule_status: str = "active"


@dataclass
class Config:
    """Main orchestrator configuration."""
    i3_conf_path: Path
    i3_home: Path
    data_dir: Path
    logs_dir: Path
    state_file: Path
    aws_profile: str = "dpla"
    slack_webhook: Optional[str] = None
    github_token: Optional[str] = None
    xmll_jar: Path = field(default_factory=lambda: Path.home() / "xmll-assembly-0.1.jar")

    # S3 source buckets for file-based harvests
    S3_SOURCE_BUCKETS = {
        "florida": "dpla-hub-fl",
        "smithsonian": "dpla-hub-si",
        "vt": "dpla-hub-vt",
        "ct": "dpla-hub-ct",
        "georgia": "dpla-hub-ga",
        "heartland": "dpla-hub-mo",
        "ohio": "dpla-hub-ohio",
        "txdl": "dpla-hub-tdl",
        "northwest-heritage": "dpla-hub-northwest-heritage",
        "nara": "dpla-hub-nara",
    }

    # S3 destination bucket for synced data
    S3_DEST_BUCKET = "dpla-master-dataset"

    # Hub name → S3 prefix mapping (must stay in sync with scripts/s3-sync.sh)
    S3_PREFIX_MAP = {
        "hathi": "hathitrust",
        "tn": "tennessee",
    }

    def __post_init__(self):
        self._hub_configs: dict[str, HubConfig] = {}
        self._parse_i3_conf()

    def _parse_i3_conf(self):
        """Parse i3.conf file to extract hub configurations."""
        if not self.i3_conf_path.exists():
            raise FileNotFoundError(f"i3.conf not found: {self.i3_conf_path}")

        content = self.i3_conf_path.read_text()

        # Find all hub names by looking for .provider entries
        hub_pattern = re.compile(r'^([a-z0-9-]+)\.provider\s*=', re.MULTILINE)
        hub_names = set(hub_pattern.findall(content))

        for hub_name in hub_names:
            self._hub_configs[hub_name] = self._parse_hub_config(hub_name, content)

    def _parse_hub_config(self, hub_name: str, content: str) -> HubConfig:
        """Parse configuration for a single hub."""
        def get_value(key: str, default: str = "") -> str:
            pattern = rf'^{re.escape(hub_name)}\.{re.escape(key)}\s*=\s*"([^"]*)"'
            match = re.search(pattern, content, re.MULTILINE)
            return match.group(1) if match else default

        def get_months(key: str) -> list[int]:
            pattern = rf'^{re.escape(hub_name)}\.{re.escape(key)}\s*=\s*\[([^\]]*)\]'
            match = re.search(pattern, content, re.MULTILINE)
            if match:
                months_str = match.group(1)
                return [int(m.strip()) for m in months_str.split(',') if m.strip()]
            return []

        return HubConfig(
            name=hub_name,
            provider=get_value("provider", hub_name),
            harvest_type=get_value("harvest.type", "oai"),
            harvest_endpoint=get_value("harvest.endpoint"),
            s3_destination=get_value("s3_destination"),
            email=get_value("email"),
            schedule_frequency=get_value("schedule.frequency"),
            schedule_months=get_months("schedule.months"),
            schedule_status=get_value("schedule.status", "active"),
        )

    def get_hub_config(self, hub_name: str) -> Optional[HubConfig]:
        """Get configuration for a specific hub."""
        return self._hub_configs.get(hub_name)

    def get_all_hubs(self) -> list[str]:
        """Get all configured hub names."""
        return list(self._hub_configs.keys())

    def get_scheduled_hubs(self, month: int) -> list[str]:
        """Get hubs scheduled for a specific month."""
        scheduled = []
        for hub_name, config in self._hub_configs.items():
            if (config.schedule_status == "active" and
                month in config.schedule_months and
                config.schedule_frequency not in ("as-needed", "on-hold")):
                scheduled.append(hub_name)
        return sorted(scheduled)

    def get_s3_source_bucket(self, hub_name: str) -> Optional[str]:
        """Get S3 source bucket for file-based harvests."""
        return self.S3_SOURCE_BUCKETS.get(hub_name)

    def get_s3_prefix(self, hub_name: str) -> str:
        """Get S3 prefix for a hub (mirrors scripts/s3-sync.sh mapping)."""
        return self.S3_PREFIX_MAP.get(hub_name, hub_name)

    def get_s3_dest_bucket(self) -> str:
        """Get S3 destination bucket for synced data."""
        return self.S3_DEST_BUCKET

    def update_harvest_endpoint(self, hub_name: str, new_endpoint: str):
        """Update harvest endpoint in i3.conf."""
        content = self.i3_conf_path.read_text()

        old_pattern = rf'^({re.escape(hub_name)}\.harvest\.endpoint\s*=\s*")[^"]*(")'
        new_content = re.sub(old_pattern, rf'\g<1>{new_endpoint}\g<2>', content, flags=re.MULTILINE)

        if new_content == content:
            # Entry doesn't exist, append it
            new_content = content + f'\n{hub_name}.harvest.endpoint = "{new_endpoint}"\n'

        self.i3_conf_path.write_text(new_content)

        # Update cached config
        if hub_name in self._hub_configs:
            self._hub_configs[hub_name].harvest_endpoint = new_endpoint


def load_config(
    i3_conf_path: Optional[str] = None,
    i3_home: Optional[str] = None,
    data_dir: Optional[str] = None,
) -> Config:
    """Load configuration from environment and defaults."""
    try:
        from dotenv import load_dotenv
        load_dotenv()  # Load .env from project root so SLACK_WEBHOOK etc. are set
    except ImportError:
        # python-dotenv not available - check if SLACK_WEBHOOK needs to be set
        if not os.environ.get('SLACK_WEBHOOK'):
            print("Note: python-dotenv not installed. To enable Slack notifications,")
            print("      either: pip install python-dotenv")
            print("      or:     export SLACK_WEBHOOK=your_webhook_url")
            print("      or:     source .env  (before running)")
            print()

    i3_home = Path(i3_home or os.environ.get("I3_HOME", "/Users/scott/dpla/code/ingestion3"))
    i3_conf = Path(i3_conf_path or os.environ.get("I3_CONF", "/Users/scott/dpla/code/ingestion3-conf/i3.conf"))
    data = Path(data_dir or os.environ.get("DPLA_DATA", "/Users/scott/dpla/data"))

    return Config(
        i3_conf_path=i3_conf,
        i3_home=i3_home,
        data_dir=data,
        logs_dir=i3_home / "logs",
        state_file=i3_home / "logs" / "orchestrator_state.json",
        aws_profile=os.environ.get("AWS_PROFILE", "dpla"),
        slack_webhook=os.environ.get("SLACK_WEBHOOK"),
        github_token=os.environ.get("GITHUB_TOKEN"),
    )
