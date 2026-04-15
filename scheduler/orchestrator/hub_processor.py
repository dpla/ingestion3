"""Hub processing logic for individual hub ingestion."""

import asyncio
import os
import re
import shlex
import time
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from .config import Config, ResourceBudget
from .state import IngestState, HubStatus
from .diagnostics import ErrorClassifier
from .anomaly_detector import AnomalyDetector, AnomalyReport
from .s3_utils import get_latest_dir


@dataclass
class ProcessResult:
    """Result of a process execution."""

    success: bool
    exit_code: int
    output: str
    duration_seconds: int
    error: Optional[str] = None
    anomaly_report: Optional[AnomalyReport] = None
    failure_stage: Optional[str] = (
        None  # harvest, mapping, enrichment, jsonl, sync, anomaly
    )


@dataclass
class HarvestCounts:
    """Counts extracted from a harvest _MANIFEST."""

    record_count: Optional[int] = None


@dataclass
class MappingCounts:
    """Counts extracted from a mapping _SUMMARY."""

    attempted: Optional[int] = None
    successful: Optional[int] = None
    failed: Optional[int] = None
    issues_summary: Optional[str] = None


class HubProcessor:
    """Handles processing for a single hub."""

    def __init__(
        self,
        hub_name: str,
        config: Config,
        state: IngestState,
        run_id: str,
        resource_budget: Optional[ResourceBudget] = None,
    ):
        self.hub_name = hub_name
        self.config = config
        self.state = state
        self.run_id = run_id
        self.hub_config = config.get_hub_config(hub_name)
        self.classifier = ErrorClassifier()
        self._logs: list[str] = []
        self.resource_budget = resource_budget or ResourceBudget.compute(parallel=1)

    @property
    def data_dir(self) -> Path:
        return self.config.data_dir / self.hub_name

    @property
    def harvest_dir(self) -> Path:
        return self.data_dir / "harvest"

    @property
    def mapping_dir(self) -> Path:
        return self.data_dir / "mapping"

    @property
    def enrichment_dir(self) -> Path:
        return self.data_dir / "enrichment"

    @property
    def jsonl_dir(self) -> Path:
        return self.data_dir / "jsonl"

    def get_logs(self) -> str:
        """Get accumulated logs."""
        return "\n".join(self._logs)

    def _log(self, message: str):
        """Add to log buffer."""
        self._logs.append(f"[{time.strftime('%H:%M:%S')}] {message}")
        print(f"  [{self.hub_name}] {message}")

    # =========================================================================
    # Pipeline Steps
    # =========================================================================

    async def prepare(self) -> bool:
        """Prepare for harvest - download S3 data if needed."""
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.PREPARING)

        if self.hub_config.harvest_type != "file":
            self._log("OAI/API harvest - no S3 download needed")
            return True

        source_bucket = self.config.get_s3_source_bucket(self.hub_name)
        if not source_bucket:
            self._log("No S3 source bucket configured")
            return True

        self._log(f"Downloading from s3://{source_bucket}/")

        # Download S3 data
        result = await self._run_command(
            f"aws s3 ls s3://{shlex.quote(source_bucket)}/ --profile dpla"
        )

        if not result.success:
            self._log(f"Failed to list S3 bucket: {result.error}")
            return False

        # Find latest data and download
        # This is simplified - the actual download logic is in the bash script
        download_script = (
            f'bash -c "cd {shlex.quote(self.config.i3_home)}/scripts'
            f" && source ./auto-ingest.sh"
            f' && download_s3_data {shlex.quote(self.hub_name)}"'
        )

        result = await self._run_command(download_script, shell=True)

        if not result.success:
            self._log(f"S3 download failed: {result.error}")
            return False

        # Special handling for Smithsonian
        if self.hub_name == "smithsonian":
            return await self._preprocess_smithsonian()

        return True

    async def _preprocess_smithsonian(self) -> bool:
        """Run xmll preprocessing for Smithsonian files."""
        self._log("Running Smithsonian xmll preprocessing...")

        if not self.config.xmll_jar.exists():
            self._log(f"xmll jar not found: {self.config.xmll_jar}")
            return False

        original_dir = self.data_dir / "originalRecords"

        # Find the latest data directory
        subdirs = [
            d
            for d in original_dir.iterdir()
            if d.is_dir() and d.name not in ("fixed", "xmll")
        ]
        if not subdirs:
            self._log("No Smithsonian data directories found")
            return False

        data_dir = max(subdirs, key=lambda d: d.stat().st_mtime)
        fixed_dir = data_dir / "fixed"
        xmll_dir = data_dir / "xmll"

        fixed_dir.mkdir(exist_ok=True)
        xmll_dir.mkdir(exist_ok=True)

        # Recompress gzip files
        self._log("Recompressing gzip files...")
        for gz_file in data_dir.glob("*.xml.gz"):
            fixed_file = fixed_dir / gz_file.name
            if not fixed_file.exists():
                result = await self._run_command(
                    f"gunzip -dck '{gz_file}' | gzip > '{fixed_file}'"
                )
                if not result.success:
                    self._log(f"Failed to recompress {gz_file.name}")

        # Run xmll on fixed files
        self._log("Running xmll shredder...")
        for fixed_file in fixed_dir.glob("*.xml.gz"):
            xmll_file = xmll_dir / fixed_file.name
            if not xmll_file.exists():
                self._log(f"Processing {fixed_file.name}...")
                result = await self._run_command(
                    f"java -Xmx4g -jar {self.config.xmll_jar} doc '{fixed_file}' '{xmll_file}'"
                )
                if not result.success:
                    self._log(f"xmll failed for {fixed_file.name}: {result.error}")

        # Update i3.conf endpoint
        new_endpoint = str(xmll_dir) + "/"
        self.config.update_harvest_endpoint("smithsonian", new_endpoint)
        self._log(f"Updated harvest endpoint to: {new_endpoint}")

        return True

    async def harvest(self, max_retries: int = 5) -> ProcessResult:
        """Run harvest with retries."""
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.HARVESTING)

        for attempt in range(1, max_retries + 1):
            self._log(f"Harvest attempt {attempt}/{max_retries}")

            result = await self._run_command(
                f"cd {shlex.quote(self.config.i3_home)} && ./scripts/harvest.sh {shlex.quote(self.hub_name)}"
            )

            # Check for actual output (more reliable than exit code)
            has_output = self._verify_harvest_output()

            if has_output:
                self._log("Harvest successful (verified output exists)")
                self.state.update_hub(
                    self.run_id, self.hub_name, HubStatus.HARVESTING, retries=attempt
                )
                return ProcessResult(
                    success=True,
                    exit_code=result.exit_code,
                    output=result.output,
                    duration_seconds=result.duration_seconds,
                )

            # Classify the failure
            diagnosis = self.classifier.classify_from_exit_code(
                self.hub_name, result.exit_code, has_output, result.output
            )

            if diagnosis and diagnosis.can_auto_retry:
                self._log(f"Retryable error: {diagnosis.error_type.value}")
                if attempt < max_retries:
                    wait_time = min(30 * attempt, 120)  # Exponential backoff, max 2 min
                    self._log(f"Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                    continue

            # Non-retryable or max retries exceeded
            self._log(f"Harvest failed after {attempt} attempts")
            return ProcessResult(
                success=False,
                exit_code=result.exit_code,
                output=result.output,
                duration_seconds=result.duration_seconds,
                error=diagnosis.description if diagnosis else "Unknown error",
                failure_stage="harvest",
            )

        return ProcessResult(
            success=False,
            exit_code=1,
            output=self.get_logs(),
            duration_seconds=0,
            error=f"Failed after {max_retries} attempts",
            failure_stage="harvest",
        )

    async def mapping(self) -> ProcessResult:
        """Run mapping step (harvest → DPLA MAP)."""
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.MAPPING)
        self._log("Running mapping (harvest → DPLA MAP)...")

        result = await self._run_command(
            f"cd {shlex.quote(self.config.i3_home)} && ./scripts/mapping.sh {shlex.quote(self.hub_name)}"
        )

        has_output = self._verify_step_output(self.mapping_dir)

        if has_output:
            self._log("Mapping successful (verified output exists)")
            return ProcessResult(
                success=True,
                exit_code=result.exit_code,
                output=result.output,
                duration_seconds=result.duration_seconds,
            )

        self._log(f"Mapping failed (exit code: {result.exit_code})")
        return ProcessResult(
            success=False,
            exit_code=result.exit_code,
            output=result.output,
            duration_seconds=result.duration_seconds,
            error="Mapping did not produce output",
            failure_stage="mapping",
        )

    async def enrich(self) -> ProcessResult:
        """Run enrichment step (DPLA MAP → enriched records)."""
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.ENRICHING)
        self._log("Running enrichment...")

        result = await self._run_command(
            f"cd {shlex.quote(self.config.i3_home)} && ./scripts/enrich.sh {shlex.quote(self.hub_name)}"
        )

        has_output = self._verify_step_output(self.enrichment_dir)

        if has_output:
            self._log("Enrichment successful (verified output exists)")
            return ProcessResult(
                success=True,
                exit_code=result.exit_code,
                output=result.output,
                duration_seconds=result.duration_seconds,
            )

        self._log(f"Enrichment failed (exit code: {result.exit_code})")
        return ProcessResult(
            success=False,
            exit_code=result.exit_code,
            output=result.output,
            duration_seconds=result.duration_seconds,
            error="Enrichment did not produce output",
            failure_stage="enrichment",
        )

    async def jsonl(self) -> ProcessResult:
        """Run JSONL export step (enriched → gzipped JSONL)."""
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.JSONL_EXPORT)
        self._log("Running JSONL export...")

        result = await self._run_command(
            f"cd {shlex.quote(self.config.i3_home)} && ./scripts/jsonl.sh {shlex.quote(self.hub_name)}"
        )

        has_output = self._verify_step_output(self.jsonl_dir)

        if has_output:
            self._log("JSONL export successful (verified output exists)")
            return ProcessResult(
                success=True,
                exit_code=result.exit_code,
                output=result.output,
                duration_seconds=result.duration_seconds,
            )

        self._log(f"JSONL export failed (exit code: {result.exit_code})")
        return ProcessResult(
            success=False,
            exit_code=result.exit_code,
            output=result.output,
            duration_seconds=result.duration_seconds,
            error="JSONL export did not produce output",
            failure_stage="jsonl",
        )

    async def remap(self) -> ProcessResult:
        """Run remap (mapping + enrichment + jsonl) as a single step.

        Kept for backwards compatibility. Prefer calling mapping(), enrich(),
        jsonl() individually when per-stage notifications are desired.
        """
        self.state.update_hub(self.run_id, self.hub_name, HubStatus.REMAPPING)
        self._log("Running remap (mapping → enrichment → jsonl)...")

        result = await self._run_command(
            f"cd {shlex.quote(self.config.i3_home)} && ./scripts/remap.sh {shlex.quote(self.hub_name)}"
        )

        # Check for jsonl output
        has_output = self._verify_step_output(self.jsonl_dir)

        if has_output:
            self._log("Remap successful (verified jsonl output exists)")
            return ProcessResult(
                success=True,
                exit_code=result.exit_code,
                output=result.output,
                duration_seconds=result.duration_seconds,
            )

        self._log(f"Remap failed (exit code: {result.exit_code})")
        return ProcessResult(
            success=False,
            exit_code=result.exit_code,
            output=result.output,
            duration_seconds=result.duration_seconds,
            error="Remap did not produce jsonl output",
            failure_stage="mapping",
        )

    def check_anomalies(self, force: bool = False) -> tuple[bool, AnomalyReport]:
        """
        Check for anomalies before S3 sync.

        Returns:
            (safe_to_proceed, report)
        """
        self._log("Checking for anomalies against S3 baseline...")

        detector = AnomalyDetector(
            data_dir=str(self.config.data_dir), aws_profile=self.config.aws_profile
        )

        report = detector.check_hub(self.hub_name, verbose=False)

        if report.anomalies:
            self._log(f"Anomalies detected: {len(report.anomalies)}")
            for anomaly in report.anomalies:
                self._log(f"  [{anomaly.severity.upper()}] {anomaly.message}")
        else:
            self._log("No anomalies detected")

        if report.should_halt and not force:
            self._log("⛔ HARD STOP: Critical anomalies detected")
            return False, report

        return True, report

    async def sync_to_s3(self, force: bool = False) -> ProcessResult:
        """Sync results to S3 with anomaly detection.

        Args:
            force: If True, skip anomaly check and sync anyway

        Returns:
            ProcessResult with anomaly_report attached (check report.anomalies
            to send notifications, even if sync succeeded with warnings)
        """
        # Check for anomalies before syncing
        safe, anomaly_report = self.check_anomalies(force=force)

        if not safe:
            self._log("Sync aborted due to anomalies. Use --force to override.")
            return ProcessResult(
                success=False,
                exit_code=1,
                output=anomaly_report.format_report(),
                duration_seconds=0,
                error="Anomaly detection halted sync",
                anomaly_report=anomaly_report,
                failure_stage="anomaly",
            )

        self.state.update_hub(self.run_id, self.hub_name, HubStatus.SYNCING)
        self._log("Syncing to S3...")

        result = await self._run_command(
            f"cd {shlex.quote(self.config.i3_home)} && ./scripts/s3-sync.sh {shlex.quote(self.hub_name)}"
        )

        if result.exit_code == 0:
            self._log("S3 sync complete")
        else:
            self._log(f"S3 sync failed (exit code: {result.exit_code})")
            result.failure_stage = "sync"

        # Attach anomaly report (may have warnings even if sync succeeded)
        result.anomaly_report = anomaly_report

        return result

    async def send_notification(self) -> bool:
        """Send email notification (placeholder)."""
        self._log("Email notification would be sent here")
        return True

    # =========================================================================
    # Count Readers
    # =========================================================================

    def get_harvest_counts(self) -> HarvestCounts:
        """Read harvest record count from the latest _MANIFEST file."""
        counts = HarvestCounts()

        latest = get_latest_dir(self.harvest_dir)
        if not latest:
            return counts

        manifest = latest / "_MANIFEST"
        if manifest.exists():
            try:
                content = manifest.read_text()
                match = re.search(r"Record count:\s*([0-9,]+)", content)
                if match:
                    counts.record_count = int(match.group(1).replace(",", ""))
            except Exception:
                pass

        return counts

    def get_mapping_counts(self) -> MappingCounts:
        """Read mapping counts from the latest _SUMMARY file."""
        counts = MappingCounts()

        latest = get_latest_dir(self.mapping_dir)
        if not latest:
            return counts

        summary = latest / "_SUMMARY"
        if not summary.exists():
            return counts

        try:
            content = summary.read_text()

            attempted = re.search(r"Attempted\.+([0-9,]+)", content)
            successful = re.search(r"Successful\.+([0-9,]+)", content)
            failed = re.search(r"Failed\.+([0-9,]+)", content)

            if attempted:
                counts.attempted = int(attempted.group(1).replace(",", ""))
            if successful:
                counts.successful = int(successful.group(1).replace(",", ""))
            if failed:
                counts.failed = int(failed.group(1).replace(",", ""))

            # Issues summary
            records_section = re.search(
                r"Records\n-\s*Errors\.+([0-9,]+)\n-\s*Warnings\.+([0-9,]+)", content
            )
            if records_section:
                errors_val = int(records_section.group(1).replace(",", ""))
                warnings_val = int(records_section.group(2).replace(",", ""))
                parts = []
                if errors_val > 0:
                    parts.append(f"{errors_val:,} errors")
                if warnings_val > 0:
                    parts.append(f"{warnings_val:,} warnings")
                if parts:
                    counts.issues_summary = ", ".join(parts)

        except Exception:
            pass

        return counts

    # =========================================================================
    # Output Verification
    # =========================================================================

    def _verify_harvest_output(self) -> bool:
        """Check if harvest produced output."""
        return self._verify_step_output(self.harvest_dir)

    def _verify_step_output(self, step_dir: Path) -> bool:
        """Check if a pipeline step produced output in its directory.

        Looks for timestamped subdirectories containing a _SUCCESS marker.
        Falls back to checking for .avro or .jsonl directories.
        """
        if not step_dir.exists():
            return False

        # Check for timestamped dirs with _SUCCESS marker
        for subdir in sorted(step_dir.iterdir(), reverse=True):
            if subdir.is_dir() and (subdir / "_SUCCESS").exists():
                return True

        # Fallback: check for .avro or .jsonl dirs (legacy naming)
        avro_dirs = list(step_dir.glob("*.avro"))
        jsonl_dirs = list(step_dir.glob("*.jsonl"))
        return len(avro_dirs) > 0 or len(jsonl_dirs) > 0

    # =========================================================================
    # Internals
    # =========================================================================

    def _get_env(self) -> dict[str, str]:
        """Build environment variables with resource-budgeted Spark/JVM settings."""
        env = os.environ.copy()
        budget = self.resource_budget
        env["SPARK_MASTER"] = budget.spark_master
        env["SBT_OPTS"] = budget.sbt_opts
        return env

    async def _run_command(
        self,
        command: str,
        shell: bool = True,
        health_check_interval: int = 300,
    ) -> ProcessResult:
        """Run a shell command with resource-budgeted env, streaming output
        and periodic health-check log lines.

        Liveness is determined by **output activity**: as long as the child
        process produces stdout/stderr (even one line every few minutes), it is
        considered healthy.  No automatic kill is performed — long-running
        harvests (12-24 h) are expected.  Health-check lines are logged so
        operators can see the process is still alive.

        Args:
            command: Shell command to execute.
            shell: Use shell execution (default True).
            health_check_interval: Seconds between health-check log lines
                (default 5 min).
        """
        start_time = time.time()
        env = self._get_env()
        output_chunks: list[str] = []
        last_output_time = start_time

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )

            pid = process.pid

            async def _read_output():
                """Read output line-by-line so the pipe doesn't block."""
                nonlocal last_output_time
                assert process.stdout is not None
                while True:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    decoded = line.decode("utf-8", errors="replace")
                    output_chunks.append(decoded)
                    last_output_time = time.time()
                    # Keep buffer bounded (last ~200KB)
                    while sum(len(c) for c in output_chunks) > 200_000:
                        output_chunks.pop(0)

            reader_task = asyncio.create_task(_read_output())

            # Monitor process health while waiting for completion
            while True:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(reader_task),
                        timeout=health_check_interval,
                    )
                    break  # reader finished → process done
                except asyncio.TimeoutError:
                    pass  # Time for a health-check log line

                now = time.time()
                elapsed = int(now - start_time)
                output_age = int(now - last_output_time)
                child_pid = self._find_java_child(pid)

                if child_pid:
                    mem_mb = self._get_rss_mb(child_pid)
                    self._log(
                        f"Health: {_fmt_duration(elapsed)} elapsed, "
                        f"PID {child_pid}, mem={mem_mb}MB, "
                        f"last output {output_age}s ago"
                    )
                else:
                    self._log(
                        f"Health: {_fmt_duration(elapsed)} elapsed, "
                        f"last output {output_age}s ago"
                    )

            await process.wait()
            if not reader_task.done():
                await reader_task

            output = "".join(output_chunks)
            duration = int(time.time() - start_time)

            self._logs.append(output[-5000:])

            return ProcessResult(
                success=(process.returncode == 0),
                exit_code=process.returncode or 0,
                output=output,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = int(time.time() - start_time)
            return ProcessResult(
                success=False,
                exit_code=1,
                output=str(e),
                duration_seconds=duration,
                error=str(e),
            )

    # =========================================================================
    # Process health helpers
    # =========================================================================

    @staticmethod
    def _find_java_child(parent_pid: int) -> int | None:
        """Find a child java process of the given parent."""
        import subprocess as _sp

        try:
            out = _sp.check_output(
                ["pgrep", "-P", str(parent_pid), "-f", "java"],
                text=True,
                stderr=_sp.DEVNULL,
            )
            pids = out.strip().split()
            return int(pids[0]) if pids else None
        except Exception:
            return None

    @staticmethod
    def _get_rss_mb(pid: int) -> int:
        """Return resident memory in MB for a process."""
        import subprocess as _sp

        try:
            out = _sp.check_output(
                ["ps", "-o", "rss=", "-p", str(pid)],
                text=True,
                stderr=_sp.DEVNULL,
            )
            return int(out.strip()) // 1024
        except Exception:
            return 0


def _fmt_duration(seconds: int) -> str:
    """Format seconds as a compact human-readable string."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m:02d}m"
