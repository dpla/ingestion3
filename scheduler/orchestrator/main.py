#!/usr/bin/env python3
"""
DPLA Monthly Ingest Orchestrator

Automated pipeline for DPLA hub ingestion with intelligent error handling,
retry logic, and escalation for failures.

Usage:
    python -m scheduler.orchestrator.main                    # Current month
    python -m scheduler.orchestrator.main --month=2          # February
    python -m scheduler.orchestrator.main --hub=maryland     # Single hub
    python -m scheduler.orchestrator.main --retry-failed     # Retry previous failures
    python -m scheduler.orchestrator.main --dry-run          # Preview only
    python -m scheduler.orchestrator.main --parallel=3       # Run 3 concurrent processes
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

from .config import Config, ResourceBudget, load_config
from .state import IngestState, HubStatus
from .hub_processor import HubProcessor
from .notifications import Notifier, format_hub_status
from .diagnostics import ErrorClassifier
from .status import get_status_table, get_status_json


class IngestOrchestrator:
    """Main orchestrator for DPLA hub ingestion."""

    def __init__(self, config: Config, parallel: int = 1):
        self.config = config
        self.state = IngestState(config.state_file)
        self.notifier = Notifier(config)
        self.classifier = ErrorClassifier()
        self.parallel = parallel  # Max concurrent sbt processes
        self._sbt_semaphore = asyncio.Semaphore(parallel) if parallel > 1 else None
        self.resource_budget = ResourceBudget.compute(parallel=parallel)

    async def run_monthly_ingest(
        self,
        month: int,
        hubs: list[str] | None = None,
        skip_harvest: bool = False,
        skip_s3_sync: bool = False,
    ) -> dict:
        """Run the monthly ingest for all scheduled hubs."""

        # Determine which hubs to process
        if hubs is None:
            hubs = self.config.get_scheduled_hubs(month)

        if not hubs:
            print(f"No hubs scheduled for month {month}")
            return {'status': 'no_hubs', 'month': month}

        # Start the run
        run_id = self.state.start_run(month, hubs)
        self.notifier.send_start_notification(run_id, hubs)

        # Separate hubs by type for optimized processing
        file_hubs = [h for h in hubs if self.config.get_hub_config(h).harvest_type == "file"]
        api_hubs = [h for h in hubs if self.config.get_hub_config(h).harvest_type != "file"]

        budget = self.resource_budget
        print(f"\nProcessing {len(hubs)} hubs (parallel={self.parallel}):")
        print(f"  File-based: {', '.join(file_hubs) if file_hubs else 'none'}")
        print(f"  OAI/API: {', '.join(api_hubs) if api_hubs else 'none'}")
        print(f"  Resources per hub: {budget.spark_master}, {budget.memory_gb}g heap")
        print()

        results = {}

        if self.parallel > 1:
            # === PARALLEL PROCESSING ===
            results = await self._run_parallel(
                hubs, run_id, skip_harvest, skip_s3_sync, file_hubs
            )
        else:
            # === SEQUENTIAL PROCESSING ===
            for hub in hubs:
                print(f"\n{'='*60}")
                print(f"  Processing: {hub}")
                print(f"{'='*60}")

                result = await self.process_hub(hub, run_id, skip_harvest, skip_s3_sync)
                results[hub] = result

                status = "✅ Complete" if result['success'] else f"❌ Failed: {result.get('error', 'Unknown')}"
                print(f"\n  {hub}: {status}")

                if hub != hubs[-1]:
                    print("  Waiting 5s before next hub...")
                    await asyncio.sleep(5)

        # Complete the run
        failed_hubs = [h for h, r in results.items() if not r['success']]
        status = "complete" if not failed_hubs else "partial"
        self.state.complete_run(run_id, status)

        # Generate summary
        summary = self.state.get_summary(run_id)

        # Handle failures
        if failed_hubs:
            failure_report = self.state.get_failure_report(run_id)
            self.notifier.escalate_failures(failure_report)

        self.notifier.send_completion_notification(run_id, summary)

        return summary

    async def _run_parallel(
        self,
        hubs: list[str],
        run_id: str,
        skip_harvest: bool,
        skip_s3_sync: bool,
        file_hubs: list[str],
    ) -> dict:
        """Run hubs in parallel with phased approach."""
        results = {}
        completed_hubs = []

        # === PHASE 1: Parallel S3 downloads for file-based hubs ===
        if file_hubs and not skip_harvest:
            print(f"\n{'='*60}")
            print(f"  Phase 1: Preparing file-based hubs (parallel)")
            print(f"{'='*60}")

            async def prepare_hub(hub: str):
                processor = HubProcessor(
                    hub, self.config, self.state, run_id,
                    resource_budget=self.resource_budget,
                )
                print(f"  [{hub}] Downloading S3 data...")
                success = await processor.prepare()
                return hub, success

            prep_tasks = [prepare_hub(h) for h in file_hubs]
            prep_results = await asyncio.gather(*prep_tasks, return_exceptions=True)

            for result in prep_results:
                if isinstance(result, Exception):
                    print(f"  Preparation error: {result}")
                else:
                    hub, success = result
                    print(f"  [{hub}] Preparation: {'✅' if success else '❌'}")

        # === PHASE 2: Parallel harvest + remap with semaphore ===
        print(f"\n{'='*60}")
        print(f"  Phase 2: Harvest & Remap ({self.parallel} concurrent)")
        print(f"{'='*60}")

        async def process_with_semaphore(hub: str) -> tuple[str, dict]:
            """Process a hub with semaphore-limited concurrency."""
            async with self._sbt_semaphore:
                print(f"\n  [{hub}] Starting (slot acquired)...")
                result = await self.process_hub(
                    hub, run_id,
                    skip_harvest=skip_harvest,
                    skip_s3_sync=True  # We'll do S3 sync in Phase 3
                )
                status = "✅" if result['success'] else "❌"
                print(f"  [{hub}] Complete {status}")
                return hub, result

        # Process all hubs with limited concurrency
        tasks = [process_with_semaphore(h) for h in hubs]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in task_results:
            if isinstance(result, Exception):
                print(f"  Processing error: {result}")
            else:
                hub, hub_result = result
                results[hub] = hub_result
                if hub_result['success']:
                    completed_hubs.append(hub)

        # === PHASE 3: Parallel S3 uploads ===
        if not skip_s3_sync and completed_hubs:
            print(f"\n{'='*60}")
            print(f"  Phase 3: S3 Sync ({len(completed_hubs)} hubs, parallel)")
            print(f"{'='*60}")

            async def sync_hub(hub: str):
                processor = HubProcessor(
                    hub, self.config, self.state, run_id,
                    resource_budget=self.resource_budget,
                )
                print(f"  [{hub}] Syncing to S3...")
                result = await processor.sync_to_s3()
                # Send anomaly alert if anomalies were detected
                if result.anomaly_report and result.anomaly_report.anomalies:
                    self.notifier.send_anomaly_alert(hub, result.anomaly_report)
                # Notify: sync complete
                if result.success:
                    self.notifier.send_sync_complete(
                        run_id, hub,
                        duration_seconds=result.duration_seconds,
                    )
                return hub, result

            sync_tasks = [sync_hub(h) for h in completed_hubs]
            sync_results = await asyncio.gather(*sync_tasks, return_exceptions=True)

            for result in sync_results:
                if isinstance(result, Exception):
                    print(f"  Sync error: {result}")
                else:
                    hub, sync_result = result
                    print(f"  [{hub}] S3 Sync: {'✅' if sync_result.success else '❌'}")
                    # If sync failed due to anomaly, mark the hub as failed
                    if not sync_result.success and sync_result.failure_stage == 'anomaly':
                        results[hub] = {
                            'success': False,
                            'hub': hub,
                            'error': sync_result.error,
                            'failure_stage': 'anomaly'
                        }
                        self.state.update_hub(
                            run_id, hub, HubStatus.FAILED,
                            error=sync_result.error,
                            failure_stage='anomaly'
                        )
                        if hub in completed_hubs:
                            completed_hubs.remove(hub)

        return results

    async def process_hub(
        self,
        hub: str,
        run_id: str,
        skip_harvest: bool = False,
        skip_s3_sync: bool = False,
    ) -> dict:
        """Process a single hub through the full pipeline with per-stage notifications."""

        processor = HubProcessor(
            hub, self.config, self.state, run_id,
            resource_budget=self.resource_budget,
        )

        try:
            # Step 1: Prepare (download S3 data if needed)
            if not skip_harvest:
                print(f"  [1/6] Preparing...")
                if not await processor.prepare():
                    raise Exception("Preparation failed")

            # Step 2: Harvest
            if not skip_harvest:
                print(f"  [2/6] Harvesting...")
                harvest_result = await processor.harvest(max_retries=5)
                if not harvest_result.success:
                    raise Exception(f"Harvest failed: {harvest_result.error}")
                # Notify: harvest complete
                harvest_counts = processor.get_harvest_counts()
                self.notifier.send_harvest_complete(
                    run_id, hub,
                    record_count=harvest_counts.record_count,
                    duration_seconds=harvest_result.duration_seconds,
                )
            else:
                print(f"  [2/6] Skipping harvest (--skip-harvest)")

            # Step 3: Mapping
            print(f"  [3/6] Mapping...")
            mapping_result = await processor.mapping()
            if not mapping_result.success:
                raise Exception(f"Mapping failed: {mapping_result.error}")
            # Notify: mapping complete
            mapping_counts = processor.get_mapping_counts()
            self.notifier.send_mapping_complete(
                run_id, hub,
                attempted=mapping_counts.attempted,
                successful=mapping_counts.successful,
                failed=mapping_counts.failed,
                duration_seconds=mapping_result.duration_seconds,
            )

            # Step 4: Enrichment
            print(f"  [4/6] Enriching...")
            enrich_result = await processor.enrich()
            if not enrich_result.success:
                raise Exception(f"Enrichment failed: {enrich_result.error}")
            # Notify: enrichment complete
            self.notifier.send_enrichment_complete(
                run_id, hub,
                duration_seconds=enrich_result.duration_seconds,
            )

            # Step 5: JSONL export
            print(f"  [5/6] Exporting JSONL...")
            jsonl_result = await processor.jsonl()
            if not jsonl_result.success:
                raise Exception(f"JSONL export failed: {jsonl_result.error}")
            # Notify: JSONL complete
            self.notifier.send_jsonl_complete(
                run_id, hub,
                duration_seconds=jsonl_result.duration_seconds,
            )

            # Step 6: S3 Sync
            if not skip_s3_sync:
                print(f"  [6/6] Syncing to S3...")
                sync_result = await processor.sync_to_s3()
                # Send anomaly alert if anomalies were detected
                if sync_result.anomaly_report and sync_result.anomaly_report.anomalies:
                    self.notifier.send_anomaly_alert(hub, sync_result.anomaly_report)
                if not sync_result.success:
                    raise Exception(f"S3 sync failed: {sync_result.error}")
                # Notify: sync complete
                self.notifier.send_sync_complete(
                    run_id, hub,
                    duration_seconds=sync_result.duration_seconds,
                )
            else:
                print(f"  [6/6] Skipping S3 sync (--skip-s3-sync)")

            # Mark complete
            self.state.update_hub(run_id, hub, HubStatus.COMPLETE)

            return {'success': True, 'hub': hub}

        except Exception as e:
            # Classify and record the error
            diagnosis = self.classifier.classify(
                hub, e, processor.get_logs(),
                self.config.get_hub_config(hub).harvest_type
            )

            # Infer failure stage from error message
            error_str = str(e).lower()
            if 'harvest' in error_str:
                failure_stage = 'harvest'
            elif 'mapping' in error_str:
                failure_stage = 'mapping'
            elif 'enrichment' in error_str:
                failure_stage = 'enrichment'
            elif 'jsonl' in error_str:
                failure_stage = 'jsonl'
            elif 'sync' in error_str or 's3' in error_str:
                failure_stage = 'sync'
            elif 'anomaly' in error_str:
                failure_stage = 'anomaly'
            else:
                failure_stage = 'unknown'

            self.state.update_hub(
                run_id, hub, HubStatus.FAILED,
                error=str(e),
                error_type=diagnosis.error_type.value,
                diagnosis=diagnosis.to_dict(),
                failure_stage=failure_stage
            )

            return {
                'success': False,
                'hub': hub,
                'error': str(e),
                'diagnosis': diagnosis.to_dict(),
                'failure_stage': failure_stage
            }

    async def retry_failed(self, run_id: str | None = None) -> dict:
        """Retry failed hubs from a previous run."""

        if run_id is None:
            # Get the most recent run
            run = self.state.get_latest_run()
            if not run:
                print("No previous runs found")
                return {'status': 'no_runs'}
            run_id = run.run_id

        failed_hubs = self.state.get_failed_hubs(run_id)

        if not failed_hubs:
            print(f"No failed hubs in run {run_id}")
            return {'status': 'no_failures', 'run_id': run_id}

        print(f"Retrying {len(failed_hubs)} failed hubs from run {run_id}")
        print(f"Hubs: {', '.join(failed_hubs)}")

        # Get the month from the original run
        run = self.state.get_run(run_id)
        month = run.month if run else datetime.now().month

        return await self.run_monthly_ingest(month, hubs=failed_hubs)


def _run_dry_run_notify(config: Config):
    """Send test notifications using sample/real data from DPLA data directory."""
    from .notifications import Notifier
    from .anomaly_detector import AnomalyDetector, AnomalyReport, Anomaly, AnomalyType, IngestCounts
    from .s3_utils import get_latest_dir

    print("\n" + "="*60)
    print("  DRY-RUN NOTIFICATIONS")
    print("  All messages will have [TEST] prefix")
    print("="*60 + "\n")

    notifier = Notifier(config)
    test_prefix = "[TEST]"

    # Build sample summary - try to use real data from DPLA data dir
    run_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Find hubs with actual mapping data
    real_hubs = []
    if config.data_dir.exists():
        for hub_dir in config.data_dir.iterdir():
            if hub_dir.is_dir() and not hub_dir.name.startswith('.'):
                mapping_dir = hub_dir / "mapping"
                if mapping_dir.exists() and get_latest_dir(mapping_dir):
                    real_hubs.append(hub_dir.name)

    print(f"Found {len(real_hubs)} hubs with mapping data: {', '.join(real_hubs[:5])}...")

    # Build sample hub data
    sample_hubs = {}

    # Use 1-2 real hubs if available
    for hub_name in real_hubs[:2]:
        sample_hubs[hub_name] = {
            'status': 'complete',
            'duration': 1234,
            'records': 10000,
        }

    # Add simulated hubs
    sample_hubs['simulated-complete'] = {
        'status': 'complete',
        'duration': 567,
        'records': 5000,
    }

    sample_hubs['simulated-failed'] = {
        'status': 'failed',
        'duration': 123,
        'records': None,
        'error': 'Simulated harvest timeout after 5 retries',
        'error_type': 'timeout',
        'failure_stage': 'harvest',
    }

    sample_hubs['simulated-no-email'] = {
        'status': 'complete',
        'duration': 890,
        'records': 7500,
    }

    # Build summary
    summary = {
        'run_id': run_id,
        'month': datetime.now().month,
        'year': datetime.now().year,
        'started_at': datetime.now().isoformat(),
        'completed_at': datetime.now().isoformat(),
        'status': 'partial',
        'hubs': sample_hubs,
        'totals': {
            'complete': sum(1 for h in sample_hubs.values() if h.get('status') == 'complete'),
            'failed': sum(1 for h in sample_hubs.values() if h.get('status') == 'failed'),
            'total': len(sample_hubs),
        }
    }

    print(f"\nSample summary:")
    print(f"  Run ID: {run_id}")
    print(f"  Hubs: {len(sample_hubs)} ({summary['totals']['complete']} complete, {summary['totals']['failed']} failed)")

    # Send completion notification
    print(f"\n--- Sending completion notification ---")
    notifier.send_completion_notification(
        run_id=run_id,
        summary=summary,
        test_prefix=test_prefix,
        write_drafts=True
    )

    # Send sample anomaly alerts
    print(f"\n--- Sending sample anomaly alerts ---")

    # Warning-level anomaly
    warning_report = AnomalyReport(
        hub="simulated-warning",
        current_mapping=IngestCounts(
            hub="simulated-warning",
            timestamp="20260203_120000",
            attempted=10000,
            successful=9500,
            failed=500,
        ),
        baseline_mapping=IngestCounts(
            hub="simulated-warning",
            timestamp="20260103_120000",
            attempted=10500,
            successful=10200,
            failed=300,
        ),
        anomalies=[
            Anomaly(
                hub="simulated-warning",
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
    notifier.send_anomaly_alert("simulated-warning", warning_report, test_prefix=test_prefix)

    # Critical-level anomaly
    critical_report = AnomalyReport(
        hub="simulated-critical",
        current_mapping=IngestCounts(
            hub="simulated-critical",
            timestamp="20260203_120000",
            attempted=5000,
            successful=2000,
            failed=3000,
        ),
        baseline_mapping=IngestCounts(
            hub="simulated-critical",
            timestamp="20260103_120000",
            attempted=10000,
            successful=9800,
            failed=200,
        ),
        anomalies=[
            Anomaly(
                hub="simulated-critical",
                anomaly_type=AnomalyType.OUTPUT_DROP,
                severity='critical',
                message="Mapped records dropped from 9,800 to 2,000 (-79.6%)",
                current_value=2000,
                baseline_value=9800,
                threshold=0.30,
                percent_change=-79.6,
            ),
            Anomaly(
                hub="simulated-critical",
                anomaly_type=AnomalyType.HIGH_FAILURE_RATE,
                severity='critical',
                message="Critical failure rate: 60% of records failed mapping",
                current_value=0.60,
                baseline_value=0.02,
                threshold=0.40,
                percent_change=60.0,
            )
        ]
    )
    notifier.send_anomaly_alert("simulated-critical", critical_report, test_prefix=test_prefix)

    print(f"\n" + "="*60)
    print("  DRY-RUN COMPLETE")
    print("  Check #tech-alerts in Slack for test messages")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="DPLA Monthly Ingest Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scheduler.orchestrator.main                    # Current month (sequential)
  python -m scheduler.orchestrator.main --parallel=3       # Run 3 concurrent processes
  python -m scheduler.orchestrator.main --month=2 -p 2     # February hubs, 2 concurrent
  python -m scheduler.orchestrator.main --hub=maryland     # Single hub
  python -m scheduler.orchestrator.main --hub=ia,florida   # Multiple hubs
  python -m scheduler.orchestrator.main --retry-failed     # Retry failures
  python -m scheduler.orchestrator.main --dry-run -p 3     # Preview parallel execution
  python -m scheduler.orchestrator.main --dry-run-notify   # Test Slack notifications

Parallel Processing:
  With --parallel=N, the orchestrator runs in three phases:
    Phase 1: Download S3 data for file-based hubs (all parallel)
    Phase 2: Harvest & Remap with N concurrent sbt processes
    Phase 3: Upload completed hubs to S3 (all parallel)

  Recommended: --parallel=2 or --parallel=3
  Higher values may cause sbt server conflicts.
        """
    )

    parser.add_argument(
        '--month', '-m',
        type=int,
        default=datetime.now().month,
        help='Month to process (1-12, default: current month)'
    )

    parser.add_argument(
        '--hub',
        type=str,
        help='Specific hub(s) to process (comma-separated)'
    )

    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Retry failed hubs from the most recent run'
    )

    parser.add_argument(
        '--run-id',
        type=str,
        help='Specific run ID to retry (with --retry-failed)'
    )

    parser.add_argument(
        '--skip-harvest',
        action='store_true',
        help='Skip harvest step, run remap on existing data'
    )

    parser.add_argument(
        '--skip-s3-sync',
        action='store_true',
        help='Skip S3 sync after processing'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without running'
    )

    parser.add_argument(
        '--dry-run-notify',
        action='store_true',
        help='Send test notifications to Slack with [TEST] prefix using sample data'
    )

    parser.add_argument(
        '--parallel', '-p',
        type=int,
        default=1,
        help='Number of concurrent processes (default: 1, recommended: 2-3)'
    )

    parser.add_argument(
        '--config',
        type=str,
        help='Path to i3.conf'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current ingest status and exit'
    )

    parser.add_argument(
        '--status-json',
        action='store_true',
        help='Show current ingest status as JSON and exit'
    )

    args = parser.parse_args()

    # Handle status commands before loading full config
    if args.status or args.status_json:
        if args.status_json:
            import json as _json
            print(_json.dumps(get_status_json(), indent=2))
        else:
            print(get_status_table())
        return

    # Load configuration
    try:
        config = load_config(i3_conf_path=args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Handle dry-run-notify: send test notifications using sample/real data
    if args.dry_run_notify:
        _run_dry_run_notify(config)
        return

    # Determine hubs
    if args.hub:
        hubs = [h.strip() for h in args.hub.split(',')]
    else:
        hubs = None

    # Dry run
    if args.dry_run:
        if hubs is None:
            hubs = config.get_scheduled_hubs(args.month)

        file_hubs = [h for h in hubs if config.get_hub_config(h).harvest_type == "file"]
        api_hubs = [h for h in hubs if h not in file_hubs]

        budget = ResourceBudget.compute(parallel=args.parallel)
        print(f"\nDry Run - Month {args.month} (parallel={args.parallel})")
        print(f"{'='*60}")
        print(f"Hubs to process ({len(hubs)}):")
        print(f"\n  File-based ({len(file_hubs)}):")
        for hub in file_hubs:
            print(f"    - {hub}")
        print(f"\n  OAI/API ({len(api_hubs)}):")
        for hub in api_hubs:
            print(f"    - {hub}")
        print(f"\n{'='*60}")
        print(f"Resource budget per hub:")
        print(f"  Spark: {budget.spark_master}")
        print(f"  Memory: {budget.memory_gb}g heap")
        print(f"  SBT_OPTS: {budget.sbt_opts}")
        print(f"\n{'='*60}")
        print(f"Execution plan:")
        print(f"  Phase 1: Download S3 data for {len(file_hubs)} file hubs (parallel)")
        print(f"  Phase 2: Harvest & Remap all {len(hubs)} hubs ({args.parallel} concurrent)")
        print(f"  Phase 3: S3 sync completed hubs (parallel)")
        print(f"{'='*60}")
        print("\nNo changes made (dry run)")
        return

    # Create orchestrator and run
    orchestrator = IngestOrchestrator(config, parallel=args.parallel)

    if args.retry_failed:
        result = asyncio.run(orchestrator.retry_failed(args.run_id))
    else:
        result = asyncio.run(orchestrator.run_monthly_ingest(
            month=args.month,
            hubs=hubs,
            skip_harvest=args.skip_harvest,
            skip_s3_sync=args.skip_s3_sync,
        ))

    # Print final summary
    print(f"\n{'='*60}")
    print("  Final Summary")
    print(f"{'='*60}")

    if 'totals' in result:
        totals = result['totals']
        print(f"  Complete: {totals.get('complete', 0)}")
        print(f"  Failed: {totals.get('failed', 0)}")
        print(f"  Total: {totals.get('total', 0)}")

    print(f"{'='*60}\n")

    # Exit with error if any failures
    if result.get('totals', {}).get('failed', 0) > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
