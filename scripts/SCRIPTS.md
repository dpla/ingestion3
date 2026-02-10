# DPLA Ingestion Scripts

Shell scripts for running the DPLA ingestion3 pipeline. All scripts are cross-platform compatible with macOS and Ubuntu Linux.

## Quick Reference

| Script | Purpose | Usage |
|--------|---------|-------|
| `ingest.sh` | Full pipeline (harvest → map → enrich → jsonl) | `./ingest.sh <hub>` |
| `harvest.sh` | Harvest records from OAI/API/file source | `./harvest.sh <hub>` |
| `remap.sh` | Re-run mapping → enrichment → jsonl | `./remap.sh <hub>` |
| `mapping.sh` | Transform harvested records to DPLA MAP | `./mapping.sh <hub>` |
| `enrich.sh` | Enrich/normalize DPLA MAP records | `./enrich.sh <hub>` |
| `jsonl.sh` | Export enriched records to JSON-L | `./jsonl.sh <hub>` |
| `auto-ingest.sh` | Automated monthly ingestion | `./auto-ingest.sh [--hub=<hub>]` |
| `batch-ingest.sh` | Run pipeline for multiple hubs | `./batch-ingest.sh <hub1> <hub2>...` |
| `nara-ingest.sh` | NARA delta ingest pipeline | `./nara-ingest.sh <nara-export.zip>` |
| `schedule.sh` | Query hub ingest schedules | `./schedule.sh [month\|hub]` |
| `s3-sync.sh` | Sync hub data to S3 | `./s3-sync.sh <hub> [subdir]` |
| `fix-si.sh` | Preprocess Smithsonian data | `./fix-si.sh <folder>` |
| `harvest-va.sh` | Download Digital Virginias repos | `./harvest-va.sh [output-dir]` |
| `check-jsonl-sync.sh` | Check JSONL sync status with S3 | `./check-jsonl-sync.sh` |
| `delete-by-id.sh` | Delete records from Elasticsearch | `./delete-by-id.sh <id>...` |
| `delete-from-jsonl.sh` | Delete records from S3 JSONL files | `./delete-from-jsonl.sh --hub <hub> <id>...` |
| `send-ingest-email.sh` | Send ingest summary email | `./send-ingest-email.sh <hub>` |
| `ingest-status.sh` | Check orchestrator status | `./ingest-status.sh` |

## Environment Variables

All scripts use these environment variables (with sensible defaults):

| Variable | Description | Default |
|----------|-------------|---------|
| `I3_HOME` | Ingestion3 repository root | Auto-detected from script location |
| `DPLA_DATA` | Data output directory | `$HOME/dpla/data` |
| `I3_CONF` | Path to i3.conf configuration | `$HOME/dpla/code/ingestion3-conf/i3.conf` |
| `JAVA_HOME` | Java installation directory | Auto-detected |
| `SPARK_MASTER` | Spark execution mode (pipeline parallelism) | `local[4]` |
| `AWS_PROFILE` | AWS credentials profile | `dpla` |
| `I3_JAR` | Override path to ingestion3 fat JAR | `$I3_HOME/target/scala-2.13/ingestion3-assembly-0.0.1.jar` |

All scripts automatically use the fat JAR when it exists (no flag needed). Build it once:

```bash
cd "$I3_HOME" && sbt assembly
# Now all scripts (harvest, mapping, enrich, jsonl, remap) use the JAR automatically
./scripts/remap.sh maryland
```

## Shared Configuration (common.sh)

All scripts source `common.sh` which provides:

- **Platform detection**: `$PLATFORM` is set to `macos` or `linux`
- **Portable utilities**: `sed_i`, `get_script_dir`, `get_common_dir`
- **Java setup**: `setup_java <memory>` auto-detects Java and sets up environment
- **Logging**: `log_info`, `log_warn`, `log_error`, `log_success`, `print_step`
- **Validation**: `require_command`, `require_file`, `require_dir`, `die`
- **Hub helpers**: `get_provider_name`, `get_hub_email`, `get_harvest_type`
- **Process management**: `kill_tree <pid>` — recursively kill a process and all its descendants (prevents orphan JVMs)
- **Entry runner**: `run_entry <EntryClass> [--arg=val ...]` — runs any Scala entry class via JAR (preferred) or sbt (fallback)
- **IngestRemap runner**: `run_ingest_remap <input> <output> <conf> <name>` — convenience wrapper for IngestRemap
- **Data finder**: `find_latest_data <provider> <step>` — finds the most recent timestamped directory for a pipeline step

### Example: Using common.sh in a new script

```bash
#!/usr/bin/env bash
set -euo pipefail

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Setup Java (4g memory)
setup_java "4g" || die "Failed to setup Java"

# Use provided variables and functions
log_info "I3_HOME is: $I3_HOME"
log_info "Platform is: $PLATFORM"
```

## Script Details

### ingest.sh - Full Pipeline

Runs the complete ingestion pipeline for a hub:

```bash
./scripts/ingest.sh maryland                  # Full pipeline
./scripts/ingest.sh maryland --skip-harvest   # Use existing harvest data
./scripts/ingest.sh maryland --harvest-only   # Only harvest
```

### auto-ingest.sh - Automated Monthly Ingestion

Processes hubs scheduled for the current month:

```bash
./scripts/auto-ingest.sh                    # All scheduled hubs
./scripts/auto-ingest.sh --hub=maryland     # Single hub
./scripts/auto-ingest.sh --month=3          # March hubs
./scripts/auto-ingest.sh --dry-run          # Preview only
./scripts/auto-ingest.sh --skip-harvest     # Skip harvest step
./scripts/auto-ingest.sh --no-email         # Skip email notifications
./scripts/auto-ingest.sh --no-s3-sync       # Skip S3 sync
```

### nara-ingest.sh - NARA Delta Ingest

Handles NARA's large dataset with delta updates:

```bash
./scripts/nara-ingest.sh /path/to/nara-export.zip
./scripts/nara-ingest.sh /path/to/export.zip --force-sync
./scripts/nara-ingest.sh --skip-to-pipeline   # Use existing merged harvest
```

### schedule.sh - Query Schedules

Query hub ingest schedules from i3.conf:

```bash
./scripts/schedule.sh              # Full year schedule
./scripts/schedule.sh feb          # February hubs
./scripts/schedule.sh 2            # Month 2 hubs
./scripts/schedule.sh virginias    # Single hub schedule
```

### fix-si.sh - Smithsonian Preprocessing

Preprocess Smithsonian XML files before harvest:

```bash
./scripts/fix-si.sh --list        # List available folders
./scripts/fix-si.sh 20260201      # Process specific folder
```

### delete-by-id.sh - Elasticsearch Delete

Delete records from Elasticsearch by DPLA ID:

```bash
./scripts/delete-by-id.sh <id1> <id2> ...
./scripts/delete-by-id.sh -f ids-to-delete.txt
cat ids.txt | ./scripts/delete-by-id.sh -f -

# Preview without deleting
DRY_RUN=true ./scripts/delete-by-id.sh -f ids.txt
```

### delete-from-jsonl.sh - S3 JSONL Delete

Delete records from JSONL files in S3:

```bash
./scripts/delete-from-jsonl.sh --hub cdl -f ids-to-delete.txt
./scripts/delete-from-jsonl.sh --hub cdl <id1> <id2> ...

# Preview without modifying
DRY_RUN=true ./scripts/delete-from-jsonl.sh --hub cdl -f ids.txt
```

> **Note**: For better performance, use `delete-from-jsonl.py` instead.

## Testing

Run the test suite to verify scripts work correctly:

```bash
./scripts/tests/test-scripts.sh           # Full test suite
./scripts/tests/test-scripts.sh --quick   # Syntax checks only
./scripts/tests/test-scripts.sh --verbose # Show detailed output
```

Tests verify:
- All scripts have valid bash syntax
- All scripts source common.sh correctly
- Environment variable defaults work
- Cross-platform functions work (sed_i, get_script_dir)
- Help/usage outputs work
- No hardcoded `/Users/scott` paths remain

## Platform Notes

### macOS vs Ubuntu Differences

| Feature | macOS | Ubuntu | Solution |
|---------|-------|--------|----------|
| `sed -i` | `sed -i '' ...` | `sed -i ...` | Use `sed_i` function |
| `readlink -f` | Not available | Works | Use `get_script_dir` function |
| Java location | `/Library/Java/...` | `/usr/lib/jvm/...` | Use `setup_java` function |

### Java Detection

The `setup_java` function automatically detects Java:

- **macOS**: Uses `/usr/libexec/java_home` or searches common locations
- **Ubuntu**: Searches `/usr/lib/jvm/`, `/opt/java/`, or uses `which java`

Override by setting `JAVA_HOME` before running scripts.

## Python Orchestrator (Parallel Ingests)

For parallel hub processing, use the Python orchestrator instead of `auto-ingest.sh`:

```bash
# Activate venv
source ./venv/bin/activate

# Current month, sequential (default)
python -m scheduler.orchestrator.main

# Run 3 hubs concurrently
python -m scheduler.orchestrator.main --parallel=3

# Specific hubs
python -m scheduler.orchestrator.main --hub=mi,va,mn --parallel=3

# Preview execution plan
python -m scheduler.orchestrator.main --dry-run --parallel=3

# Retry failures from last run
python -m scheduler.orchestrator.main --retry-failed

# Check status
python -m scheduler.orchestrator.main --status
```

Resource budgeting is automatic: `--parallel=2` gives each hub `local[2]` and ~4g heap, `--parallel=3` gives `local[2]` and ~4g heap each. Per-hub status files are written to `logs/status/<hub>.status`.

## Workflow Diagrams

### Standard Ingest Pipeline

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Harvest │ -> │ Mapping │ -> │ Enrich  │ -> │  JSONL  │
└─────────┘    └─────────┘    └─────────┘    └─────────┘
     │              │              │              │
     v              v              v              v
  harvest/       mapping/     enrichment/      jsonl/
```

### Script Relationships

```
auto-ingest.sh
     │
     ├── harvest.sh (or S3 download for file hubs)
     │
     └── remap.sh
           │
           └── IngestRemap (mapping + enrichment + jsonl)

ingest.sh
     │
     ├── HarvestEntry
     │
     └── IngestRemap

batch-ingest.sh
     │
     └── ingest.sh (for each hub)
```

## Updating This Documentation

When adding or modifying scripts:

1. Update the Quick Reference table
2. Add/update the Script Details section
3. Update environment variables if new ones are added
4. Run `./scripts/tests/test-scripts.sh` to verify changes
5. Test on both macOS and Ubuntu if possible
