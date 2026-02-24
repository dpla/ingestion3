# DPLA Ingest Debugging Skill

## Purpose
Debug and fix DPLA hub ingestion failures with intelligent analysis and automated fixes.

## When to Use
Activate this skill when the user says:
- "Debug the X harvest/ingest failure"
- "Why did X hub fail"
- "Fix the X ingest"
- "Check the escalation for X"
- "Retry the X harvest"

**Environment:** From repo root, run `source .env` before running any scripts (e.g. `harvest.sh`, `remap.sh`, `s3-sync.sh`) so `JAVA_HOME`, `DPLA_DATA`, `I3_CONF` are set. See [AGENTS.md](AGENTS.md) § Environment and build.

## Key Locations

| Resource | Path |
|----------|------|
| Config | `/Users/scott/dpla/code/ingestion3-conf/i3.conf` |
| Data | `/Users/scott/dpla/data/<hub>/` |
| Logs | `/Users/scott/dpla/code/ingestion3/logs/` |
| Escalations | `/Users/scott/dpla/data/escalations/` |
| Scripts | `/Users/scott/dpla/code/ingestion3/scripts/` |
| Orchestrator | `/Users/scott/dpla/code/ingestion3/scheduler/orchestrator/` |

## Debugging Workflow

### Step 1: Gather Context
1. Check for escalation files:
   ```bash
   ls ~/dpla/data/escalations/
   ```
2. Read the failure report if it exists:
   ```bash
   cat ~/dpla/data/escalations/failures-*.md | tail -1
   ```
3. Check recent logs:
   ```bash
   ls -lt ~/dpla/code/ingestion3/logs/*.log | head -5
   ```
4. Check hub data directory:
   ```bash
   ls ~/dpla/data/<hub>/
   ```

### Step 2: Diagnose the Error

#### Common Error Patterns

| Error Pattern | Type | Cause | Fix |
|---------------|------|-------|-----|
| `OutOfMemoryError` | OOM | Large XML files | Run xmll preprocessing (see below) |
| `Connection.*timed out` | Timeout | Slow OAI feed | Retry with backoff, check feed status |
| `Address already in use` | sbt conflict | Concurrent sbt | Wait, kill stale `ps aux \| grep sbt` |
| `NullPointerException.*InputHelper` | No data | Missing harvest | Run harvest first |
| Exit 0 but no output | OAI issue | Empty feed | Check OAI URL, contact provider |
| `NoSuchBucket` | S3 error | Wrong bucket | Check bucket name in config |

### Step 3: Apply Fixes

#### For Smithsonian OutOfMemoryError
This is the most common issue. Smithsonian files need xmll preprocessing:

```bash
# 1. Find the data directory
ls ~/dpla/data/smithsonian/originalRecords/

# 2. Create preprocessing directories
DATA_DIR=~/dpla/data/smithsonian/originalRecords/<latest-date>
mkdir -p "$DATA_DIR/fixed" "$DATA_DIR/xmll"

# 3. Recompress gzip files (SI exports have issues)
for f in "$DATA_DIR"/*.xml.gz; do
  basename=$(basename "$f")
  echo "Recompressing: $basename"
  gunzip -dck "$f" | gzip > "$DATA_DIR/fixed/$basename"
done

# 4. Run xmll shredder
for f in "$DATA_DIR/fixed"/*.xml.gz; do
  basename=$(basename "$f")
  echo "Processing: $basename"
  java -Xmx4g -jar ~/xmll-assembly-0.1.jar doc "$f" "$DATA_DIR/xmll/$basename"
done

# 5. Update i3.conf endpoint to use xmll directory
# Edit: smithsonian.harvest.endpoint = "$DATA_DIR/xmll/"

# 6. Retry harvest
cd ~/dpla/code/ingestion3 && ./scripts/harvest.sh smithsonian
```

#### For Timeout Errors
```bash
# Simply retry - the script has built-in retry logic
cd ~/dpla/code/ingestion3 && ./scripts/harvest.sh <hub>
```

#### For sbt Conflicts
```bash
# Check for running sbt processes
ps aux | grep sbt

# Kill stale processes if needed
pkill -f "sbt.*ingestion3"

# Wait a moment, then retry
sleep 10
cd ~/dpla/code/ingestion3 && ./scripts/harvest.sh <hub>
```

#### For No Harvest Output
```bash
# Check if harvest exists
ls ~/dpla/data/<hub>/harvest/

# If no harvest, run it first
cd ~/dpla/code/ingestion3 && ./scripts/harvest.sh <hub>

# Then run remap
cd ~/dpla/code/ingestion3 && ./scripts/remap.sh <hub>
```

### Step 4: Verify Success

After applying fix, verify:

```bash
# Check harvest output exists
ls ~/dpla/data/<hub>/harvest/*.avro

# Check jsonl output exists (after remap)
ls ~/dpla/data/<hub>/jsonl/*.jsonl

# Check for errors in latest log
grep -i "error\|exception\|failed" ~/dpla/code/ingestion3/logs/*.log | tail -20
```

### Step 5: Sync to S3 (if successful)

```bash
cd ~/dpla/code/ingestion3 && ./scripts/s3-sync.sh <hub>
```

## Hub-Specific Notes

### Smithsonian (smithsonian)
- **Type**: File harvest
- **Source**: s3://dpla-hub-si/
- **Special**: Requires xmll preprocessing for large XML files
- **Common Issue**: OutOfMemoryError on NMNHBIRDS_DPLA.xml.gz

### Florida (florida)
- **Type**: File harvest
- **Source**: s3://dpla-hub-fl/
- **Format**: JSONL files that need zipping

### Vermont (vt)
- **Type**: File harvest
- **Source**: s3://dpla-hub-vt/
- **Format**: Pre-zipped files

### Harvard (harvard)
- **Type**: OAI harvest
- **Common Issue**: Feed may return no records - check OAI endpoint

### MWDL (mwdl)
- **Type**: API harvest (Primo)
- **Note**: Very large (~1.1M records), takes many hours

## Using the Python Orchestrator

For automated runs:

```bash
# Run current month's scheduled hubs
python -m scheduler.orchestrator.main

# Run specific hub
python -m scheduler.orchestrator.main --hub=maryland

# Retry failed hubs from last run
python -m scheduler.orchestrator.main --retry-failed

# Dry run to see what would be processed
python -m scheduler.orchestrator.main --dry-run
```

## Escalation Files

When the orchestrator encounters failures, it creates:
- `~/dpla/data/escalations/failures-<run_id>.json` - Machine-readable
- `~/dpla/data/escalations/failures-<run_id>.md` - Human-readable

Read these to understand what failed and why.

## Example Debugging Session

User: "Debug the smithsonian harvest failure"

1. Read escalation file to understand the error
2. Identify OutOfMemoryError on specific file
3. Check if xmll preprocessing was done
4. If not, run the preprocessing workflow
5. Update i3.conf endpoint to point to xmll/ directory
6. Retry harvest: `./scripts/harvest.sh smithsonian`
7. Verify output exists: `ls ~/dpla/data/smithsonian/harvest/`
8. Run remap: `./scripts/remap.sh smithsonian`
9. Verify jsonl: `ls ~/dpla/data/smithsonian/jsonl/`
10. Sync to S3: `./scripts/s3-sync.sh smithsonian`
11. Report success to user
