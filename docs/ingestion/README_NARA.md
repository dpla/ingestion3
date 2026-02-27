# NARA Delta Ingest Pipeline

Complete guide to running NARA (National Archives and Records Administration) delta
ingests. NARA is the largest DPLA provider at ~18.8 million records. Delta ingests are
delivered quarterly as ZIP files containing XML records grouped by "export group."

> **Last updated**: February 2026 (Dec 2025 + Jan 2026 delta cycle)  
> **Current record count**: 18,789,995

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Prerequisites](#prerequisites)
3. [Data Delivery Format](#data-delivery-format)
4. [Step-by-Step Workflow](#step-by-step-workflow)
   - [Step 1: Preprocessing](#step-1-preprocessing-unzip-repackage)
   - [Step 2: S3 Base Harvest Sync](#step-2-sync-base-harvest-from-s3)
   - [Step 3: Delta Harvest](#step-3-delta-harvest)
   - [Step 4: Merge](#step-4-merge)
   - [Step 5: Pipeline](#step-5-pipeline-mapping--enrichment--json-l)
   - [Step 6: S3 Sync](#step-6-sync-outputs-to-s3)
5. [Multi-Month Ingests](#multi-month-ingests)
6. [Validation](#validation)
7. [Performance & Memory Tuning](#performance--memory-tuning)
8. [Troubleshooting](#troubleshooting)
9. [Directory Layout](#directory-layout)
10. [History](#history)

---

## Automated Script

The `scripts/harvest/nara-ingest.sh` script automates the entire workflow:

```bash
# Single month, full pipeline
./scripts/harvest/nara-ingest.sh --month=202601

# Two months: Dec merge-only, Jan merge + full pipeline
./scripts/harvest/nara-ingest.sh --month=202512,202601

# Stop after merge (no mapping/enrichment/jsonl)
./scripts/harvest/nara-ingest.sh --month=202601 --skip-pipeline

# Use a specific base harvest
./scripts/harvest/nara-ingest.sh --month=202601 --base=~/dpla/data/nara/harvest/20250429_000000-nara-OriginalRecord.avro

# Resume pipeline on an existing merged harvest
./scripts/harvest/nara-ingest.sh --skip-to-pipeline --harvest=~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro
```

The script handles:
- Automatic preprocessing (unzip by export group, create tar.gz, separate deletes)
- S3 base harvest sync (if no local base exists)
- Idempotent step detection (skips already-completed preprocess/harvest/merge steps)
- Per-step memory tuning (configurable via environment variables)
- Multi-month chaining (merged output from month N becomes base for month N+1)

See `./scripts/harvest/nara-ingest.sh --help` for all options.

---

## Quick Reference (Manual)

```bash
# Environment setup (required for all sbt commands)
export JAVA_HOME=/path/to/java19+/Home
export SBT_OPTS="-Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
I3_HOME=~/dpla/code/ingestion3
I3_CONF=~/dpla/code/ingestion3-conf/i3.conf

# 1. Preprocess: unzip each export group, repackage as tar.gz
#    (see Step 1 below for detailed instructions)

# 2. Sync base harvest from S3
aws s3 sync s3://dpla-master-dataset/nara/harvest/ ~/dpla/data/nara/harvest/ \
  --exclude "*" --include "*-nara-OriginalRecord.avro/*"

# 3. Delta harvest
cd $I3_HOME && sbt -java-home "$JAVA_HOME" \
  "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
  --output=~/dpla/data/nara/delta/YYYYMM/harvest \
  --conf=$I3_CONF --name=nara --sparkMaster=local[1]"

# 4. Merge delta into base
cd $I3_HOME && sbt -java-home "$JAVA_HOME" \
  "runMain dpla.ingestion3.utils.NaraMergeUtil \
  /path/to/base.avro /path/to/delta-harvest.avro \
  /path/to/deletes/ /path/to/output.avro local[1]"

# 5. Full pipeline (mapping -> enrichment -> JSON-L)
cd $I3_HOME && sbt -java-home "$JAVA_HOME" \
  "runMain dpla.ingestion3.entries.ingest.IngestRemap \
  --input=/path/to/merged-harvest.avro --output=~/dpla/data \
  --conf=$I3_CONF --name=nara --sparkMaster=local[4]"
```

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **Java** | 11+ required; Java 19 recommended. Must set `JAVA_HOME` explicitly for `sbt`. |
| **SBT** | Scala Build Tool. Install via Homebrew: `brew install sbt` |
| **AWS CLI** | Configured with profile that has access to `dpla-master-dataset` bucket |
| **Disk Space** | ~150 GB minimum (34 GB base harvest + processing overhead + pipeline output) |
| **RAM** | 18 GB recommended. JVM heap of 12-14 GB for merge/pipeline operations. |
| **i3.conf** | Configuration file at `~/dpla/code/ingestion3-conf/i3.conf` |

### Java Setup (macOS)

```bash
# Find installed JDKs
/usr/libexec/java_home -V

# Set for NARA ingest (Java 19 example)
export JAVA_HOME=/Users/$USER/Library/Java/JavaVirtualMachines/openjdk-19.0.2/Contents/Home
```

---

## Data Delivery Format

NARA delivers quarterly delta exports as sets of ZIP files organized by **export group**
(a numeric code like `17.115`, `09.113`, etc.). Each month's delivery may contain:

- **Multiple export groups**, each with one or more ZIP files
  - `2026-02-07_17.115_DESC_0001.zip` through `..._0010.zip`
- **Delete files** (XML) listing NARA IDs to remove
  - `2026-02-07_14.041_DESC_NaidsList.xml`

The ZIPs are located at: `~/dpla/data/nara/originalRecords/YYYYMM/`

### Naming Convention

```
DATE_EXPORTGROUP_DESC_SEQNUM.zip     # data ZIP
DATE_EXPORTGROUP_DESC_NaidsList.xml  # deletes file
```

---

## Step-by-Step Workflow

### Step 1: Preprocessing (Unzip & Repackage)

The `NaraDeltaHarvester` reads `.tar.gz` files from a directory. Each export group's
ZIPs must be unzipped and combined into a single `.tar.gz` per export group.

**For each export group in the month:**

```bash
MONTH=202601
GROUP=17.115
SRC=~/dpla/data/nara/originalRecords/$MONTH
DEST=~/dpla/data/nara/delta/$MONTH

mkdir -p $DEST/deletes $DEST/work

# Unzip all ZIPs for this export group into a work directory
for zip in $SRC/*_${GROUP}_DESC_*.zip; do
  unzip -o "$zip" -d $DEST/work/
done

# Create a single tar.gz for this export group
cd $DEST/work && tar -czf $DEST/${GROUP}_nara_delta.tar.gz *.xml
rm -rf $DEST/work
```

**Move delete files separately:**

```bash
# Copy delete XMLs (NaidsList files) to the deletes directory
cp $SRC/*_DESC_NaidsList.xml $DEST/deletes/
```

**Result structure:**

```
~/dpla/data/nara/delta/202601/
  09.113_nara_delta.tar.gz      # one per export group
  09.117_nara_delta.tar.gz
  17.115_nara_delta.tar.gz
  ...
  deletes/
    2026-02-07_14.041_DESC_NaidsList.xml
    2026-02-08_09.019_DESC_NaidsList.xml
    ...
```

> **Important**: The harvester reads ALL `.tar.gz` files in the delta directory.
> Each tar.gz should contain only the XML data files for its export group.
> Delete XML files must be in the `deletes/` subdirectory, not alongside the tar.gz files.

### Step 2: Sync Base Harvest from S3

The merge needs the most recent base harvest as input. Download it from S3:

```bash
aws s3 sync s3://dpla-master-dataset/nara/harvest/ ~/dpla/data/nara/harvest/ \
  --exclude "*" --include "*-nara-OriginalRecord.avro/*"
```

This downloads ~34 GB. The most recent directory (sorted alphabetically) is the base:

```bash
ls -td ~/dpla/data/nara/harvest/*-nara-OriginalRecord.avro | head -1
```

> **Tip**: If running multiple months sequentially, the merged output from the first
> month becomes the base for the next month. No need to re-download from S3.

### Step 3: Delta Harvest

Update `i3.conf` to point to the delta directory, then run the harvester:

```bash
# Update i3.conf
# Set both of these to the delta directory:
#   nara.harvest.endpoint = "/Users/you/dpla/data/nara/delta/202601/"
#   nara.harvest.delta.update = "/Users/you/dpla/data/nara/delta/202601/"

cd ~/dpla/code/ingestion3
export JAVA_HOME=/path/to/java19/Home
export SBT_OPTS="-Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

sbt -java-home "$JAVA_HOME" \
  "runMain dpla.ingestion3.entries.ingest.HarvestEntry \
  --output=~/dpla/data/nara/delta/202601/harvest \
  --conf=~/dpla/code/ingestion3-conf/i3.conf \
  --name=nara \
  --sparkMaster=local[1]"
```

The output will be at:
`~/dpla/data/nara/delta/202601/harvest/nara/harvest/YYYYMMDD_HHMMSS-nara-OriginalRecord.avro`

> **Use `local[1]`** for the harvest step. The NaraDeltaHarvester writes a single
> Avro file sequentially and multiple Spark tasks are not beneficial.

### Step 4: Merge

Merge the delta harvest into the base to produce a new combined harvest.

```bash
cd ~/dpla/code/ingestion3
export SBT_OPTS="-Xms4g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

sbt -java-home "$JAVA_HOME" \
  "runMain dpla.ingestion3.utils.NaraMergeUtil \
  ~/dpla/data/nara/harvest/BASE_HARVEST.avro \
  ~/dpla/data/nara/delta/202601/harvest/nara/harvest/DELTA_HARVEST.avro \
  ~/dpla/data/nara/delta/202601/deletes/ \
  ~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro \
  local[1]"
```

**Parameters (positional):**

1. Path to base harvest (Avro directory)
2. Path to delta harvest (Avro directory)
3. Path to deletes directory (folder with `*NaidsList.xml` files)
4. Path for merged output (will be created)
5. Spark master (`local[1]` recommended - see Performance section)

**Important**: The output path must follow the `YYYYMMDD_HHMMSS-nara-OriginalRecord.avro`
naming convention (8-digit date) for the pipeline's `IngestRemap` to recognize it.

**After completion**, inspect the merge summary:

```bash
cat ~/dpla/data/nara/harvest/OUTPUT/_LOGS/_SUMMARY.txt
```

### Step 5: Pipeline (Mapping -> Enrichment -> JSON-L)

Run the full pipeline on the merged harvest. This is the longest step (~10 hours
for 18.8M records on a local machine).

```bash
cd ~/dpla/code/ingestion3

# Using the remap script (recommended - uses fat JAR when available)
./scripts/remap.sh nara ~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro

# Or standalone JSON-L export (if only jsonl step is needed)
./scripts/jsonl.sh nara
```

> **Use `local[4]`** for the pipeline. The mapping step is CPU-intensive (XML parsing)
> and benefits from parallelism. 4 cores balances speed vs memory usage.

**Output locations:**

| Stage | Path |
|-------|------|
| Mapping | `~/dpla/data/nara/mapping/YYYYMMDD_HHMMSS-nara-MAP4_0.MAPRecord.avro/` |
| Enrichment | `~/dpla/data/nara/enrichment/YYYYMMDD_HHMMSS-nara-MAP4_0.EnrichRecord.avro/` |
| JSON-L | `~/dpla/data/nara/jsonl/YYYYMMDD_HHMMSS-nara-MAP3_1.IndexRecord.jsonl/` |

### Step 6: Sync Outputs to S3

After validating the pipeline output, sync everything to S3:

```bash
# Harvest
aws s3 sync ~/dpla/data/nara/harvest/MERGED_OUTPUT.avro \
  s3://dpla-master-dataset/nara/harvest/MERGED_OUTPUT.avro/

# Mapping
aws s3 sync ~/dpla/data/nara/mapping/ s3://dpla-master-dataset/nara/mapping/

# Enrichment
aws s3 sync ~/dpla/data/nara/enrichment/ s3://dpla-master-dataset/nara/enrichment/

# JSON-L
aws s3 sync ~/dpla/data/nara/jsonl/ s3://dpla-master-dataset/nara/jsonl/
```

---

## Multi-Month Ingests

When processing multiple months (e.g., catching up on Dec 2025 + Jan 2026):

1. **Process months in chronological order** (Dec before Jan)
2. **For intermediate months** (Dec), you can skip the pipeline (`--skip-pipeline`)
   and only produce the merged harvest
3. **Use the merged output from month N as the base for month N+1**
4. **Run the full pipeline only once**, on the final month's merged output

### Example: Dec 2025 + Jan 2026

```bash
# == December (merge only, no pipeline) ==

# 1. Preprocess Dec originalRecords into delta/202512/
# 2. Harvest Dec delta
sbt ... "runMain ...HarvestEntry --output=~/dpla/data/nara/delta/202512/harvest ..."

# 3. Merge Dec delta into S3 base
sbt ... "runMain ...NaraMergeUtil \
  ~/dpla/data/nara/harvest/20250429_000000-nara-OriginalRecord.avro \
  ~/dpla/data/nara/delta/202512/harvest/.../DELTA.avro \
  ~/dpla/data/nara/delta/202512/deletes/ \
  ~/dpla/data/nara/harvest/202512_000000-nara-OriginalRecord.avro \
  local[1]"

# == January (merge + full pipeline) ==

# 1. Preprocess Jan originalRecords into delta/202601/
# 2. Harvest Jan delta
sbt ... "runMain ...HarvestEntry --output=~/dpla/data/nara/delta/202601/harvest ..."

# 3. Merge Jan delta into Dec merged output
sbt ... "runMain ...NaraMergeUtil \
  ~/dpla/data/nara/harvest/202512_000000-nara-OriginalRecord.avro \
  ~/dpla/data/nara/delta/202601/harvest/.../DELTA.avro \
  ~/dpla/data/nara/delta/202601/deletes/ \
  ~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro \
  local[1]"

# 4. Run full pipeline on final merged output
sbt ... "runMain ...IngestRemap \
  --input=~/dpla/data/nara/harvest/20260209_000000-nara-OriginalRecord.avro \
  --output=~/dpla/data --conf=$I3_CONF --name=nara --sparkMaster=local[4]"
```

---

## Validation

### Merge Summary

After each merge, check `_LOGS/_SUMMARY.txt` for:

- **Total [actual] == Total [expected]**: Confirms merge arithmetic is correct
- **Duplicate count**: Usually 0 for base (already deduped); small numbers for delta are normal
- **New + Update == Delta unique**: All delta records classified correctly
- **Delete valid vs invalid**: Invalid deletes are IDs in the delete file not found in the dataset

### Example Summary (Jan 2026)

```
 Base:   18,696,139 records (0 duplicates)
 Delta:  298,459 records (1,315 duplicates) -> 297,144 unique
 Merged: 18,789,995 = 18,696,139 + 93,856 new
         203,288 updates, 93,856 inserts
 Delete: 2 in file, 0 valid, 2 invalid
 Final:  18,789,995
```

### Operations CSV

The merge also writes a CSV of every operation at `_LOGS/ops/`:

```csv
id,operation
580001954,insert
17426465,update
1311955,invalid delete
```

### Pipeline Validation

After the pipeline completes, verify the output directories exist and contain data:

```bash
# Check each output has files
for dir in mapping enrichment jsonl; do
  echo "$dir: $(find ~/dpla/data/nara/$dir -name '_SUCCESS' | wc -l) activities"
done
```

---

## Performance & Memory Tuning

### Why NARA Is Special

The NARA dataset is ~34 GB of Avro files containing 18.8M records, each with a large
XML document in the `document` field. This creates unique memory challenges:

- **Spark columnar caching** (`MEMORY_AND_DISK`) tries to build in-memory columnar batches
  of the XML strings, causing OOM even with 15 GB heap
- **Spark `DISK_ONLY` caching** serializes via Kryo, expanding 34 GB of Avro to 300+ GB
  of temp files
- **SQL GROUP BY** for deduplication causes expensive full shuffles of all records

### NaraMergeUtil: Zero-Persistence Strategy

The current `NaraMergeUtil` uses a **zero-persistence** approach optimized for local
SSD-based processing:

- **Never persists the base DataFrame** -- re-reads from Avro each time (~30s per scan)
- **Writes delta dedup to temp Avro** instead of Spark cache (compact, no Kryo bloat)
- **Broadcasts delta IDs** (small Set) for efficient filtering of the large base
- **Skips base dedup** (base was already deduped from previous merge)
- **Writes merged output directly** in a single streaming pass (filter + union + write)
- **Collects all ID sets to driver** for ops log (safe because sets are bounded by delta size)

This approach completes in **~12 minutes** using only **3 GB of RAM** and **~1 GB of temp disk**.

### Recommended Settings by Step

| Step | `SBT_OPTS` | `sparkMaster` | Time (18.8M records) |
|------|-----------|--------------|---------------------|
| Delta Harvest | `-Xmx12g` | `local[1]` | ~5 min |
| Merge | `-Xmx12g` | `local[1]` | ~12 min |
| Pipeline (IngestRemap) | `-Xmx14g` | `local[4]` | ~10 hours |

### If You Hit OutOfMemoryError

1. **In the merge**: Ensure you're using the latest `NaraMergeUtil` code (zero-persistence).
   If OOM persists, the issue is likely the broadcast set. Reduce delta batch size.
2. **In the pipeline**: Reduce parallelism (`local[2]` or `local[1]`). Each mapping task
   processes ~65K XML records and uses ~1-2 GB of heap.
3. **General**: Never use `local[*]` for NARA -- it launches too many concurrent tasks.

---

## Troubleshooting

### "Unable to load harvest data" in IngestRemap

The input path must match the activity path pattern: `YYYYMMDD_HHMMSS-provider-Schema`.
The date portion must be **8 digits** (e.g., `20260209`), not 6 (e.g., `202601`).

**Fix**: Ensure the merged harvest directory name uses the full 8-digit date format:
```
20260209_000000-nara-OriginalRecord.avro   # correct
202601_000000-nara-OriginalRecord.avro     # WRONG - only 6 digits
```

### Delta Harvest Produces 0 Records

This was caused by a conflict between `NaraDeltaHarvester` and its parent class
`LocalHarvester`. The parent creates an empty Avro file in the same temp directory.
**Fix** (already applied): `NaraDeltaHarvester` now uses a unique subdirectory for its
temp Avro file (`/tmp/nara/delta/`).

Also ensure:
- `avroWriterNara.close()` is called (not just `flush()`) to finalize the Avro footer
- The `i3.conf` paths point to the correct delta directory

### Merge Uses 300+ GB of Disk

This happens when using `DISK_ONLY` persistence with Kryo serialization. The current
code uses the **zero-persistence strategy** which avoids this entirely. Make sure you
have the latest `NaraMergeUtil.scala`.

### macOS OOM Killer Terminates Spark

The macOS OOM killer (`jetsam`) silently kills processes when system memory is exhausted.
Symptoms: process disappears with no Java exception in logs.

**Fix**: Reduce heap size to leave room for the OS (at least 4 GB free). With 18 GB RAM:
- Use `-Xmx12g` for merge (leaves 6 GB for OS)
- Use `-Xmx14g` for pipeline (leaves 4 GB for OS)
- Never use `-Xmx16g+` on an 18 GB machine

### sbt Buffers Output (Can't See Progress)

When running via `sbt`, the forked JVM's stdout is buffered. Log messages may not appear
for minutes. To monitor progress:

```bash
# Check the actual Spark process
jps -l | grep -i "nara\|IngestRemap\|MergeUtil"

# Check process vitals
ps -o pid,pcpu,pmem,rss,etime -p <PID>

# Check Spark temp disk usage
du -sh /private/var/folders/*/T/blockmgr-* 2>/dev/null
```

---

## Directory Layout

```
~/dpla/data/nara/
  originalRecords/           # Raw ZIP files from NARA (per month)
    202512/
      2025-11-29_21.018_DESC_0001.zip
      ...
    202601/
      2026-02-07_17.115_DESC_0001.zip
      ...

  delta/                     # Preprocessed deltas (per month)
    202512/
      10.018_nara_delta.tar.gz        # one tar.gz per export group
      21.018_nara_delta.tar.gz
      deletes/                        # delete XMLs
        2025-11-29_10.008_DESC_NaidsList.xml
      harvest/                        # delta harvest output
        nara/harvest/
          20260209_HHMMSS-nara-OriginalRecord.avro

    202601/
      09.113_nara_delta.tar.gz
      17.115_nara_delta.tar.gz
      ...
      deletes/
        2026-02-07_14.041_DESC_NaidsList.xml
        ...
      harvest/
        nara/harvest/
          20260209_HHMMSS-nara-OriginalRecord.avro

  harvest/                   # Merged base harvests (cumulative)
    20250429_000000-nara-OriginalRecord.avro    # from S3
    202512_000000-nara-OriginalRecord.avro      # Dec merge output
    20260209_000000-nara-OriginalRecord.avro/   # Jan merge output (current)
      _LOGS/
        _SUMMARY.txt
        ops/
          part-00000-*.csv    # insert/update/delete operations

  mapping/                   # Pipeline output
    YYYYMMDD_HHMMSS-nara-MAP4_0.MAPRecord.avro/

  enrichment/
    YYYYMMDD_HHMMSS-nara-MAP4_0.EnrichRecord.avro/

  jsonl/
    YYYYMMDD_HHMMSS-nara-MAP3_1.IndexRecord.jsonl/
```

---

## History

| Date | Base | Delta | New | Updates | Deletes | Final |
|------|------|-------|-----|---------|---------|-------|
| 2025-04-29 | -- | -- | -- | -- | -- | 18,687,707 |
| 2025-12 | 18,687,707 | 9,247 | 8,432 | 815 | 0 | 18,696,139 |
| 2026-01 | 18,696,139 | 298,459 (297,144 unique) | 93,856 | 203,288 | 0 | 18,789,995 |

---

## Key Scala Entry Points

| Class | Purpose |
|-------|---------|
| `dpla.ingestion3.entries.ingest.HarvestEntry` | Runs NaraDeltaHarvester to harvest delta XML |
| `dpla.ingestion3.utils.NaraMergeUtil` | Merges delta harvest into base + processes deletes |
| `dpla.ingestion3.entries.ingest.IngestRemap` | Full pipeline: mapping -> enrichment -> JSON-L |
| `dpla.ingestion3.entries.ingest.MappingEntry` | Mapping only |
| `dpla.ingestion3.entries.ingest.EnrichEntry` | Enrichment only |
| `dpla.ingestion3.entries.ingest.JsonlEntry` | JSON-L export only |
| `dpla.ingestion3.harvesters.file.NaraDeltaHarvester` | Reads tar.gz XML files, writes Avro |

### i3.conf NARA Settings

```hocon
nara.provider = "National Archives and Records Administration"
nara.harvest.type = "nara.file.delta"
nara.harvest.endpoint = "/Users/you/dpla/data/nara/delta/YYYYMM/"
nara.harvest.delta.update = "/Users/you/dpla/data/nara/delta/YYYYMM/"
nara.s3_destination = "s3://dpla-master-dataset/nara/"
```

Both `endpoint` and `delta.update` must point to the delta directory for the month
being processed. Update these before each harvest step.
