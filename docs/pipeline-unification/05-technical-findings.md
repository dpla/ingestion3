# DPLA Pipeline Unification -- Technical Findings

**Audience:** Engineers performing implementation, AI agents executing code changes
**Usage:** Reference document and work queue. Each finding is an actionable item with file paths and proposed fixes.

---

## How to Use This Document

This appendix lists every technical issue discovered during analysis of all three repositories. Items are organized by repository and severity. Each item includes:

- **Severity:** CRITICAL (must fix before automation), HIGH (fix in Phase 1), MEDIUM (fix when convenient), LOW (accept or defer)
- **File path and line numbers** (as of February 2026; may drift as code changes)
- **What's wrong** and why it matters
- **Proposed fix** with enough detail to implement
- **Roadmap step** linking to the implementation plan in [04-implementation-roadmap.md](04-implementation-roadmap.md)
- **Status:** OPEN, IN PROGRESS, or RESOLVED

---

## 1. ingestion3 Findings

### CRITICAL

#### I3-C1: Non-Atomic State File Writes

**File:** `scheduler/orchestrator/state.py`, line 177
**Roadmap:** P1-1 | **Status:** OPEN

The orchestrator writes state to `logs/orchestrator_state.json` using `write_text()`, which is not atomic. If the process crashes mid-write, the file is corrupted. On the next startup, `json.loads()` fails, and the state is silently reset to empty -- losing all run history.

**Impact:** The orchestrator cannot resume a partially-completed run. An operator must manually determine which hubs are done and which need re-running.

**Proposed fix:** Write to a temporary file in the same directory, fsync, then atomic rename:

```python
import tempfile, os

def _save_state(self):
    data = json.dumps(self._state, indent=2)
    fd, tmp_path = tempfile.mkstemp(
        dir=self._state_path.parent, suffix=".tmp"
    )
    try:
        os.write(fd, data.encode())
        os.fsync(fd)
        os.close(fd)
        os.rename(tmp_path, self._state_path)
    except:
        os.close(fd)
        os.unlink(tmp_path)
        raise
```

#### I3-C2: Division by Zero in Anomaly Detector

**File:** `scheduler/orchestrator/anomaly_detector.py`, lines 443 and 469
**Roadmap:** P1-2 | **Status:** OPEN

When comparing current run counts to the S3 baseline, the code divides by `baseline_mapping.successful` and `baseline_mapping.attempted`. If a hub has never been ingested (baseline is zero) or the baseline is corrupted, these divide by zero.

**Proposed fix:**

```python
if baseline_mapping.successful == 0:
    return AnomalyResult.PASS
```

#### I3-C3: Command Injection in S3 Listing

**File:** `scheduler/orchestrator/anomaly_detector.py`, line 242
**Roadmap:** P1-3 | **Status:** OPEN

Hub names from i3.conf are interpolated into a shell command via `subprocess.run()` with `shell=True`. If a hub name contains shell metacharacters, arbitrary commands could execute.

**Proposed fix:** Use `subprocess.run()` with `shell=False` and list arguments:

```python
result = subprocess.run(
    ["aws", "s3", "ls", f"s3://{bucket}/{prefix}/jsonl/", "--profile", "dpla"],
    capture_output=True, text=True
)
```

### HIGH

#### I3-H1: Race Condition in Parallel State Updates

**File:** `scheduler/orchestrator/main.py`, line 160
**Roadmap:** P1-4 | **Status:** OPEN

State file writes from concurrent hub processors are not synchronized. Two hubs completing simultaneously can corrupt the state file.

**Proposed fix:** Add a file lock or use a thread-safe queue for state updates.

#### I3-H2: No Post-Sync Verification

**File:** `scripts/s3-sync.sh`
**Roadmap:** P1-5 | **Status:** OPEN

`aws s3 sync` can fail silently. No verification that the sync completed successfully.

**Proposed fix:** After sync, compare local `_MANIFEST` to S3 listing. Verify `_SUCCESS` marker exists in S3.

#### I3-H3: JAR Build Race Condition

**File:** `scripts/common.sh`, line 365
**Roadmap:** P1-6 | **Status:** OPEN

Parallel ingests may trigger concurrent `sbt assembly` builds.

**Proposed fix:** Use `flock` around the build check.

#### I3-H4: Hardcoded Default Paths

**File:** `scheduler/orchestrator/config.py`, lines 234--236
**Roadmap:** P0-7 | **Status:** OPEN

`I3_HOME` defaults to `/Users/scott/dpla/code/ingestion3`. Works on one machine only.

**Proposed fix:** Require as environment variables. Fail with clear error if not set.

### MEDIUM

#### I3-M1: Anomaly Detection Thresholds Are Global

**File:** `scheduler/orchestrator/anomaly_detector.py`, lines 221--227
**Roadmap:** P2-8 | **Status:** OPEN

All hubs use the same thresholds. A hub that legitimately removes a collection triggers a critical halt.

**Proposed fix:** Add per-hub threshold overrides in i3.conf.

#### I3-M2: Iterator Contract Violation in OAI Harvester

**File:** `src/main/scala/dpla/ingestion3/harvesters/oai/OaiMultiPageResponseBuilder.scala`, line 86
**Roadmap:** P1-7 | **Status:** OPEN

The iterator's `next()` throws `RuntimeException` instead of `NoSuchElementException`.

**Proposed fix:** Change to `throw new NoSuchElementException("No more OAI pages")`.

#### I3-M3: Notification Script Timeout

**File:** `src/main/scala/dpla/ingestion3/executors/HarvestExecutor.scala`, line 168
**Roadmap:** P2-9 | **Status:** OPEN

15-second timeout may be too short. If killed, the notification is lost.

**Proposed fix:** Increase to 30 seconds. Log when timeout is hit.

#### I3-M4: Missing Environment Variables in .env.example

**File:** `.env.example`
**Roadmap:** P0-6 | **Status:** OPEN

`DPLA_DATA` and `I3_CONF` not documented.

**Proposed fix:** Add all required variables with comments.

### LOW

#### I3-L1: Java Version Detection Fragility

**File:** `scripts/common.sh`, lines 120--173
**Roadmap:** Defer | **Status:** OPEN

macOS `java_home` may return a different version than `JAVA_HOME` points to. Acceptable; document that `JAVA_HOME` must be set in `.env`.

#### I3-L2: Status File Write Failures Silently Swallowed

**File:** `scheduler/orchestrator/state.py`, lines 250--254
**Roadmap:** Defer | **Status:** OPEN

Per-hub status writes swallow `OSError`. Should log at WARNING level.

---

## 2. sparkindexer Findings

### CRITICAL

#### SI-C1: Silent ES Write Failures

**File:** `src/main/scala/dpla/ingestion3/indexer/ElasticSearchWriter.scala`, lines 40--44
**Roadmap:** P1-8 | **Status:** OPEN

ES write failures are caught, printed, and discarded. The job reports success. Records can fail to write without anyone knowing.

**Impact:** This is the most dangerous silent failure in the pipeline. A significant fraction of records can be lost from the index, and the alias flip proceeds as if nothing is wrong.

**Proposed fix:** Exit non-zero on write failure:

```scala
val result = Try(EsSpark.saveJsonToEs(rdd, s"$indexName/item", configs))
result match {
    case Success(_) => ()
    case Failure(e) =>
        System.err.println(s"FATAL: ES write failed: ${e.getMessage}")
        System.exit(1)
}
```

#### SI-C2: IndexerMain Always Exits 0

**File:** `src/main/scala/dpla/ingestion3/indexer/IndexerMain.scala`
**Roadmap:** P1-9 | **Status:** OPEN

No `System.exit(1)` calls. Any caught exception results in a successful exit.

**Proposed fix:** Wrap main body in try-catch that calls `System.exit(1)` on unhandled exceptions.

#### SI-C3: Hardcoded AWS Resource IDs in EMR Script

**File:** `sparkindexer-emr.sh`
**Roadmap:** P1-10 | **Status:** OPEN

Security groups, subnet, instance profile, and service role are all hardcoded.

**Proposed fix:** Extract to `emr-config.json`:

```json
{
    "subnet_id": "subnet-90afd9ba",
    "security_groups": {
        "master": "sg-07459c7a",
        "slave": "sg-0e4ef863"
    },
    "instance_profile": "sparkindexer-s3",
    "service_role": "EMR_DefaultRole",
    "log_uri": "s3://dpla-sparkindexer/logs/",
    "master_instance_type": "m5.2xlarge",
    "core_instance_type": "m5.2xlarge",
    "core_instance_count": 9
}
```

### HIGH

#### SI-H1: Timestamp Comparison Bug in MasterDataset

**File:** `src/main/scala/dpla/ingestion3/indexer/MasterDataset.scala`, line 52
**Roadmap:** P1-11 | **Status:** OPEN

`maxTimestamp="now"` works by accident (string `"n"` sorts after digits). Malformed timestamps produce wrong results silently.

**Proposed fix:** Handle `"now"` explicitly:

```scala
val isBeforeMax = if (maxTimestamp == "now") true
                  else key.compareTo(f"${provider}jsonl/$maxTimestamp") < 1
```

#### SI-H2: No Pre-Flight Health Checks

**File:** `src/main/scala/dpla/ingestion3/indexer/IndexerMain.scala`
**Roadmap:** P1-12 | **Status:** OPEN

No verification of ES health, S3 data presence, or index name availability before starting.

**Proposed fix:** Add pre-flight checks: ES cluster health, S3 path listing, index name uniqueness.

#### SI-H3: Alias Flip Script Race Condition

**File:** `scripts/flip-alias.sh`
**Roadmap:** P1-13 | **Status:** OPEN

Script reads current alias, then later flips. Another process could flip in between.

**Proposed fix:** Use conditional `_aliases` API that specifies expected current target.

#### SI-H4: Resource Leak in Index.scala

**File:** `src/main/scala/dpla/ingestion3/indexer/Index.scala`, line 27
**Roadmap:** P1-14 | **Status:** OPEN

`getResourceAsStream` not properly closed. Uses deprecated `closeQuietly()`.

**Proposed fix:** Use `scala.util.Using` pattern.

#### SI-H5: ES Connection Uses HTTP, Not HTTPS

**File:** `src/main/scala/dpla/ingestion3/indexer/Index.scala`, lines 38, 46
**Roadmap:** Phase 2 | **Status:** OPEN

Connections use `http://`. Should be configurable, defaulting to `https://`.

### MEDIUM

#### SI-M1: Hardcoded ES Host in TagReportWriter

**File:** `src/main/scala/dpla/ingestion3/indexer/TagReportWriter.scala`, line 39
**Roadmap:** Phase 2 | **Status:** OPEN

`"search.internal.dp.la"` is hardcoded. Should be a parameter.

#### SI-M2: S3 Public Read Permission in TagReportWriter

**File:** `src/main/scala/dpla/ingestion3/indexer/TagReportWriter.scala`, lines 212--221
**Roadmap:** Phase 2 | **Status:** OPEN

Grants `GroupGrantee.AllUsers` on S3 objects. Review for handoff.

#### SI-M3: Deprecated ES API Usage

**File:** `src/main/scala/dpla/ingestion3/indexer/Index.scala`, line 38
**Roadmap:** Phase 2 | **Status:** OPEN

`include_type_name=true` deprecated in ES 7.x, removed in ES 8+.

#### SI-M4: Unsafe Type Casts in DplaDataRelation

**File:** `src/main/scala/dpla/datasource/DplaDataRelation.scala`, lines 74--75
**Roadmap:** Phase 2 | **Status:** OPEN

`asInstanceOf[String]` will throw `ClassCastException` on unexpected types.

### LOW

#### SI-L1: Scala Version Mismatch with ingestion3

**Files:** ingestion3 (Scala 2.13.15), sparkindexer (Scala 2.12.18)
**Roadmap:** Defer | **Status:** OPEN

No current impact. Note for future code sharing.

#### SI-L2: No Test Coverage for S3 and ES Operations

**Roadmap:** Phase 2 | **Status:** OPEN

No tests for MasterDataset, ElasticSearchWriter, Index, or alias management.

---

## 3. ingest-wikimedia Findings

### CRITICAL

#### WM-C1: pywikibot Session Timeout Unhandled

**File:** `ingest_wikimedia/wikimedia.py`, lines 205--209
**Roadmap:** P1-16 | **Status:** OPEN

`get_site()` calls `site.login()` once at startup. If the session expires during weeks-long uploads, uploads silently fail.

**Impact:** Root cause of undetected upload failures. The process appears alive but makes no progress.

**Proposed fix:**

```python
def ensure_logged_in(site):
    if not site.logged_in():
        site.login()
    try:
        site.userinfo
    except Exception:
        site.login()
```

#### WM-C2: No Checkpoint/Resume Capability

**File:** `tools/uploader.py`, `tools/downloader.py`
**Roadmap:** P1-15 | **Status:** OPEN

Crashes require re-processing the entire ID list from the beginning.

**Proposed fix:** Write checkpoint file after each item. On restart, resume from checkpoint.

#### WM-C3: No Signal Handler for Graceful Shutdown

**File:** `tools/uploader.py`, `tools/downloader.py`
**Roadmap:** P1-17 | **Status:** OPEN

SIGTERM/SIGINT may leave incomplete uploads in unknown state.

**Proposed fix:** Signal handler sets `shutdown_requested` flag; main loop saves checkpoint and exits.

### HIGH

#### WM-H1: Silent Failure Pattern (Broad Exception Catching)

**Files:** `tools/downloader.py` lines 183--185, 254--259; `tools/uploader.py` lines 180--181, 246--250
**Roadmap:** P1-18 | **Status:** OPEN

Failed items logged but not written to structured error file.

**Proposed fix:** Write `{partner}/logs/failures.jsonl` with `dpla_id`, error type, timestamp per failed item.

#### WM-H2: In-Memory-Only Tracker

**File:** `ingest_wikimedia/tracker.py`, lines 18--21
**Roadmap:** P1-19 | **Status:** OPEN

Tracker data lost on crash.

**Proposed fix:** Flush to disk every 100 items or 5 minutes.

#### WM-H3: Unbounded Log Files

**File:** EC2 instance, ~13GB total
**Roadmap:** P2-4 | **Status:** OPEN

No log rotation. NARA log alone is 167MB. BPL logs total 4GB.

**Proposed fix:** Install logrotate with `copytruncate` (avoids restarting uploaders).

#### WM-H4: IIIF URL Maximization Fragility

**File:** `ingest_wikimedia/iiif.py`, lines 79--180
**Roadmap:** Phase 2 | **Status:** OPEN

Regex patterns fail silently for unrecognized URL structures.

**Short-term fix:** Log WARNING when no regex matches.
**Long-term fix:** Use IIIF Image API `info.json` instead of regex heuristics.

### MEDIUM

#### WM-M1: Banlist Loaded Once at Startup

**File:** `ingest_wikimedia/banlist.py`, lines 8--10
**Roadmap:** Phase 2 | **Status:** OPEN

123-entry banlist never refreshed during run. Changes require restart.

**Proposed fix:** Check file modification time periodically; reload if changed.

#### WM-M2: NARA URL Data Quality Hack

**File:** `tools/downloader.py`, lines 274--275
**Roadmap:** Phase 2 | **Status:** OPEN

Runtime workaround for `https/` -> `https://`. Should be fixed in ingestion3's NARA mapping.

#### WM-M3: No Rate Limiting for DPLA API

**File:** `ingest_wikimedia/dpla.py`
**Roadmap:** Phase 2 | **Status:** OPEN

No explicit rate limiting. Current sequential processing naturally limits rate, but parallelization would need throttling.

#### WM-M4: Config Loading Has No Error Handling

**File:** `ingest_wikimedia/tools_context.py`, lines 56--67
**Roadmap:** P1-20 | **Status:** OPEN

Missing or malformed `config.toml` gives raw traceback.

**Proposed fix:** Wrap in try-except with clear error message.

### LOW

#### WM-L1: Python 3.13 Requirement

**File:** `pyproject.toml`, line 13
**Roadmap:** Defer | **Status:** OPEN

Python 3.13 is very new. Consider whether 3.11+ would suffice.

#### WM-L2: No Parallel Downloads

**File:** ROADMAP.md, lines 18--19
**Roadmap:** Defer | **Status:** OPEN

Sequential downloads are a bottleneck for multi-page documents.

#### WM-L3: Tracker Not Thread-Safe

**File:** `ingest_wikimedia/tracker.py`, lines 23--24
**Roadmap:** Defer | **Status:** OPEN

No impact today (single-threaded). Would need fixing for parallelization.

---

## 4. Cross-Project Integration Findings

### CRITICAL

#### XP-C1: S3 Path Format Has No Explicit Contract

**Files:** Producer: `ingestion3/OutputHelper.scala` line 88. Consumer: `sparkindexer/MasterDataset.scala` line 51.
**Roadmap:** P0-3 | **Status:** OPEN

No shared definition or test verifying path format agreement.

**Proposed fix:** Document contract (done in [03-integration-contracts-and-gates.md](03-integration-contracts-and-gates.md)). Add cross-project integration test.

#### XP-C2: Hub Name Mapping Has Three Separate Sources

**Files:** `config.py` lines 118--121, `institutions_v2.json`, sparkindexer ES content.
**Roadmap:** P0-2 | **Status:** OPEN

No centralized source of truth for hub naming.

**Proposed fix:** Create canonical `hub-registry.json`.

### HIGH

#### XP-H1: institutions_v2.json Branch Mismatch

**Files:** `dpla.py` line 365 (fetches `main`), `sdc-sync.py` line 35 (fetches `develop`).
**Roadmap:** P0-5 | **Status:** OPEN

Inconsistent branch references cause inconsistent behavior.

**Proposed fix:** Standardize all references to `main`.

#### XP-H2: No DPLA API Field Versioning

**Roadmap:** P0-4 | **Status:** OPEN

No schema definition or compatibility check for API field dependencies.

**Proposed fix:** Document field contract. Add health check that verifies expected fields exist.

### MEDIUM

#### XP-M1: Provider Name Matching Must Be Exact

**Files:** `dpla.py` line 1078, `institutions_v2.json`
**Roadmap:** Phase 0 | **Status:** OPEN

Trailing space, case difference, or name change breaks lookup silently.

**Proposed fix:** Case-insensitive lookup with whitespace normalization. Log WARNING on normalized match.

---

## Finding Summary by Severity

| Severity | ingestion3 | sparkindexer | ingest-wikimedia | Cross-Project | Total |
|----------|-----------|--------------|------------------|---------------|-------|
| CRITICAL | 3 | 3 | 3 | 2 | 11 |
| HIGH | 4 | 5 | 4 | 2 | 15 |
| MEDIUM | 4 | 4 | 4 | 1 | 13 |
| LOW | 2 | 2 | 3 | 0 | 7 |
| **Total** | **13** | **14** | **14** | **5** | **46** |

Phase 0 addresses 5 cross-project findings. Phase 1 addresses all 11 CRITICAL and most HIGH findings. Phases 2+ address remaining MEDIUM and LOW findings.

---

## 5. AWS Infrastructure Reference

This section documents AWS-specific details needed for implementation and handoff. These values are current as of February 2026 and should be verified before use.

### IAM Roles and Policies

**For Ingestion EC2 (if moving off laptop):**
- S3: `GetObject`, `PutObject`, `ListBucket`, `DeleteObject` on `arn:aws:s3:::dpla-master-dataset/*`
- SES: `SendEmail`, `SendRawEmail` (for hub contact emails)

**For EMR (existing):**
- Service role: `EMR_DefaultRole` (AWS managed)
- Instance profile: `sparkindexer-s3` (needs S3 read for `dpla-master-dataset`, ES write access)
- These roles must be documented with exact policies before handoff

**For Lambda Coordinator (if implemented in Phase 3+):**
- SSM: `SendCommand`, `GetCommandInvocation` (for EC2 command execution)
- EMR: `RunJobFlow`, `DescribeStep`, `TerminateJobFlows`, `ListClusters`
- SNS: `Publish` to `arn:aws:sns:us-east-1:*:dpla-pipeline-*`

### CloudWatch Alarms (Phase 2--3)

| Alarm | Metric | Threshold | Action |
|-------|--------|-----------|--------|
| EMR cluster running too long | EMR step duration | > 4 hours | SNS -> Slack + email |
| EC2 disk usage high | CloudWatch agent disk% | > 80% | SNS -> Slack |
| EC2 memory low | CloudWatch agent mem_available | < 500MB | SNS -> Slack |
| Ingestion EC2 unhealthy | StatusCheckFailed | > 0 for 5 min | SNS -> Slack + email |

### SNS Topic Configuration

```
Topic: dpla-pipeline-alerts
  Subscriptions:
    - Protocol: HTTPS, Endpoint: {Slack webhook URL}
    - Protocol: email, Endpoint: tech@dp.la
```

Using SNS as the notification hub (rather than direct Slack webhooks) adds email fallback and makes it easy to add subscribers.

### Security Groups (sparkindexer EMR)

These are referenced in `sparkindexer-emr.sh` and must be verified and documented before handoff:
- Master SG: `sg-07459c7a` -- inbound from Spark workers, outbound to ES
- Slave SG: `sg-0e4ef863` -- inbound from master, outbound to S3
- Service Access SG: `sg-6af5b311` -- EMR service access

If the VPC changes, all security groups must be recreated.

### S3 Bucket Inventory

| Bucket | Purpose | Access Pattern | Lifecycle |
|--------|---------|---------------|-----------|
| `dpla-master-dataset` | Hub JSONL and Avro data | Read/write by ingestion3; read by sparkindexer | Retain indefinitely |
| `dpla-wikimedia` | Downloaded media staging | Read/write by ingest-wikimedia | Retain indefinitely |
| `dpla-sparkindexer` | Sparkindexer JAR and EMR logs | Write by build; read by EMR | Retain logs 90 days |

### Elasticsearch Cluster

| Property | Value | Notes |
|----------|-------|-------|
| Host | `search.internal.dp.la` | Internal DNS, not public |
| Port | 9200 | HTTP (should migrate to HTTPS, see SI-H5) |
| Active alias | `dpla_alias` | Points to current `dpla-all-{timestamp}` index |
| Index settings | 5 shards, 1 replica | Set during creation by sparkindexer |
| Document count | ~40 million (varies) | Verify against previous index before alias flip |
