# i3.conf Hub Key Normalization Refactor

**Status:** Pending review and validation  
**Risk:** Medium-high — touches i3.conf (shared config), CHProviderRegistry.scala, orchestrator Python, and scripts  
**Blast radius:** Any code that passes a hub name as a string argument or reads i3.conf keys

---

## Background

Two hub keys in i3.conf do not match their S3 folder names, requiring lookup tables
in 5 separate files to paper over the mismatch:

| i3.conf key | `getProviderName` (DPLA ID prefix) | S3 folder |
|---|---|---|
| `hathi` | `hathitrust` | `hathitrust` |
| `tn` | `tn` | `tennessee` |

These two mismatches are the **sole reason** `S3_PREFIX_MAP` / `S3_PREFIX_TO_HUB` lookup
tables exist across `config.py`, `s3-sync.sh`, `check-jsonl-sync.sh`, `backlog_emails.py`,
and `staged_report.py`.

All other hubs are already consistent across i3.conf key, DPLA ID prefix, and S3 folder.

---

## Cross-project impact assessment

| Project | Reads i3.conf? | Uses hub shortnames? | Impact of rename |
|---|---|---|---|
| ingestion3 | Yes | Yes — i3.conf keys, CHProviderRegistry | Directly affected — see changes below |
| sparkindexer | No | Yes — but uses S3 folder names directly | None — already uses `hathitrust`/`tennessee` |
| wikimedia-ingest | No | Yes — DPLA_PARTNERS dict, no `hathi`/`tn` entries | None |

sparkindexer discovers providers by listing S3 folder names in `dpla-master-dataset`.
It already uses `hathitrust` and `tennessee` as provider identifiers. This rename
aligns ingestion3 to match what sparkindexer already uses.

wikimedia-ingest uses `config.toml` for secrets and fetches hub/institution mappings
from `src/main/resources/wiki/institutions_v2.json` (display names only, no shortnames).
Completely unaffected.

---

## Hard constraint: no DPLA ID changes, no Scala renames

`getProviderName` in `TnMapping.scala` (`"tn"`) and `HathiMapping.scala` (`"hathitrust"`)
must not change — these values are baked into millions of existing DPLA record IDs
(`tn--{id}`, `hathitrust--{id}`). **No Scala file renames. No class renames. No
`getProviderName` changes.**

After the rename, `tn` will remain the DPLA ID prefix for the hub whose config key
and S3 folder are both `tennessee`. This divergence is intentional and pre-existing
(the S3 folder has always been `tennessee` while the local config key was `tn`).

---

## Baseline test results (Feb 24, 2026)

`sbt test` run before any changes:

- **1,174 tests passed, 0 failed**
- 68 suites, 0 aborted
- Run time: ~13 seconds

Re-run `sbt test` after step 2 (CHProviderRegistry.scala) as the first checkpoint.

---

## Changes required

### 1. `~/dpla/code/ingestion3-conf/i3.conf`

Rename all `hathi.*` keys to `hathitrust.*` and all `tn.*` keys to `tennessee.*`.
No other changes to this file.

### 2. `src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala`

Two string key changes only — no file rename, no class rename:

```scala
// Before:
"hathi" -> new HathiProfile,
"tn"    -> new TnProfile,

// After:
"hathitrust" -> new HathiProfile,
"tennessee"  -> new TnProfile,
```

**Run `sbt test` after this step.** All 1,174 tests should still pass.

### 3. `scheduler/orchestrator/config.py`

Remove `S3_PREFIX_MAP` (lines 117–121) and the `get_s3_prefix()` method (lines 190–192).
Any caller that needs the S3 folder for a hub should read `hub_config.s3_destination`
and parse the trailing path component directly from i3.conf.

### 4. `scripts/s3-sync.sh`

Remove the `case` statement that maps `hathi → hathitrust` and `tn → tennessee`.
After the i3.conf rename the hub key and S3 folder name are identical — no mapping needed.

### 5. `scripts/check-jsonl-sync.sh`

Remove `get_s3_hub_name()` function (the `case tn) echo "tennessee"` block).
Same reason as above.

### 6. `scheduler/orchestrator/backlog_emails.py`

Remove `S3_PREFIX_TO_HUB = {"hathitrust": "hathi", "tn": "tennessee"}` (lines 31–34)
and update all usages to use the hub key directly from i3.conf.

### 7. `scheduler/orchestrator/staged_report.py`

Remove `S3_PREFIX_TO_HUB = {"hathitrust": "hathi", "tn": "tennessee"}` (lines 26–29)
and update usages.

### 8. `scheduler/collect-hub-data.py`

Update reverse-mapping entries:
- Line 97: `"HathiTrust": "hathi"` → `"HathiTrust": "hathitrust"`
- Line 116: Remove `"tennessee": "tn"` special-case entry (now matches directly)

### 9. Test files

- `scheduler/orchestrator/tests/conftest.py` lines 32–33: update `S3_PREFIX_MAP`
  fixture entries to reflect renamed keys
- `scheduler/orchestrator/tests/test_config.py` lines 130–131: remove or update
  `get_s3_prefix("hathi")` and `get_s3_prefix("tn")` assertions (method is being removed)

---

## Validation checklist

- [ ] Baseline: `sbt test` passes (1,174/1,174) — confirmed Feb 24, 2026
- [ ] After step 2: `sbt test` passes again (key checkpoint)
- [ ] `./venv/bin/python -m pytest scheduler/orchestrator/tests/` passes
- [ ] `./venv/bin/python -m scheduler.orchestrator.main --dry-run --hub=hathitrust,tennessee` runs without error
- [ ] Confirm local data dir for `hathi` (`$DPLA_DATA/hathi/`) and plan migration to `$DPLA_DATA/hathitrust/` if needed
- [ ] Confirm local data dir for `tn` (`$DPLA_DATA/tn/`) and plan migration to `$DPLA_DATA/tennessee/` if needed
- [ ] Smoke test `./scripts/s3-sync.sh hathitrust` and `./scripts/s3-sync.sh tennessee`
- [ ] Verify no other files in ingestion3, sparkindexer, or wikimedia-ingest reference `hathi` or `tn` as hub shortnames after changes

---

## Deferred follow-on work

These were identified in the same audit but are out of scope for this change:

| Issue | Description |
|---|---|
| `harvest.s3_source_bucket` | 10 file-based hubs have S3 source buckets hardcoded in `config.py` (`S3_SOURCE_BUCKETS` dict) and `auto-ingest.sh` (`case` statement). Should move to i3.conf. NARA's entry needs a comment noting the bucket is externally owned. |
| `S3_DEST_BUCKET` | `dpla-master-dataset` hardcoded in 6 files. Should move to `.env` as `S3_DEST_BUCKET`. |
| Smithsonian hub-name checks | `hub_processor.py` and `diagnostics.py` check `hub == "smithsonian"` instead of a config flag. Should add `smithsonian.harvest.preprocess_type = "xmll"` to i3.conf. |
