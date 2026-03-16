---
name: dpla-takedown
description: Remove DPLA items from the live site and search index in response to takedown requests. Use when the user says take down, delete, remove item(s) from dp.la, or action a takedown request. Accepts DPLA IDs directly or criteria (hub, institution, collection) to generate an ID list. Includes mandatory pre-flight verification, search-before-delete on Elasticsearch, and post-deletion confirmation with count delta verification.
---

# DPLA Takedown

## Purpose

Safely remove one or more DPLA items from the live dp.la search index and from the
S3 master dataset (so they do not reappear on the next full index rebuild). This is
the most sensitive operation in the ingestion system — an error here affects the
production aggregation directly.

## When to Use

- "Take down item X"
- "Delete this item from dp.la"
- "Remove these records"
- "Action this takedown request"
- "Remove all items from [institution] in [hub]"
- "Delete collection X from [hub]"

## Threat Model

| Risk | Mitigation |
|------|-----------|
| Malformed ES query deletes wrong records | Mandatory search-before-delete: run the exact query as `_search` first, confirm hit count matches expected count, show results to user |
| SSM command escaping corrupts query JSON | Write all SSM payloads to JSON files and pass via `--cli-input-json file://...` — no shell escaping |
| S3 JSONL file corrupted during filter/upload | S3 versioning is enabled on `dpla-master-dataset` — previous versions are recoverable |
| Network failure during S3 re-upload | Re-run the S3 delete step; the script is idempotent (re-filtering an already-filtered file is a no-op) |
| Wrong hub identified for S3 delete | Derive hub from DPLA API metadata, not user input; cross-check with user before proceeding |
| Criteria-based query returns too many IDs | Always show the full count AND a sample of records to the user; require explicit confirmation before proceeding |
| Record reappears after next harvest | Warn user that upstream source must also remove the record; no persistent exclusion list exists |

## Prerequisites

- **AWS credentials:** The `dpla` AWS profile must have access to the DPLA account (283408157088). Fall back to `default` if `dpla` is absent.
- **DPLA API key:** Stored in `~/.claude/secrets/dpla.env` as `DPLA_API_KEY`. Load with: `. ~/.claude/secrets/dpla.env`
- **S3 access:** Read/write to `s3://dpla-master-dataset/`.
- **SSM access:** `ssm:SendCommand` and `ssm:GetCommandInvocation` on ES nodes (`i-00bbdfe0a6ff6cf78` = search-prod1).
- **Python with boto3:** Run the preflight block below before Phase 1.

## Pre-Run Snapshot (MANDATORY for count delta verification)

Before any other step, capture a baseline. This is used in Phase 4 to verify only the expected records were removed.

```bash
. ~/.claude/secrets/dpla.env
TOTAL_BEFORE=$(curl -s "https://api.dp.la/v2/items?api_key=$DPLA_API_KEY&page_size=1" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "Total DPLA items before: $TOTAL_BEFORE"
```

Also capture the hub count once the hub is known (after Phase 0/1 identifies it):

```bash
HUB_BEFORE=$(curl -s "https://api.dp.la/v2/items?api_key=$DPLA_API_KEY&page_size=1&provider.name=<hub-name>" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "Hub items before: $HUB_BEFORE"
```

## Python Preflight (run once before Phase 2)

Run this single block to discover the correct Python binary with boto3 installed:

```bash
PYTHON=""
for p in "./venv/bin/python" "/opt/homebrew/bin/python3.9" "/opt/homebrew/bin/python3" "python3"; do
  if command -v "$p" &>/dev/null && "$p" -c "import boto3" 2>/dev/null; then
    PYTHON="$p"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  # boto3 not installed — attempt install then retry
  pip3 install boto3 -q && PYTHON="python3"
fi
echo "PYTHON=$PYTHON"
"$PYTHON" --version
```

Record the value of `$PYTHON` — use it verbatim in all subsequent Python invocations.

## Workflow

### Phase 0: Determine the ID list

The user provides EITHER:

**A) Explicit IDs** — one or more 32-character DPLA IDs (hex strings). Proceed to Phase 1.

**B) Criteria** — e.g. "all items from [institution] in [hub]", or "collection X". In this case:

1. Query the DPLA API to generate the list. Use `provider.@id` and/or `dataProvider` and/or `sourceResource.collection.title` filters with `page_size=500` and paginate to collect all matching IDs:
   ```bash
   . ~/.claude/secrets/dpla.env
   curl -s "https://api.dp.la/v2/items?provider.@id=http://dp.la/api/contributor/<hub>&dataProvider=<institution>&page_size=500&page=1&api_key=$DPLA_API_KEY"
   ```
2. **STOP and show the user:**
   - Total count of matching records
   - The query used
   - A sample of 5-10 records (ID, title, dataProvider, isShownAt)
   - Ask: "This will delete N records. Please confirm."
3. Only proceed after explicit user confirmation.

### Phase 1: Pre-flight verification (MANDATORY — never skip)

For every ID in the list:

1. **Look up via DPLA API:**
   ```bash
   . ~/.claude/secrets/dpla.env
   curl -s "https://api.dp.la/v2/items/<id>?api_key=$DPLA_API_KEY"
   ```
2. **Extract and record:**
   - `id` — the DPLA ID
   - `provider.@id` — extract hub short name (last path segment, e.g. `orbiscascade`)
   - `provider.name` — hub display name (used for hub count snapshot)
   - `dataProvider` — contributing institution
   - `sourceResource.title` — item title (first value)
   - `isShownAt` — link to original item
3. **Verify the item exists.** If the API returns no results, warn the user and skip that ID.
4. **Determine S3 hub prefix.** The API slug (e.g. `orbiscascade`) may differ from the S3 prefix (e.g. `orbis-cascade`). Check the actual S3 prefix:
   ```bash
   aws s3 ls s3://dpla-master-dataset/ --profile dpla | grep -i <hub-slug>
   ```
5. **Group IDs by hub.** All IDs in a single S3 delete run must belong to the same hub.
6. **Capture the hub baseline count** (see Pre-Run Snapshot above) using the hub's `provider.name`.
7. **Present the full manifest to the user:**

   ```
   Takedown Manifest
   =================
   Items: N
   Hub(s): <hub list>

   ID                                Title                              DataProvider
   --------------------------------  -----------------------------------  --------------------------
   57d000d66004c73c5e31ea7dc7f57201  "Letter from John Adams, 1776"      Massachusetts Historical Society
   ...

   This will:
   1. Remove these items from the S3 master dataset (s3://dpla-master-dataset/<hub>/jsonl/)
   2. Remove these items from the live Elasticsearch index
   3. Items will no longer appear on dp.la

   WARNING: There is no persistent exclusion list. If the hub re-harvests
   and these items are still in their source feed, they will reappear after
   the next ingest + index rebuild. Coordinate with the hub to remove from
   their source.

   Proceed? [y/N]
   ```

8. **Wait for explicit user confirmation.** Do not proceed without it.

### Phase 2: Delete from S3 JSONL

This prevents the items from reappearing on the next full ES index rebuild.

For each hub in the manifest:

1. **Determine AWS profile:**
   ```bash
   aws sts get-caller-identity --profile dpla 2>/dev/null && PROFILE="dpla" || PROFILE="default"
   echo "Using profile: $PROFILE"
   ```

2. **Write IDs to a temp file** (one per line):
   ```bash
   IDFILE=$(mktemp /tmp/dpla-takedown-XXXXXX.txt)
   printf '%s\n' <id1> <id2> ... > "$IDFILE"
   cat "$IDFILE"
   ```

3. **Run the delete-from-jsonl script** using the Python discovered in the preflight:
   ```bash
   cd /Users/dominic/Documents/GitHub/ingestion3
   $PYTHON scripts/delete/delete-from-jsonl.py \
     --hub <s3-hub-prefix> \
     --profile $PROFILE \
     -y \
     -f "$IDFILE"
   ```
   The script handles `.gz`, `.jsonl`, and `.json` (uncompressed) file extensions.

4. **Check the output.** Verify:
   - `Total records removed` matches the expected count for this hub
   - No `ERROR` lines in the output
   - If removed count is 0: the IDs may not be in the latest export (warn user but continue to ES delete — the item may still be in the live index from a prior export)

5. **If the script fails:** Do NOT proceed to ES delete. Report the error to the user. S3 versioning allows recovery if any files were partially modified.

### Phase 3: Delete from Elasticsearch

This removes items from the live search index immediately.

**Step 3a: Resolve the index name from dpla_alias**

Write the SSM payload to a local file to avoid shell escaping issues:

```bash
cat > /tmp/ssm-resolve-alias.json << 'ENDJSON'
{
  "InstanceIds": ["i-00bbdfe0a6ff6cf78"],
  "DocumentName": "AWS-RunShellScript",
  "Parameters": {
    "commands": ["IP=$(hostname -I | awk '{print $1}'); curl -s http://$IP:9200/_alias/dpla_alias | python3 -c \"import sys,json; print(list(json.load(sys.stdin).keys())[0])\""]
  }
}
ENDJSON

RESOLVE_CMD_ID=$(aws ssm send-command \
  --cli-input-json file:///tmp/ssm-resolve-alias.json \
  --query 'Command.CommandId' --output text --profile $PROFILE)

echo "SSM command: $RESOLVE_CMD_ID"
sleep 8

INDEX_NAME=$(aws ssm get-command-invocation \
  --command-id "$RESOLVE_CMD_ID" \
  --instance-id i-00bbdfe0a6ff6cf78 \
  --query 'StandardOutputContent' --output text \
  --profile $PROFILE | tr -d '[:space:]')

echo "Resolved index: $INDEX_NAME"
```

**Validate the index name:** It must match the pattern `dpla-all-YYYYMMDD-HHMMSS`. If it does not, **STOP** and report the error.

**Step 3b: Search-before-delete (MANDATORY — never skip)**

Build the query JSON locally (no escaping issues), then pass to SSM via `--cli-input-json`:

```bash
# Build the query file
python3 -c "
import json
ids = [<quoted-id-list>]
query = {'query': {'terms': {'id': ids}}}
print(json.dumps(query))
" > /tmp/dpla-takedown-query.json

# Validate JSON
python3 -m json.tool /tmp/dpla-takedown-query.json > /dev/null && echo "JSON valid"

# Build SSM payload for search
python3 -c "
import json
with open('/tmp/dpla-takedown-query.json') as f:
    q = f.read().strip()
idx = '${INDEX_NAME}'
cmd = f\"IP=\$(hostname -I | awk '{{print \$1}}'); curl -s -XPOST 'http://\$IP:9200/{idx}/_search?size=10' -H 'Content-Type: application/json' -d '{q}'\"
payload = {
    'InstanceIds': ['i-00bbdfe0a6ff6cf78'],
    'DocumentName': 'AWS-RunShellScript',
    'Parameters': {'commands': [cmd]}
}
print(json.dumps(payload))
" > /tmp/ssm-search.json

SEARCH_CMD_ID=$(aws ssm send-command \
  --cli-input-json file:///tmp/ssm-search.json \
  --query 'Command.CommandId' --output text --profile $PROFILE)

sleep 8

SEARCH_RESULT=$(aws ssm get-command-invocation \
  --command-id "$SEARCH_CMD_ID" \
  --instance-id i-00bbdfe0a6ff6cf78 \
  --query 'StandardOutputContent' --output text \
  --profile $PROFILE)

echo "$SEARCH_RESULT"
HIT_COUNT=$(echo "$SEARCH_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['hits']['total']['value'])")
echo "Expected: <N>  Actual: $HIT_COUNT"
```

**Verify the search result:**
- Extract `hits.total.value` — it **MUST exactly equal** the number of IDs being deleted
- If it does not match: **STOP.** Report the discrepancy to the user.
- **Show the verification to the user:**
  ```
  ES Pre-Delete Verification
  ==========================
  Index:    dpla-all-20260202-164030
  Query:    terms match on N IDs
  Expected: N hits
  Actual:   N hits ✓

  Proceed with deletion? [y/N]
  ```
- **Wait for explicit user confirmation.**

**Step 3c: Execute the delete**

```bash
# Build SSM payload for delete_by_query
python3 -c "
import json
with open('/tmp/dpla-takedown-query.json') as f:
    q = f.read().strip()
idx = '${INDEX_NAME}'
cmd = f\"IP=\$(hostname -I | awk '{{print \$1}}'); curl -s -XPOST 'http://\$IP:9200/{idx}/_delete_by_query?conflicts=abort' -H 'Content-Type: application/json' -d '{q}'\"
payload = {
    'InstanceIds': ['i-00bbdfe0a6ff6cf78'],
    'DocumentName': 'AWS-RunShellScript',
    'Parameters': {'commands': [cmd]}
}
print(json.dumps(payload))
" > /tmp/ssm-delete.json

DELETE_CMD_ID=$(aws ssm send-command \
  --cli-input-json file:///tmp/ssm-delete.json \
  --query 'Command.CommandId' --output text --profile $PROFILE)

sleep 10

DELETE_RESULT=$(aws ssm get-command-invocation \
  --command-id "$DELETE_CMD_ID" \
  --instance-id i-00bbdfe0a6ff6cf78 \
  --query 'StandardOutputContent' --output text \
  --profile $PROFILE)

echo "$DELETE_RESULT"
```

**Verify the delete result:**
- Extract `deleted` count — must equal expected count
- Check `failures` array is empty
- Check `version_conflicts` is 0
- If any check fails: report to user immediately

### Phase 4: Post-deletion verification (MANDATORY — never skip)

**Step 4a: Verify each ID is gone from the DPLA API**

For each deleted ID, confirm the API returns 0 results:

```bash
. ~/.claude/secrets/dpla.env
for ID in <id1> <id2>; do
  COUNT=$(curl -s "https://api.dp.la/v2/items/${ID}?api_key=$DPLA_API_KEY" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
  echo "ID $ID: count=$COUNT (expected 0)"
done
```

Note: There may be a brief API cache delay (up to a few minutes) before the deletion is reflected. If count > 0, wait 2 minutes and retry before declaring a failure.

**Step 4b: Verify via dp.la web** (recommended for single-item takedowns)

Navigate to `https://dp.la/item/<id>` and confirm it returns a 404 or "item not found" page.

**Step 4c: Count delta verification**

Confirm that only the expected number of items were removed — and no others:

```bash
. ~/.claude/secrets/dpla.env
TOTAL_AFTER=$(curl -s "https://api.dp.la/v2/items?api_key=$DPLA_API_KEY&page_size=1" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
HUB_AFTER=$(curl -s "https://api.dp.la/v2/items?api_key=$DPLA_API_KEY&page_size=1&provider.name=<hub-name>" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")

echo "Total before: $TOTAL_BEFORE  |  Total after: $TOTAL_AFTER  |  Delta: $((TOTAL_BEFORE - TOTAL_AFTER))"
echo "Hub before:   $HUB_BEFORE    |  Hub after:   $HUB_AFTER    |  Delta: $((HUB_BEFORE - HUB_AFTER))"
echo "Expected delta: <N>"
```

**Expected outcomes:**
- `TOTAL_BEFORE - TOTAL_AFTER` = exactly N (the number of IDs taken down)
- `HUB_BEFORE - HUB_AFTER` = exactly N
- If total delta > N: other records were deleted — investigate immediately
- If total delta < N: some records were not found in ES (may have already been absent)
- If hub delta ≠ total delta: records from multiple hubs were affected — investigate

**Step 4d: Report final status to the user:**

```
Takedown Complete
=================
Items deleted: N
S3 JSONL: N records removed from s3://dpla-master-dataset/<hub>/jsonl/<export>/
Elasticsearch: N records deleted from index <index-name>
API verification: N items confirmed removed (count: 0)
dp.la page: 404 confirmed ✓

Count delta:
  Total DPLA: <before> → <after>  (Δ = -N) ✓
  Hub <name>: <before> → <after>  (Δ = -N) ✓

REMINDER: These items will reappear if the hub re-harvests and they are
still in the upstream source. Coordinate with the hub to remove from source.
```

### Phase 5: Cleanup

```bash
rm -f /tmp/dpla-takedown-*.json /tmp/dpla-takedown-*.txt /tmp/ssm-*.json
```

## Critical Rules

- **NEVER skip the search-before-delete step.** This is the primary safeguard against accidentally deleting wrong records.
- **NEVER use `match_all` or any broad query.** Only `terms` queries on explicit IDs are permitted.
- **NEVER proceed without user confirmation** at both the manifest stage (Phase 1) and the ES pre-delete verification stage (Phase 3b).
- **Write all SSM payloads to JSON files** and pass via `--cli-input-json file://...`. Never try to pass complex commands via `--parameters` shell quoting — it breaks.
- **ES nodes are bound to their private NIC IP, not localhost.** All ES curl commands in SSM must use `IP=$(hostname -I | awk '{print $1}')` to resolve the address dynamically.
- **The index name must match `dpla-all-*` pattern.** If alias resolution returns anything else, stop.
- **If hit count does not exactly match ID count, stop.** Do not proceed with partial matches — investigate first.
- **Do not batch more than 100 IDs** in a single ES `_delete_by_query` terms list. For larger lists, batch in groups of 100 with verification between each batch.
- **Always delete from S3 BEFORE deleting from ES.** If S3 delete fails, do not proceed to ES delete.
- **Count delta must match exactly.** If more records disappeared than expected, treat this as a critical incident.
- **AWS profile:** Check for `dpla` profile first; fall back to `default`.

## Recovery Procedures

| Failure | Recovery |
|---------|----------|
| S3 JSONL file corrupted | Restore previous version via S3 versioning: `aws s3api list-object-versions --bucket dpla-master-dataset --prefix <key>` then `aws s3api get-object --bucket dpla-master-dataset --key <key> --version-id <version-id> <local-file>` |
| Wrong records deleted from ES | Full re-index from S3 JSONL via sparkindexer on EMR (multi-hour operation). The S3 JSONL is the authoritative dataset. |
| S3 delete succeeded but ES delete failed | Re-run only the ES delete (Phase 3). The S3 state is already correct. |
| ES delete succeeded but S3 delete failed | Re-run only the S3 delete (Phase 2). Items are gone from live site but will reappear on next index rebuild if S3 is not fixed. |
| Script fails mid-file during S3 processing | Re-run the S3 delete script — it will re-process all files. Already-removed IDs simply won't be found again (idempotent). |
| Count delta > expected | Investigate immediately: check ES `_search` for unexpectedly missing IDs; if needed, trigger full re-index from S3 |

## Limitations

- **No persistent exclusion list.** Deleted items will reappear if re-harvested. The only prevention is removing the item from the hub's upstream source.
- **ES delete is not reversible** without a full re-index from S3 (hours).
- **ES nodes are in a private VPC.** All ES operations go through SSM Run Command, which adds ~10-15s per command. Use `sleep 8` after send-command before checking the result; use `sleep 10` for delete operations.
- **Large hubs (e.g. NARA: 476 part files, 15GB)** take 30-60 minutes for the S3 step.
- **API cache:** The DPLA API may take a few minutes to reflect ES deletions. Retry count checks after 2 minutes if needed.
- **S3 hub prefix vs API slug may differ.** Always verify the S3 prefix with `aws s3 ls s3://dpla-master-dataset/ --profile dpla | grep -i <slug>`.

## Key References

| Resource | Path |
|----------|------|
| Delete from ES script | [scripts/delete/delete-by-id.sh](../../../scripts/delete/delete-by-id.sh) |
| Delete from S3 JSONL (Python) | [scripts/delete/delete-from-jsonl.py](../../../scripts/delete/delete-from-jsonl.py) |
| S3 master dataset | `s3://dpla-master-dataset/<hub>/jsonl/` |
| ES nodes (SSM-managed) | `i-00bbdfe0a6ff6cf78` (search-prod1), all nodes in SSM |
| DPLA API | `https://api.dp.la/v2/items/<id>?api_key=$DPLA_API_KEY` |
| ES alias | `dpla_alias` → `dpla-all-YYYYMMDD-HHMMSS` |
| Agent guide | [AGENTS.md](../../../AGENTS.md) |
