---
name: dpla-takedown
description: Remove DPLA items from the live site and search index in response to takedown requests. Use when the user says take down, delete, remove item(s) from dp.la, or action a takedown request. Accepts DPLA IDs directly or criteria (hub, institution, collection) to generate an ID list. Includes mandatory pre-flight verification, search-before-delete on Elasticsearch, and post-deletion confirmation with count delta verification.
allowed-tools: Bash
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
export DPLA_API_KEY=$(grep '^DPLA_API_KEY=' ~/.claude/secrets/dpla.env | cut -d= -f2)
TOTAL_BEFORE=$(curl -s "https://api.dp.la/v2/items?api_key=$DPLA_API_KEY&page_size=1" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "Total DPLA items before: $TOTAL_BEFORE"
aws sts get-caller-identity --profile dpla 2>/dev/null && PROFILE="dpla" || PROFILE="default"
echo "Using AWS profile: $PROFILE"
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

The user provides ONE OF:

**A) Explicit IDs** — one or more 32-character DPLA IDs (hex strings). Proceed to Phase 1.

**B) A dp.la search URL** — e.g. `https://dp.la/search?partner=...&provider=...&collection=...`. In this case:

1. Parse the URL query parameters to extract filter values. The relevant mappings are (from the dp.la frontend source):
   - `partner` → `provider.name` (hub display name, e.g. "California Digital Library")
   - `provider` → `admin.contributingInstitution` (**not** `dataProvider`)
   - `collection` → `sourceResource.collection.title`
   - Other filters (e.g. `subject`, `format`) → corresponding API fields
2. URL-decode all parameter values before using them in API queries.
3. Build the API query and proceed as in option C below (paginate, show sample, confirm).

**C) Criteria** — e.g. "all items from [institution] in [hub]", or "collection X". In this case:

1. Query the DPLA API to generate the list. Use `provider.name` and/or `admin.contributingInstitution` and/or `sourceResource.collection.title` filters with `page_size=500` and paginate to collect all matching IDs:
   ```bash
   export DPLA_API_KEY=$(grep '^DPLA_API_KEY=' ~/.claude/secrets/dpla.env | cut -d= -f2)
   curl -s "https://api.dp.la/v2/items?provider.name=<hub+display+name>&admin.contributingInstitution=<institution>&page_size=500&page=1&api_key=$DPLA_API_KEY"
   ```
   > **IMPORTANT — `sourceResource.collection.title` uses tokenized (full-text) search, not exact matching.** A query for "Maps - Marin County" will also return records from "Maps - Marin County Plat Maps, J.C. Oglesby Collection" because the tokens appear in that longer title. Always post-filter results with a client-side exact string match on `collection.title` after paginating:
   > ```python
   > exact = [r for r in records
   >          if any(c.get('title') == TARGET_COLLECTION
   >                 for c in (r['sourceResource'].get('collection') or [])
   >                 if isinstance(c, dict))]
   > ```
   > The API count and the dp.la website count may differ by the number of partial-match false positives; the post-filtered count is authoritative.
2. **STOP and show the user:**
   - Total count of matching records (after exact-match post-filter)
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
4. **Verify the source record is gone (MANDATORY).** For each item, determine the correct URL to check and verify it returns 404. This confirms the item has been deleted at the source institution and will not be re-harvested.

   **ContentDM (common case):** ContentDM uses a React SPA that always returns HTTP 200 for the page shell regardless of whether the item or collection exists — do NOT rely on HTTP status codes from `/cdm/ref/collection/` or `/digital/collection/` URLs. Use the ContentDM REST API instead:

   ```bash
   # Extract collection alias and item ID from isShownAt URL
   # isShownAt format: http://<host>/cdm/ref/collection/<alias>/id/<n>
   # or:               http://<host>/digital/collection/<alias>/id/<n>
   HOST="<contentdm-hostname>"
   ALIAS="<collection-alias>"    # e.g. "stanleymisc"
   ITEM_ID="<n>"                 # e.g. "0"

   # 1. Check if the entire collection is unpublished (most efficient for batch takedowns)
   curl -o /dev/null -s -w "%{http_code}" "http://$HOST/digital/api/collections/$ALIAS"
   # 404 → entire collection removed → all items confirmed deleted

   # 2. Check individual item existence
   curl -o /dev/null -s -w "%{http_code}" "http://$HOST/digital/api/collections/$ALIAS/items/$ITEM_ID"
   # 404 → item removed ✓

   # NOTE: Do NOT use the legacy dmwebservices endpoint:
   #   /digital/bl/dmwebservices/index.php?q=dmGetItemInfo/<alias>/<id>/json
   # This endpoint returns item metadata even for unpublished/restricted collections
   # and is NOT a reliable indicator that the item is publicly accessible.
   ```
   - **If the collection API returns 404:** entire collection is unpublished — all items in the collection are confirmed deleted. Proceed with all of them.
   - **If the item API returns 404:** that item is confirmed deleted.
   - **If either returns 200:** the item/collection is still publicly accessible at the source. **STOP for that ID**, flag it to the user, and exclude it from the takedown manifest.
   - **If the URL is unreachable / connection error:** flag it to the user and ask whether to proceed for that ID.

   **Calisphere / CDL ARK items:** Calisphere (`calisphere.org`) is behind AWS WAF which returns `202 + x-amzn-waf-action: challenge` to all non-browser curl requests, making HTTP status checks impossible via curl. Use the browser automation tools instead:

   ```javascript
   // Run in-browser via mcp__Claude_in_Chrome__javascript_tool (avoids WAF)
   // First navigate to calisphere.org to establish session, then:
   const urls = ["https://calisphere.org/item/ark:/13030/<ark1>", ...];
   window._results = null;
   Promise.all(urls.map(url =>
     fetch(url, {method: 'GET', redirect: 'follow'})
       .then(r => ({url, status: r.status}))
       .catch(e => ({url, status: 'ERROR'}))
   )).then(r => { window._results = r; });
   // Then check: JSON.stringify(window._results)
   ```
   - Process in batches of ≤30 URLs to avoid WAF rate-limiting; use 1-2s delay between batches.
   - **404** = item confirmed deleted ✓
   - **503** = also means item not found (same "Whoops! 404: Not Found" error page renders) ✓
   - **200** = item still live — **STOP**, exclude from manifest ✗
   - isShownAt format is `http://ark.cdlib.org/ark:/13030/<ark>` → convert to `https://calisphere.org/item/ark:/13030/<ark>`

5. **Determine S3 hub prefix.** The API slug (e.g. `orbiscascade`) may differ from the S3 prefix (e.g. `orbis-cascade`). Check the actual S3 prefix:
   ```bash
   aws s3 ls s3://dpla-master-dataset/ --profile $PROFILE | grep -i <hub-slug>
   ```
6. **Group IDs by hub.** All IDs in a single S3 delete run must belong to the same hub.
7. **Capture the hub baseline count** (see Pre-Run Snapshot above) using the hub's `provider.name`.
8. **Present the full manifest to the user:**

   ```
   Takedown Manifest
   =================
   Items: N
   Hub(s): <hub list>

   ID                                Title                              DataProvider                      Source Status
   --------------------------------  -----------------------------------  --------------------------  ---------------
   57d000d66004c73c5e31ea7dc7f57201  "Letter from John Adams, 1776"      Massachusetts Historical Society  404 ✓
   ...

   Excluded (source still live — not taking down):
   <id>  "<title>"  <dataProvider>  200 ✗
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

9. **Wait for explicit user confirmation.** Do not proceed without it.

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
    "commands": ["IP=$(hostname -I | cut -d' ' -f1); curl -s http://$IP:9200/_alias/dpla_alias | python3 -c \"import sys,json; print(list(json.load(sys.stdin).keys())[0])\""]
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
print(json.dumps({'query': {'terms': {'id': ids}}}))
" > /tmp/dpla-takedown-query.json

python3 -m json.tool /tmp/dpla-takedown-query.json > /dev/null && echo "JSON valid"

# Transfer query via base64 to remote instance, then search using file reference.
# This avoids all JSON escaping issues in the SSM shell command.
python3 - "$INDEX_NAME" << 'PYEOF' > /tmp/ssm-search.json
import json, base64, sys
idx = sys.argv[1]
with open('/tmp/dpla-takedown-query.json') as f:
    query = f.read().strip()
b64 = base64.b64encode(query.encode()).decode()
cmd = (f"echo {b64} | base64 -d > /tmp/dpla-q.json && "
       f"IP=$(hostname -I | awk '{{print $1}}') && "
       f"curl -s -XPOST \"http://$IP:9200/{idx}/_search?size=10\" "
       f"-H 'Content-Type: application/json' -d @/tmp/dpla-q.json")
payload = {
    'InstanceIds': ['i-00bbdfe0a6ff6cf78'],
    'DocumentName': 'AWS-RunShellScript',
    'Parameters': {'commands': [cmd]}
}
print(json.dumps(payload))
PYEOF

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
python3 - "$INDEX_NAME" << 'PYEOF' > /tmp/ssm-delete.json
import json, base64, sys
idx = sys.argv[1]
with open('/tmp/dpla-takedown-query.json') as f:
    query = f.read().strip()
b64 = base64.b64encode(query.encode()).decode()
cmd = (f"echo {b64} | base64 -d > /tmp/dpla-q.json && "
       f"IP=$(hostname -I | awk '{{print $1}}') && "
       f"curl -s -XPOST \"http://$IP:9200/{idx}/_delete_by_query?conflicts=abort\" "
       f"-H 'Content-Type: application/json' -d @/tmp/dpla-q.json")
payload = {
    'InstanceIds': ['i-00bbdfe0a6ff6cf78'],
    'DocumentName': 'AWS-RunShellScript',
    'Parameters': {'commands': [cmd]}
}
print(json.dumps(payload))
PYEOF

DELETE_CMD_ID=$(aws ssm send-command \
  --cli-input-json file:///tmp/ssm-delete.json \
  --query 'Command.CommandId' --output text --profile $PROFILE)

sleep 12

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
export DPLA_API_KEY=$(grep '^DPLA_API_KEY=' ~/.claude/secrets/dpla.env | cut -d= -f2)
for ID in <id1> <id2>; do
  RESP=$(curl -s "https://api.dp.la/v2/items/${ID}?api_key=$DPLA_API_KEY")
  if echo "$RESP" | grep -q "could not be found"; then
    echo "ID $ID: not found ✓"
  else
    COUNT=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count','?'))" 2>/dev/null || echo "?")
    echo "ID $ID: count=$COUNT ✗ STILL PRESENT"
  fi
done
```

Note: There may be a brief API cache delay (up to a few minutes) before the deletion is reflected. If an ID still shows as present, wait 2 minutes and retry before declaring a failure.

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
- **ES nodes are bound to their private NIC IP, not localhost.** All ES curl commands in SSM must use `IP=$(hostname -I | cut -d' ' -f1)` to resolve the address dynamically.
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
- **S3 hub prefix vs API slug may differ.** Always verify the S3 prefix with `aws s3 ls s3://dpla-master-dataset/ --profile $PROFILE | grep -i <slug>`.

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
