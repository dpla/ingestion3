#!/usr/bin/env bash
#
# test-prune-hub-data.sh - Verify prune-hub-data.sh and the run-dir S3 helpers
# in common.sh (list_s3_complete_runs, sync_run_to_s3).
#
# Stubs aws with a fake that maps s3://bucket/key to a local directory, so no
# AWS calls are made. I3_HOME points at an empty temp dir so the repo's .env is
# never loaded.
#
# Usage:
#   ./scripts/tests/test-prune-hub-data.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
PRUNE="$REPO_ROOT/scripts/prune-hub-data.sh"
COMMON="$REPO_ROOT/scripts/common.sh"

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

FAKE_BIN="$TMP/fake-bin"
mkdir -p "$FAKE_BIN" "$TMP/i3home"

PASS=0
FAIL=0
pass() { echo "[PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL + 1)); }

# ---------------------------------------------------------------------------
# Fake aws: s3 ls --recursive, s3 sync (honours --exclude _SUCCESS), s3 cp.
# FAKE_S3 is the "bucket root"; AWS_FAIL_LS / AWS_FAIL_SYNC force errors.
# ---------------------------------------------------------------------------
cat > "$FAKE_BIN/aws" << 'EOF'
#!/usr/bin/env bash
loc() { echo "${FAKE_S3:?}/${1#s3://}"; }
[ "$1" = s3 ] || exit 2
case "$2" in
  ls)
    [ "${AWS_FAIL_LS:-0}" = 1 ] && { echo "An error occurred (AccessDenied)" >&2; exit 255; }
    d=$(loc "$3"); [ -d "$d" ] || exit 1
    bucket_root="${FAKE_S3}/$(echo "${3#s3://}" | cut -d/ -f1)"
    (cd "$bucket_root" && find . -type f | sed 's|^\./||' | while read -r k; do
       case "$k" in "${3#s3://*/}"*) echo "2026-11-01 00:00:00 0 $k" ;; esac; done)
    ;;
  sync)
    [ "${AWS_FAIL_SYNC:-0}" = 1 ] && { echo "upload failed" >&2; exit 1; }
    src="$3"; dst=$(loc "$4"); mkdir -p "$dst"
    (cd "$src" && find . -type f ! -path ./_SUCCESS | while read -r f; do
       mkdir -p "$dst/$(dirname "$f")"; cp "$f" "$dst/$f"; done)
    ;;
  cp)
    dst=$(loc "$4"); mkdir -p "$(dirname "$dst")"; cp "$3" "$dst" ;;
  rm)
    [ "${AWS_FAIL_RM:-0}" = 1 ] && { echo "delete failed" >&2; exit 1; }
    rm -f "$(loc "$3")" ;;
  *) exit 2 ;;
esac
EOF
chmod +x "$FAKE_BIN/aws"

ENVV=(env -i HOME="$HOME" PATH="$FAKE_BIN:$PATH" I3_HOME="$TMP/i3home"
      I3_CONF="$TMP/i3home/i3.conf" DPLA_DATA="$TMP/data" FAKE_S3="$TMP/s3" TMPDIR="$TMP")

S3H="$TMP/s3/dpla-master-dataset/ohio"
D="$TMP/data/ohio"

run() { local n="$1"; mkdir -p "$D/$2/$n"; touch "$D/$2/$n/part-0"; [ "${3:-}" = incomplete ] || touch "$D/$2/$n/_SUCCESS"; }
in_s3() { mkdir -p "$S3H/$2/$1"; touch "$S3H/$2/$1/part-0" "$S3H/$2/$1/_SUCCESS"; }

reset() {
  rm -rf "$TMP/data" "$TMP/s3"
  # harvest: 4 complete runs (oldest in S3, second not) + 1 incomplete
  run 20260101_000000-ohio-OriginalRecord.avro harvest
  run 20260201_000000-ohio-OriginalRecord.avro harvest
  run 20260301_000000-ohio-OriginalRecord.avro harvest
  run 20260401_000000-ohio-OriginalRecord.avro harvest
  run 20260215_000000-ohio-OriginalRecord.avro harvest incomplete
  in_s3 20260101_000000-ohio-OriginalRecord.avro harvest
  # jsonl: 4 complete; oldest in S3, second held back (not in S3)
  run 20260101_000000-ohio-MAP3_1.IndexRecord.jsonl jsonl
  run 20260201_000000-ohio-MAP3_1.IndexRecord.jsonl jsonl
  run 20260301_000000-ohio-MAP3_1.IndexRecord.jsonl jsonl
  run 20260401_000000-ohio-MAP3_1.IndexRecord.jsonl jsonl
  in_s3 20260101_000000-ohio-MAP3_1.IndexRecord.jsonl jsonl
  # mapping: only incomplete runs
  run 20260401_000000-ohio-MAP4_0.MAPRecord.avro mapping incomplete
}

# ---------------------------------------------------------------------------
reset
out=$("${ENVV[@]}" bash "$PRUNE" ohio --dry-run 2>&1)
if [ -d "$D/harvest/20260101_000000-ohio-OriginalRecord.avro" ] \
   && [ -d "$D/harvest/20260215_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -d "$S3H/harvest/20260201_000000-ohio-OriginalRecord.avro" ] \
   && echo "$out" | grep -q "WOULD DELETE: .*harvest/20260101_000000-ohio-OriginalRecord.avro (in S3)" \
   && echo "$out" | grep -q "WOULD DELETE: .*harvest/20260215_000000-ohio-OriginalRecord.avro (incomplete run)" \
   && echo "$out" | grep -q "WOULD SYNC + DELETE: .*harvest/20260201_000000-ohio-OriginalRecord.avro" \
   && [ ! -e "$TMP/i3home/logs/data-history/ohio.log" ]
then pass "dry run lists deletes and syncs, changes nothing, records nothing"
else fail "dry run"; echo "$out"; fi

# ---------------------------------------------------------------------------
reset
out=$("${ENVV[@]}" bash "$PRUNE" ohio 2>&1) || true
ok=true
for r in 20260301 20260401; do [ -d "$D/harvest/${r}_000000-ohio-OriginalRecord.avro" ] || ok=false; done
for r in 20260101 20260201 20260215; do [ ! -e "$D/harvest/${r}_000000-ohio-OriginalRecord.avro" ] || ok=false; done
[ -f "$S3H/harvest/20260201_000000-ohio-OriginalRecord.avro/_SUCCESS" ] || ok=false
[ -f "$S3H/harvest/20260201_000000-ohio-OriginalRecord.avro/part-0" ] || ok=false
if $ok; then pass "harvest: keeps newest 2, deletes in-S3 and incomplete, syncs then deletes the rest"
else fail "harvest prune"; echo "$out"; fi

if [ ! -e "$D/jsonl/20260101_000000-ohio-MAP3_1.IndexRecord.jsonl" ] \
   && [ -d "$D/jsonl/20260201_000000-ohio-MAP3_1.IndexRecord.jsonl" ] \
   && [ ! -e "$S3H/jsonl/20260201_000000-ohio-MAP3_1.IndexRecord.jsonl" ] \
   && echo "$out" | grep -q "KEEP (not in S3; JSONL is never uploaded by prune): .*20260201"
then pass "jsonl: deletes in-S3 snapshots, never uploads or deletes held-back ones"
else fail "jsonl prune"; echo "$out"; fi

if [ -d "$D/mapping/20260401_000000-ohio-MAP4_0.MAPRecord.avro" ] \
   && echo "$out" | grep -q "KEEP ALL (no complete run found): .*mapping"
then pass "a step with no complete run is left alone"
else fail "no-complete-run step"; echo "$out"; fi

HIST="$TMP/i3home/logs/data-history/ohio.log"
if grep -F "SYNCED" "$HIST" | grep -qF "harvest/20260201_000000-ohio-OriginalRecord.avro -> s3://dpla-master-dataset/ohio/harvest/20260201_000000-ohio-OriginalRecord.avro/" \
   && grep -F "DELETED" "$HIST" | grep -qF "harvest/20260201_000000-ohio-OriginalRecord.avro (synced to S3)" \
   && grep -F "DELETED" "$HIST" | grep -qF "harvest/20260215_000000-ohio-OriginalRecord.avro (incomplete run)" \
   && grep -F "DELETED" "$HIST" | grep -qF "jsonl/20260101_000000-ohio-MAP3_1.IndexRecord.jsonl (in S3)" \
   && ! grep -qF "20260401" "$HIST"
then pass "data history records each sync and delete, nothing for kept runs"
else fail "data history"; cat "$HIST" 2>/dev/null; fi

# ---------------------------------------------------------------------------
reset
rc=0; out=$("${ENVV[@]}" AWS_FAIL_SYNC=1 bash "$PRUNE" ohio 2>&1) || rc=$?
if [ "$rc" -eq 1 ] && [ -d "$D/harvest/20260201_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -e "$S3H/harvest/20260201_000000-ohio-OriginalRecord.avro/_SUCCESS" ] \
   && echo "$out" | grep -q "KEEP (S3 sync failed)" \
   && grep -F "SYNC FAILED" "$TMP/i3home/logs/data-history/ohio.log" | grep -qF "20260201_000000-ohio-OriginalRecord.avro"
then pass "failed sync keeps the run, records SYNC FAILED, exits 1"
else fail "failed sync (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
reset
rc=0; out=$("${ENVV[@]}" AWS_FAIL_LS=1 bash "$PRUNE" ohio 2>&1) || rc=$?
if [ "$rc" -eq 1 ] && [ -d "$D/harvest/20260101_000000-ohio-OriginalRecord.avro" ] \
   && [ -d "$D/harvest/20260201_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -e "$D/harvest/20260215_000000-ohio-OriginalRecord.avro" ] \
   && echo "$out" | grep -q "KEEP (couldn't list S3)"
then pass "S3 listing error: keeps complete runs, still clears incomplete ones, exits 1"
else fail "S3 listing error (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
reset
"${ENVV[@]}" bash "$PRUNE" ohio --keep 1 >/dev/null 2>&1 || true
if [ -d "$D/harvest/20260401_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -e "$D/harvest/20260301_000000-ohio-OriginalRecord.avro" ]
then pass "--keep 1 keeps only the newest"
else fail "--keep 1"; fi

# ---------------------------------------------------------------------------
rc=0; out=$("${ENVV[@]}" bash "$PRUNE" nara --dry-run 2>&1) || rc=$?
if [ "$rc" -ne 0 ] && echo "$out" | grep -q "NARA has its own prune"
then pass "refuses nara"
else fail "nara refusal"; fi

rc=0; out=$("${ENVV[@]}" bash "$PRUNE" nosuchhub 2>&1) || rc=$?
if [ "$rc" -eq 0 ] && echo "$out" | grep -q "Nothing to prune"
then pass "missing hub dir is a no-op"
else fail "missing hub dir"; fi

# ---------------------------------------------------------------------------
# _SUCCESS in S3 but a part missing (interrupted sync): keep, exit 1
reset
touch "$D/harvest/20260101_000000-ohio-OriginalRecord.avro/part-1"
rc=0; out=$("${ENVV[@]}" bash "$PRUNE" ohio 2>&1) || rc=$?
if [ "$rc" -eq 1 ] && [ -d "$D/harvest/20260101_000000-ohio-OriginalRecord.avro" ] \
   && echo "$out" | grep -q "KEEP (has _SUCCESS in S3 but 1 file(s) missing there): .*20260101"
then pass "_SUCCESS in S3 with a missing part keeps the run and exits 1"
else fail "partial upload (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
# --no-upload (ingest.sh --skip-s3-sync): never uploads, keeps runs not in S3
reset
rc=0; out=$("${ENVV[@]}" bash "$PRUNE" ohio --no-upload 2>&1) || rc=$?
if [ "$rc" -eq 0 ] && [ -d "$D/harvest/20260201_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -e "$S3H/harvest/20260201_000000-ohio-OriginalRecord.avro" ] \
   && [ ! -e "$D/harvest/20260101_000000-ohio-OriginalRecord.avro" ] \
   && echo "$out" | grep -q "KEEP (not in S3; --no-upload): .*20260201"
then pass "--no-upload keeps runs not in S3, still deletes ones that are"
else fail "--no-upload (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
# S3 listing error with only incomplete runs to clear: still exits 1
rm -rf "$TMP/data" "$TMP/s3"
run 20260301_000000-ohio-OriginalRecord.avro harvest
run 20260215_000000-ohio-OriginalRecord.avro harvest incomplete
rc=0; out=$("${ENVV[@]}" AWS_FAIL_LS=1 bash "$PRUNE" ohio 2>&1) || rc=$?
if [ "$rc" -eq 1 ] && [ ! -e "$D/harvest/20260215_000000-ohio-OriginalRecord.avro" ]
then pass "S3 listing error exits 1 even with no complete run to judge"
else fail "listing error exit status (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
# common.sh helpers on their own
reset
if "${ENVV[@]}" bash -c "source '$COMMON' && [ \"\$(list_s3_complete_runs ohio harvest)\" = 20260101_000000-ohio-OriginalRecord.avro ] && [ -z \"\$(list_s3_complete_runs ohio enrichment)\" ]"
then pass "list_s3_complete_runs: lists runs with _SUCCESS; empty for a missing step"
else fail "list_s3_complete_runs"; fi

if "${ENVV[@]}" bash -c "source '$COMMON' && ! sync_run_to_s3 ohio mapping '$D/mapping/20260401_000000-ohio-MAP4_0.MAPRecord.avro'" 2>/dev/null \
   && [ ! -e "$S3H/mapping" ]
then pass "sync_run_to_s3: refuses an incomplete run"
else fail "sync_run_to_s3 incomplete"; fi

rm -rf "$S3H/harvest/20260401_000000-ohio-OriginalRecord.avro"
if "${ENVV[@]}" AWS_FAIL_LS=0 bash -c "source '$COMMON' && sync_run_to_s3 ohio harvest '$D/harvest/20260401_000000-ohio-OriginalRecord.avro'" \
   && [ -f "$S3H/harvest/20260401_000000-ohio-OriginalRecord.avro/_SUCCESS" ] \
   && [ -f "$S3H/harvest/20260401_000000-ohio-OriginalRecord.avro/part-0" ]
then pass "sync_run_to_s3: uploads the run with its _SUCCESS"
else fail "sync_run_to_s3 upload"; fi

# Repeat sync into a run already in S3: its old _SUCCESS comes down first, so a
# repeat upload that fails leaves the run incomplete in S3
R=20260101_000000-ohio-OriginalRecord.avro
touch "$D/harvest/$R/part-1"
if "${ENVV[@]}" AWS_FAIL_SYNC=1 bash -c "source '$COMMON' && ! sync_run_to_s3 ohio harvest '$D/harvest/$R'" 2>/dev/null \
   && [ ! -e "$S3H/harvest/$R/_SUCCESS" ] && [ ! -e "$S3H/harvest/$R/part-1" ]
then pass "sync_run_to_s3: failed repeat sync leaves the run without _SUCCESS in S3"
else fail "sync_run_to_s3 repeat sync"; fi

touch "$S3H/harvest/$R/_SUCCESS"
if "${ENVV[@]}" AWS_FAIL_RM=1 bash -c "source '$COMMON' && ! sync_run_to_s3 ohio harvest '$D/harvest/$R'" 2>/dev/null \
   && [ ! -e "$S3H/harvest/$R/part-1" ]
then pass "sync_run_to_s3: stops without uploading if the old _SUCCESS can't be removed"
else fail "sync_run_to_s3 rm failure"; fi

if "${ENVV[@]}" bash -c "source '$COMMON' && [ -z \"\$(list_s3_stage_keys ohio harvest | grep part-1)\" ] && sync_run_to_s3 ohio harvest '$D/harvest/$R'" \
   && [ -f "$S3H/harvest/$R/_SUCCESS" ] && [ -f "$S3H/harvest/$R/part-1" ]
then pass "sync_run_to_s3: repeat sync completes the run in S3"
else fail "sync_run_to_s3 repeat sync completes"; fi

# run_files_missing_in_s3: an unreadable run is an error, never "nothing missing"
rc=0; "${ENVV[@]}" bash -c "source '$COMMON' && run_files_missing_in_s3 '$D/harvest/no-such-run' 'x/_SUCCESS'" >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 2 ]
then pass "run_files_missing_in_s3: returns 2 when the run can't be read"
else fail "run_files_missing_in_s3 error status (rc=$rc)"; fi

rc=0; out=$("${ENVV[@]}" bash -c "source '$COMMON' && run_files_missing_in_s3 '$D/harvest/$R' \"\$(list_s3_stage_keys ohio harvest)\"" 2>&1) || rc=$?
if [ "$rc" -eq 0 ] && [ -z "$out" ]
then pass "run_files_missing_in_s3: status 0 and no output when every file is in S3"
else fail "run_files_missing_in_s3 all present (rc=$rc)"; echo "$out"; fi

# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
