#!/usr/bin/env bash
#
# test-nara-gate.sh - Verify nara-ingest.sh's run-scoped resume and zero-delete
# gate: valid-delete parsing from the real _SUMMARY.txt format, per-month gate
# bypass, completion markers for preprocessing and the delta harvest, the merge
# reuse guard, and launch_nara.py's parsing of the gate's resume command.
#
# Stubs aws, curl, java and sbt (the sbt stub fakes HarvestEntry and
# NaraMergeUtil output), so no AWS, Slack or Spark calls are made. I3_HOME points
# at an empty temp dir so the repo's .env is never loaded.
#
# Usage:
#   ./scripts/tests/test-nara-gate.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
NARA_SCRIPT="$REPO_ROOT/scripts/harvest/nara-ingest.sh"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

FAKE_BIN="$TMPDIR/fake-bin"
mkdir -p "$FAKE_BIN" "$TMPDIR/jdk/bin" "$TMPDIR/i3home"
CALLS="$TMPDIR/calls.log"

PASS=0
FAIL=0
pass() { echo "[PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL + 1)); }

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------
printf '#!/usr/bin/env bash\necho "aws $*" >> "${CALLS:?}"\n' > "$FAKE_BIN/aws"
printf '#!/usr/bin/env bash\necho "curl BLOCKED" >> "${CALLS:?}"\nexit 1\n' > "$FAKE_BIN/curl"
printf '#!/usr/bin/env bash\necho "openjdk 17"\n' > "$TMPDIR/jdk/bin/java"

# HarvestEntry: writes a delta harvest (with _MANIFEST unless NO_MANIFEST=1).
# NaraMergeUtil: writes the output and a _SUMMARY.txt in NaraMergeUtil's format,
# with VALID valid deletes (default 0).
cat > "$FAKE_BIN/sbt" << 'EOF'
#!/usr/bin/env bash
cmd="$*"
case "$cmd" in
  *HarvestEntry*)
    echo "HARVEST" >> "${CALLS:?}"
    out=$(echo "$cmd" | sed -E 's/.*--output=([^ ]+).*/\1/')
    d="$out/nara/harvest/20261021_000000-nara-OriginalRecord.avro"
    mkdir -p "$d"; touch "$d/part-0.avro"
    [ "${NO_MANIFEST:-0}" = 1 ] || touch "$d/_MANIFEST"
    ;;
  *NaraMergeUtil*)
    set -- $(echo "$cmd" | sed 's/.*NaraMergeUtil//')
    echo "MERGE $(basename "$4")" >> "${CALLS:?}"
    mkdir -p "$4/_LOGS"; touch "$4/_SUCCESS"
    printf ' Base\n ----------\n path: %s\n record count 100\n\n Delta\n -----------\n path: %s\n record count: 10\n\n Delete\n ------------\n path: %s\n ids to delete specified at path: 50,702\n invalid deletes (IDs not in merged dataset): 45,351\n valid deletes (IDs in merged dataset): %s\n actual removed (merged total - total after deletes): %s\n' \
      "$1" "$2" "$3" "${VALID:-0}" "${VALID:-0}" > "$4/_LOGS/_SUMMARY.txt"
    ;;
esac
exit 0
EOF
chmod +x "$FAKE_BIN"/* "$TMPDIR/jdk/bin/java"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DATA="$TMPDIR/data"
BASE="$DATA/nara/harvest/20250429_000000-nara-OriginalRecord.avro"

# Fresh data dir with a base harvest and one staged delivery per month given.
# Each delivery is one ZIP with a data XML and a NAC_DESC_Deletes file.
reset_data() {
    rm -rf "$DATA"
    mkdir -p "$BASE"; touch "$BASE/_SUCCESS"
    for m in "$@"; do
        local src="$DATA/nara/originalRecords/$m" w="$TMPDIR/zipwork"
        mkdir -p "$src"; rm -rf "$w"; mkdir -p "$w"
        echo "<record/>" > "$w/rec.xml"
        echo "<das_deletes><delete><naId>36792</naId></delete></das_deletes>" > "$w/NAC_DESC_Deletes_0001.xml"
        python3 - "$src/2025-10-06_11.038_DESC_0001.zip" "$w" << 'PY'
import os, sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w") as z:
    for f in os.listdir(sys.argv[2]):
        z.write(os.path.join(sys.argv[2], f), f)
PY
    done
    printf 'nara.harvest.delta.update = "/tmp/fake/"\nnara.harvest.endpoint = "/tmp/fake/"\n' > "$TMPDIR/i3.conf"
    : > "$CALLS"
}

# Run nara-ingest.sh non-interactively; output in $OUT, exit status in $RC
OUT="$TMPDIR/out.log"
run_nara() {
    RC=0
    env -u SLACK_BOT_TOKEN -u SLACK_TOKEN -u AWS_PROFILE \
        PATH="$FAKE_BIN:$PATH" CALLS="$CALLS" JAVA_HOME="$TMPDIR/jdk" \
        DPLA_DATA="$DATA" I3_CONF="$TMPDIR/i3.conf" I3_HOME="$TMPDIR/i3home" \
        bash "$NARA_SCRIPT" "$@" < /dev/null > "$OUT" 2>&1 || RC=$?
}

expect_rc()       { if [ "$RC" -eq "$1" ]; then pass "$2 (exit $RC)"; else fail "$2 (expected exit $1, got $RC)"; tail -20 "$OUT" | sed 's/^/    /'; fi; }
expect_out()      { if grep -qF -- "$1" "$OUT"; then pass "$2"; else fail "$2 — missing: $1"; tail -20 "$OUT" | sed 's/^/    /'; fi; }
expect_not_out()  { if ! grep -qF -- "$1" "$OUT"; then pass "$2"; else fail "$2 — unexpected: $1"; fi; }
expect_file()     { if [ -e "$1" ]; then pass "$2"; else fail "$2 — not found: $1"; fi; }
calls_count()     { grep -c "^$1" "$CALLS" || true; }

RUN=20261021
DELTA="$DATA/nara/delta/202510/$RUN"

# ---------------------------------------------------------------------------
# 1. Valid deletes parsed from the real summary format: gate passes
# ---------------------------------------------------------------------------
echo "--- 1. gate passes when the summary reports valid deletes"
reset_data 202510
VALID=5,351 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$BASE"
expect_rc 0 "run with 5,351 valid deletes completes"
expect_out "Delete gate: 5351 record(s) deleted — OK" "gate reads 'valid deletes (IDs in merged dataset): 5,351'"
expect_file "$DELTA/.preprocess_complete" "preprocessing writes its completion marker in delta/<month>/<run id>/"
expect_file "$DELTA/harvest/nara/harvest/20261021_000000-nara-OriginalRecord.avro/_MANIFEST" "delta harvest lands in delta/<month>/<run id>/harvest/"
expect_file "$DATA/nara/harvest/202510_000000-nara-OriginalRecord.avro/_LOGS/_SUMMARY.txt" "--skip-pipeline merge is named after the month"

# ---------------------------------------------------------------------------
# 2. Rerun with the same inputs reuses every step; a different base is refused
# ---------------------------------------------------------------------------
echo "--- 2. resume reuses finished steps; merge reuse checks the base"
: > "$CALLS"
VALID=5,351 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$BASE"
expect_rc 0 "same-run rerun completes"
expect_out "Month 202510 already preprocessed" "preprocessing skipped (marker present)"
expect_out "Delta harvest already exists" "complete delta harvest reused"
expect_out "Merge output already exists" "merge with the same base and delta reused"
[ "$(calls_count HARVEST)$(calls_count MERGE)" = "00" ] && pass "no harvest or merge rerun" || fail "harvest/merge rerun on resume"

# Without --base the base is the newest local harvest, which is now this merge
: > "$CALLS"
VALID=5,351 run_nara --month=202510 --run-id=$RUN --skip-pipeline
expect_rc 0 "rerun without --base completes"
expect_out "Merge output already exists" "merge reused without --base (its own output isn't taken as a new base)"

OTHER_BASE="$DATA/nara/harvest/20240101_000000-nara-OriginalRecord.avro"
mkdir -p "$OTHER_BASE"
VALID=5,351 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$OTHER_BASE"
expect_rc 1 "rerun with a different --base is refused"
expect_out "merged from different inputs" "reuse guard names the input mismatch"

# ---------------------------------------------------------------------------
# 3. 0 valid deletes: gate halts and its resume command approves only that month
# ---------------------------------------------------------------------------
echo "--- 3. gate halts on 0 valid deletes; resume approves only that month"
reset_data 202510
VALID=0 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$BASE"
expect_rc 1 "run with 0 valid deletes halts"
expect_out "halting at zero-delete gate" "gate halts non-interactively"
expect_out "--skip-delete-check-month=202510" "resume command approves 202510 only"
if ! grep -qE -- "--skip-delete-check( |$)" "$OUT"; then pass "resume command doesn't use the all-months --skip-delete-check"
else fail "resume command uses the all-months --skip-delete-check"; fi

# ---------------------------------------------------------------------------
# 4. Multi-month: the approved month is bypassed, the next is still gated
# ---------------------------------------------------------------------------
echo "--- 4. per-month bypass leaves later months gated"
reset_data 202510 202511
VALID=0 run_nara --month=202510,202511 --run-id=$RUN --skip-pipeline --base="$BASE" --skip-delete-check-month=202510
expect_rc 1 "202511 still halts at the gate"
expect_out "Zero-delete gate bypassed for 202510" "202510 bypassed"
expect_out "Zero-delete gate triggered for 202511" "202511 gated"
expect_out "--skip-delete-check-month=202510,202511" "next resume command keeps 202510 and adds 202511"

# ---------------------------------------------------------------------------
# 5. Partial outputs from a stopped run are not reused
# ---------------------------------------------------------------------------
echo "--- 5. completion markers: partial preprocessing and harvest are redone"
reset_data 202510
mkdir -p "$DELTA"; echo partial > "$DELTA/11.038_nara_delta.tar.gz"   # tar.gz, no marker
NO_MANIFEST=1 VALID=1 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$BASE"
expect_rc 1 "harvest without _MANIFEST is treated as failed"
expect_out "Preprocessing complete" "tar.gz without marker is re-preprocessed"
expect_out "Delta harvest failed" "incomplete harvest is not used"
: > "$CALLS"
VALID=1 run_nara --month=202510 --run-id=$RUN --skip-pipeline --base="$BASE"
expect_rc 0 "rerun completes"
[ "$(calls_count HARVEST)" = "1" ] && pass "incomplete harvest is redone, not reused" || fail "incomplete harvest reused"

# ---------------------------------------------------------------------------
# 6. A non-date run id needs --datestamp
# ---------------------------------------------------------------------------
echo "--- 6. non-date run id requires --datestamp"
reset_data 202510
run_nara --month=202510 --run-id=backfill --skip-pipeline --base="$BASE"
expect_rc 1 "--run-id=backfill without --datestamp is refused"
expect_out "is not a YYYYMMDD date" "error explains --datestamp is needed"

# ---------------------------------------------------------------------------
# 7. launch_nara.py recovers the month run from the logged resume command
# ---------------------------------------------------------------------------
echo "--- 7. launch_nara.py parses the gate's resume command"
PY="$REPO_ROOT/venv/bin/python"; [ -x "$PY" ] || PY=python3
if "$PY" - "$REPO_ROOT/ingest_python_scripts/nara" << 'PY'
import sys
sys.path.insert(0, sys.argv[1])
from launch_nara import parse_gate_resume_cmd as p
line = "\x1b[0;31m[ERROR]\x1b[0m   ./nara-ingest.sh --month=202510 --run-id=20261021 --base=/d/202507_000000-nara-OriginalRecord.avro --skip-pipeline --skip-delete-check-month=202510\x1b[0m\n"
r = p(line)
assert r == {"month": "202510", "run_id": "20261021",
             "extra": ["--run-id=20261021", "--base=/d/202507_000000-nara-OriginalRecord.avro", "--skip-pipeline"],
             "skip": ["--skip-delete-check-month=202510"]}, r
assert p("[ERROR]   ./nara-ingest.sh --skip-to-pipeline --harvest=/h.avro --skip-delete-check") == {}
assert p("") == {}
PY
then pass "month, run id, flags and per-month skip recovered; skip-to-pipeline halts ignored"
else fail "parse_gate_resume_cmd"; fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
