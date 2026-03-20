#!/usr/bin/env bash
#
# test-nara-force-sync.sh - Verify nara-ingest.sh --force-sync targets only the
# latest S3 base harvest rather than syncing the entire historical prefix.
#
# Stubs the aws CLI so no real AWS calls are made.
#
# Usage:
#   ./scripts/tests/test-nara-force-sync.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
NARA_SCRIPT="$REPO_ROOT/scripts/harvest/nara-ingest.sh"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

FAKE_BIN="$TMPDIR/fake-bin"
mkdir -p "$FAKE_BIN"
AWS_CALLS_LOG="$TMPDIR/aws-calls.log"
: > "$AWS_CALLS_LOG"

PASS=0
FAIL=0

pass() { echo "[PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL + 1)); }

assert_log_contains() {
    local needle="$1"
    if grep -qF "$needle" "$AWS_CALLS_LOG" 2>/dev/null; then
        pass "AWS call made: $needle"
    else
        fail "Expected AWS call not found: $needle"
        echo "  Recorded calls:"
        sed 's/^/    /' "$AWS_CALLS_LOG"
    fi
}

assert_log_not_contains() {
    local needle="$1"
    if ! grep -qF "$needle" "$AWS_CALLS_LOG" 2>/dev/null; then
        pass "AWS call NOT made (correct): $needle"
    else
        fail "Unexpected AWS call found: $needle"
    fi
}

# ---------------------------------------------------------------------------
# Stub aws: s3 ls returns three fake historical harvests + the latest.
# s3 sync succeeds silently. All other calls are no-ops.
# ---------------------------------------------------------------------------
cat > "$FAKE_BIN/aws" << 'EOF'
#!/usr/bin/env bash
echo "$*" >> "${AWS_CALLS_LOG:?}"

if [ "${1:-}" = "s3" ] && [ "${2:-}" = "ls" ]; then
    # Return fake harvest prefix listing
    echo "                           PRE 20221025_000000-nara-OriginalRecord.avro/"
    echo "                           PRE 20230430_000000-nara-OriginalRecord.avro/"
    echo "                           PRE 20231031_000000-nara-OriginalRecord.avro/"
    echo "                           PRE 20260210_103600-nara-OriginalRecord.avro/"
    exit 0
fi

if [ "${1:-}" = "s3" ] && [ "${2:-}" = "sync" ]; then
    exit 0
fi

exit 0
EOF
chmod +x "$FAKE_BIN/aws"

# Minimal fake i3.conf to satisfy prerequisite checks
FAKE_CONF="$TMPDIR/i3.conf"
cat > "$FAKE_CONF" << 'EOF'
nara.harvest.delta.update = "/tmp/fake/"
nara.harvest.endpoint = "/tmp/fake/"
EOF

# Create a fake delta directory so --skip-preprocess doesn't abort
FAKE_DELTA="$TMPDIR/data/nara/delta/202602"
mkdir -p "$FAKE_DELTA/deletes"
touch "$FAKE_DELTA/fake_group_nara_delta.tar.gz"

# Minimal fake sbt so the script doesn't fail prerequisite check
cat > "$FAKE_BIN/sbt" << 'EOF'
#!/usr/bin/env bash
echo "sbt $*" >> "${AWS_CALLS_LOG:?}"
exit 0
EOF
chmod +x "$FAKE_BIN/sbt"

# ---------------------------------------------------------------------------
# Run: --force-sync --skip-preprocess --skip-pipeline stops after the S3
# download, which is exactly the behavior we want to test.
# ---------------------------------------------------------------------------
PATH="$FAKE_BIN:$PATH" \
AWS_CALLS_LOG="$AWS_CALLS_LOG" \
DPLA_DATA="$TMPDIR/data" \
I3_CONF="$FAKE_CONF" \
I3_HOME="$REPO_ROOT" \
  "$NARA_SCRIPT" \
    --month=202602 \
    --force-sync \
    --skip-preprocess \
    --skip-pipeline 2>/dev/null || true

# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

# Should list the harvest prefix to find the latest key
assert_log_contains "s3 ls s3://dpla-master-dataset/nara/harvest/"

# Should sync ONLY the latest key
assert_log_contains "s3 sync s3://dpla-master-dataset/nara/harvest/20260210_103600-nara-OriginalRecord.avro/"

# Should NOT sync the entire prefix (the old behavior)
assert_log_not_contains "s3 sync s3://dpla-master-dataset/nara/harvest/ "

# Should NOT sync any older historical harvest
assert_log_not_contains "20221025_000000-nara-OriginalRecord.avro"
assert_log_not_contains "20230430_000000-nara-OriginalRecord.avro"
assert_log_not_contains "20231031_000000-nara-OriginalRecord.avro"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
