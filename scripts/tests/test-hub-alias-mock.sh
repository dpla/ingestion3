#!/usr/bin/env bash
#
# test-hub-alias-mock.sh - Deterministic hub-alias smoke tests without AWS writes
#
# This test stubs the aws CLI to validate hub->S3 prefix behavior for:
#   - scripts/s3-sync.sh
#   - scripts/check-jsonl-sync.sh
#
# It never talks to real AWS and never writes to production buckets.
#
# Usage:
#   ./scripts/tests/test-hub-alias-mock.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

TMPDIR="$(mktemp -d)"
cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

FAKE_BIN="$TMPDIR/fake-bin"
mkdir -p "$FAKE_BIN"
AWS_CALLS_LOG="$TMPDIR/aws-calls.log"
: > "$AWS_CALLS_LOG"

# Stub aws so tests are deterministic and side-effect free.
cat > "$FAKE_BIN/aws" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "${AWS_CALLS_LOG:?}"
if [ "${1:-}" = "s3" ] && [ "${2:-}" = "ls" ]; then
  exit 0
fi
if [ "${1:-}" = "s3" ] && [ "${2:-}" = "sync" ]; then
  exit 0
fi
exit 0
EOF
chmod +x "$FAKE_BIN/aws"

DATA_DIR="$TMPDIR/data"
mkdir -p \
  "$DATA_DIR/hathi/jsonl/20260201_000000-hathi-MAP3_1.IndexRecord.jsonl" \
  "$DATA_DIR/tn/jsonl/20260201_000000-tn-MAP3_1.IndexRecord.jsonl" \
  "$DATA_DIR/tennessee/jsonl/20260201_000000-tennessee-MAP3_1.IndexRecord.jsonl"

PYTHON_BIN="$REPO_ROOT/venv/bin/python"

run_with_stubbed_aws() {
    PATH="$FAKE_BIN:$PATH" \
    AWS_CALLS_LOG="$AWS_CALLS_LOG" \
    DPLA_DATA="$DATA_DIR" \
    I3_HOME="$REPO_ROOT" \
    "$@"
}

assert_log_contains() {
    local needle="$1"
    if ! grep -F "$needle" "$AWS_CALLS_LOG" >/dev/null 2>&1; then
        echo "Expected AWS call not found: $needle"
        echo "Recorded calls:"
        sed 's/^/  /' "$AWS_CALLS_LOG"
        exit 1
    fi
}

echo "Running alias smoke tests with stubbed AWS..."

# 1) s3-sync legacy alias behavior
run_with_stubbed_aws "$REPO_ROOT/scripts/s3-sync.sh" hathi
run_with_stubbed_aws "$REPO_ROOT/scripts/s3-sync.sh" tn
assert_log_contains "s3 sync $DATA_DIR/hathi/ s3://dpla-master-dataset/hathitrust/ --profile dpla"
assert_log_contains "s3 sync $DATA_DIR/tn/ s3://dpla-master-dataset/tennessee/ --profile dpla"

# 2) strict mode disables aliasing
PATH="$FAKE_BIN:$PATH" \
AWS_CALLS_LOG="$AWS_CALLS_LOG" \
DPLA_DATA="$DATA_DIR" \
I3_HOME="$REPO_ROOT" \
I3_STRICT_HUB_NAMES=1 \
"$REPO_ROOT/scripts/s3-sync.sh" hathi
assert_log_contains "s3 sync $DATA_DIR/hathi/ s3://dpla-master-dataset/hathi/ --profile dpla"

# 3) check-jsonl-sync uses alias for tn
run_with_stubbed_aws "$REPO_ROOT/scripts/status/check-jsonl-sync.sh" --data-dir "$DATA_DIR" --profile dpla || true
assert_log_contains "s3 ls s3://dpla-master-dataset/tennessee/jsonl/20260201_000000-tn-MAP3_1.IndexRecord.jsonl/ --profile dpla"

# 4) check-jsonl-sync keeps canonical tennessee unchanged
assert_log_contains "s3 ls s3://dpla-master-dataset/tennessee/jsonl/20260201_000000-tennessee-MAP3_1.IndexRecord.jsonl/ --profile dpla"

# 5) orchestrator dry-run succeeds against canonical-key i3.conf with both
# legacy and canonical hub arguments (no AWS writes in dry-run path)
if [ -x "$PYTHON_BIN" ]; then
  I3_CANONICAL_CONF="$TMPDIR/i3-canonical.conf"
  cat > "$I3_CANONICAL_CONF" <<'EOF'
tennessee.provider = "Digital Library of Tennessee"
tennessee.harvest.type = "localoai"
tennessee.schedule.frequency = "monthly"
tennessee.schedule.months = [1]
tennessee.schedule.status = "active"
tennessee.s3_destination = "s3://dpla-master-dataset/tennessee/"

hathitrust.provider = "HathiTrust"
hathitrust.harvest.type = "localoai"
hathitrust.schedule.frequency = "monthly"
hathitrust.schedule.months = [1]
hathitrust.schedule.status = "active"
hathitrust.s3_destination = "s3://dpla-master-dataset/hathitrust/"
EOF

  PATH="$FAKE_BIN:$PATH" AWS_CALLS_LOG="$AWS_CALLS_LOG" \
    "$PYTHON_BIN" -m scheduler.orchestrator.main --dry-run \
    --config "$I3_CANONICAL_CONF" --hub=tn,hathi >/dev/null

  PATH="$FAKE_BIN:$PATH" AWS_CALLS_LOG="$AWS_CALLS_LOG" \
    "$PYTHON_BIN" -m scheduler.orchestrator.main --dry-run \
    --config "$I3_CANONICAL_CONF" --hub=tennessee,hathitrust >/dev/null
fi

echo "PASS: mock alias tests completed without real AWS writes."
