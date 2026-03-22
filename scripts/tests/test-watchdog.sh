#!/usr/bin/env bash
# test-watchdog.sh — unit tests for ingest-watchdog.sh sentinel deduplication
#
# Tests the three key behaviours of the watchdog:
#   1. Alerts and creates sentinel on first detection of a dead ingest
#   2. Suppresses repeated alerts when sentinel is newer than status file
#   3. Re-arms (alerts again) when a new ingest updates the status file mtime
#
# Usage:
#   ./scripts/tests/test-watchdog.sh
#   ./scripts/tests/test-watchdog.sh --verbose

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(dirname "$SCRIPT_DIR")"
WATCHDOG="$SCRIPTS_DIR/ingest-watchdog.sh"

# ── colours & counters ────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
TESTS_RUN=0; TESTS_PASSED=0; TESTS_FAILED=0
VERBOSE=false
[[ "${1:-}" == "--verbose" || "${1:-}" == "-v" ]] && VERBOSE=true

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; TESTS_PASSED=$((TESTS_PASSED+1)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; TESTS_FAILED=$((TESTS_FAILED+1)); }
log_info() { $VERBOSE && echo -e "${YELLOW}[INFO]${NC} $1" || true; }

run_test() {
    local name="$1"; local cmd="$2"
    TESTS_RUN=$((TESTS_RUN+1))
    echo -e "${BLUE}[TEST]${NC} $name"
    if eval "$cmd" >/dev/null 2>&1; then log_pass "$name"
    else
        log_fail "$name"
        $VERBOSE && { echo "  Command: $cmd"; eval "$cmd" 2>&1 | sed 's/^/  /'; } || true
    fi
}

# ── test setup / teardown ─────────────────────────────────────────────────────

setup() {
    TMPDIR_TEST="$(mktemp -d)"
    STATUS_DIR="$TMPDIR_TEST/logs/status"
    mkdir -p "$STATUS_DIR"

    # Fake .env — no real token, so Slack calls are skipped
    ENV_FILE="$TMPDIR_TEST/.env"
    touch "$ENV_FILE"

    # Minimal status JSON for hub "testhub" in an active state
    STATUS_FILE="$STATUS_DIR/testhub.status"
    python3 -c "
import json, datetime
print(json.dumps({
    'hub': 'testhub',
    'status': 'harvesting',
    'started_at': datetime.datetime.utcnow().isoformat(),
    'updated_at': datetime.datetime.utcnow().isoformat(),
}))" > "$STATUS_FILE"

    # Back-date the status file so it exceeds MIN_AGE_SECS (300s)
    touch -t "$(date -d '10 minutes ago' '+%Y%m%d%H%M.%S' 2>/dev/null \
              || date -v-10M '+%Y%m%d%H%M.%S')" "$STATUS_FILE"

    SENTINEL="$STATUS_DIR/testhub.watchdog-alerted"
}

teardown() {
    rm -rf "$TMPDIR_TEST"
}

# Run the watchdog in a controlled environment.  Since SLACK_BOT_TOKEN is
# empty the Slack call is skipped; we only care about the sentinel file.
run_watchdog() {
    I3_HOME="$TMPDIR_TEST" \
        bash "$WATCHDOG" 2>/dev/null
}

# ── tests ─────────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo "  Testing ingest-watchdog.sh"
echo "=========================================="

# 1. Syntax check
run_test "watchdog has valid bash syntax" "bash -n '$WATCHDOG'"

# 2. Script is executable
run_test "watchdog is executable" "[[ -x '$WATCHDOG' ]]"

# 3. Exits cleanly when STATUS_DIR does not exist
run_test "exits 0 when STATUS_DIR absent" \
    "I3_HOME=/nonexistent bash '$WATCHDOG'"

# ── Sentinel deduplication tests ──────────────────────────────────────────────
# The watchdog uses `stat -c '%Y'` (GNU/Linux syntax).  These tests require
# Linux and are skipped on macOS/BSD where that flag is unavailable.

echo ""
echo "--- Sentinel deduplication ---"

if ! stat -c '%Y' /dev/null >/dev/null 2>&1; then
    echo -e "${YELLOW}[SKIP]${NC} Sentinel tests require Linux stat — skipping on this platform"
    # Jump straight to summary
    echo ""
    echo "=========================================="
    echo "  Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
    echo "=========================================="
    [[ "$TESTS_FAILED" -eq 0 ]]
    exit $?
fi

# 4. Alert fires and sentinel is created on first detection
setup
TESTS_RUN=$((TESTS_RUN+1))
echo -e "${BLUE}[TEST]${NC} Sentinel created on first alert"
run_watchdog
if [[ -f "$SENTINEL" ]]; then
    log_pass "Sentinel created on first alert"
else
    log_fail "Sentinel not created after first alert"
fi
teardown

# 5. No duplicate alert when sentinel is newer than status file
setup
run_watchdog                                     # first run — creates sentinel
SENTINEL_MTIME_1=$(stat -c '%Y' "$SENTINEL" 2>/dev/null || stat -f '%m' "$SENTINEL")
sleep 1
run_watchdog                                     # second run — should skip
SENTINEL_MTIME_2=$(stat -c '%Y' "$SENTINEL" 2>/dev/null || stat -f '%m' "$SENTINEL")
TESTS_RUN=$((TESTS_RUN+1))
echo -e "${BLUE}[TEST]${NC} Sentinel mtime unchanged on duplicate run"
if [[ "$SENTINEL_MTIME_1" == "$SENTINEL_MTIME_2" ]]; then
    log_pass "Sentinel mtime unchanged on duplicate run"
else
    log_fail "Sentinel was re-touched on duplicate run (would re-alert)"
fi
teardown

# 6. Watchdog re-arms when a new ingest updates the status file
setup
run_watchdog                                     # first run — creates sentinel
# Simulate new ingest: update status file mtime to "now" (newer than sentinel)
sleep 1
touch "$STATUS_FILE"
# Back-date the sentinel so it is older than the freshly touched status file
touch -t "$(date -d '2 minutes ago' '+%Y%m%d%H%M.%S' 2>/dev/null \
          || date -v-2M '+%Y%m%d%H%M.%S')" "$SENTINEL"
# Also back-date the status file well beyond MIN_AGE_SECS
touch -t "$(date -d '10 minutes ago' '+%Y%m%d%H%M.%S' 2>/dev/null \
          || date -v-10M '+%Y%m%d%H%M.%S')" "$STATUS_FILE"
SENTINEL_MTIME_BEFORE=$(stat -c '%Y' "$SENTINEL" 2>/dev/null || stat -f '%m' "$SENTINEL")
run_watchdog                                     # should re-alert and re-touch sentinel
SENTINEL_MTIME_AFTER=$(stat -c '%Y' "$SENTINEL" 2>/dev/null || stat -f '%m' "$SENTINEL")
TESTS_RUN=$((TESTS_RUN+1))
echo -e "${BLUE}[TEST]${NC} Sentinel re-touched when status file is newer (re-arm)"
if [[ "$SENTINEL_MTIME_AFTER" -gt "$SENTINEL_MTIME_BEFORE" ]]; then
    log_pass "Sentinel re-touched when status file is newer (re-arm)"
else
    log_fail "Sentinel NOT re-touched — watchdog failed to re-arm after new ingest"
fi
teardown

# 7. Terminal status (e.g. "complete") is not alerted
setup
python3 -c "
import json, datetime
print(json.dumps({
    'hub': 'testhub',
    'status': 'complete',
    'started_at': datetime.datetime.utcnow().isoformat(),
    'updated_at': datetime.datetime.utcnow().isoformat(),
}))" > "$STATUS_FILE"
touch -t "$(date -d '10 minutes ago' '+%Y%m%d%H%M.%S' 2>/dev/null \
          || date -v-10M '+%Y%m%d%H%M.%S')" "$STATUS_FILE"
run_watchdog
TESTS_RUN=$((TESTS_RUN+1))
echo -e "${BLUE}[TEST]${NC} No sentinel created for terminal status"
if [[ ! -f "$SENTINEL" ]]; then
    log_pass "No sentinel created for terminal status"
else
    log_fail "Sentinel was created for a 'complete' status — should be skipped"
fi
teardown

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "=========================================="
echo "  Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
echo "=========================================="

[[ "$TESTS_FAILED" -eq 0 ]]
