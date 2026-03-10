#!/usr/bin/env bash
#
# test-integration.sh - Lightweight integration smoke tests
#
# Verifies the fat JAR and scripts work end-to-end without running actual ingests.
#
# Usage:
#   ./scripts/tests/test-integration.sh
#

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(dirname "$TEST_DIR")"
I3_HOME="$(dirname "$SCRIPTS_DIR")"
JAR="$I3_HOME/target/scala-2.13/ingestion3-assembly-0.0.1.jar"
VENV="$I3_HOME/venv"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; TESTS_PASSED=$((TESTS_PASSED + 1)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; TESTS_FAILED=$((TESTS_FAILED + 1)); }
log_skip() { echo -e "${YELLOW}[SKIP]${NC} $1"; TESTS_SKIPPED=$((TESTS_SKIPPED + 1)); }

echo "=========================================="
echo "  Integration Smoke Tests"
echo "=========================================="
echo "I3_HOME: $I3_HOME"
echo ""

# =============================================================================
# Test: Fat JAR exists
# =============================================================================
echo ""
echo "--- Fat JAR ---"
TESTS_RUN=$((TESTS_RUN + 1))
if [[ -f "$JAR" ]]; then
    jar_size=$(du -h "$JAR" | cut -f1)
    log_pass "Fat JAR exists ($jar_size)"
else
    log_fail "Fat JAR not found at $JAR"
    echo "  Run: cd $I3_HOME && sbt assembly"
fi

# =============================================================================
# Test: Fat JAR has IngestRemap class
# =============================================================================
TESTS_RUN=$((TESTS_RUN + 1))
if [[ -f "$JAR" ]]; then
    # Running IngestRemap with no args should fail with usage error, not ClassNotFoundException
    set +e
    output=$(java -cp "$JAR" dpla.ingestion3.entries.ingest.IngestRemap 2>&1)
    rc=$?
    set -e

    if echo "$output" | grep -qi 'ClassNotFoundException'; then
        log_fail "JAR missing IngestRemap class"
    else
        # Any non-zero exit is fine (missing args), as long as the class loaded
        log_pass "JAR has IngestRemap class (exit=$rc)"
    fi
else
    log_skip "JAR not found - skipping class test"
fi

# =============================================================================
# Test: jsonl.sh usage output
# =============================================================================
echo ""
echo "--- Script Usage ---"
TESTS_RUN=$((TESTS_RUN + 1))
set +e
output=$("$SCRIPTS_DIR/jsonl.sh" 2>&1)
rc=$?
set -e

if [[ $rc -eq 1 ]] && echo "$output" | grep -q 'Usage:'; then
    log_pass "jsonl.sh shows usage (exit 1)"
else
    log_fail "jsonl.sh usage test failed (exit=$rc)"
fi

# =============================================================================
# Test: mapping.sh usage output
# =============================================================================
TESTS_RUN=$((TESTS_RUN + 1))
set +e
output=$("$SCRIPTS_DIR/mapping.sh" 2>&1)
rc=$?
set -e

if [[ $rc -eq 1 ]] && echo "$output" | grep -q 'Usage:'; then
    log_pass "mapping.sh shows usage (exit 1)"
else
    log_fail "mapping.sh usage test failed (exit=$rc)"
fi

# =============================================================================
# Test: common.sh can be sourced and run_entry is defined
# =============================================================================
echo ""
echo "--- common.sh ---"
TESTS_RUN=$((TESTS_RUN + 1))
set +e
result=$(bash -c "source '$SCRIPTS_DIR/common.sh' && declare -f run_entry >/dev/null && echo OK" 2>&1)
set -e

if [[ "$result" == "OK" ]]; then
    log_pass "common.sh sources and run_entry is defined"
else
    log_fail "common.sh sourcing or run_entry failed"
fi

# =============================================================================
# Test: Python venv exists and has pytest
# =============================================================================
echo ""
echo "--- Python Venv ---"
TESTS_RUN=$((TESTS_RUN + 1))
if [[ -f "$VENV/bin/python" ]]; then
    py_version=$("$VENV/bin/python" --version 2>&1)
    log_pass "Python venv exists ($py_version)"
else
    log_fail "Python venv not found at $VENV"
fi

TESTS_RUN=$((TESTS_RUN + 1))
if [[ -f "$VENV/bin/pytest" ]]; then
    log_pass "pytest is installed in venv"
else
    log_skip "pytest not installed in venv (run: $VENV/bin/pip install pytest)"
fi

# =============================================================================
# Test: Python orchestrator can be imported
# =============================================================================
TESTS_RUN=$((TESTS_RUN + 1))
if [[ -f "$VENV/bin/python" ]]; then
    set +e
    "$VENV/bin/python" -c "from scheduler.orchestrator.config import ResourceBudget; print('OK')" 2>/dev/null
    rc=$?
    set -e

    if [[ $rc -eq 0 ]]; then
        log_pass "Python orchestrator imports successfully"
    else
        log_fail "Python orchestrator import failed"
    fi
else
    log_skip "Python venv not available"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=========================================="
echo "  Integration Test Summary"
echo "=========================================="
echo -e "Total:   $TESTS_RUN"
echo -e "Passed:  ${GREEN}$TESTS_PASSED${NC}"
echo -e "Failed:  ${RED}$TESTS_FAILED${NC}"
echo -e "Skipped: ${YELLOW}$TESTS_SKIPPED${NC}"
echo ""

if [[ $TESTS_FAILED -gt 0 ]]; then
    echo -e "${RED}TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
    exit 0
fi
