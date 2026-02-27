#!/usr/bin/env bash
#
# test-scripts.sh - Verify DPLA ingestion scripts work correctly
#
# This test suite validates:
#   1. All scripts have valid bash syntax
#   2. All scripts source common.sh correctly
#   3. Environment variable defaults work on both platforms
#   4. Help/usage outputs work without errors
#   5. Cross-platform functions work correctly
#
# Usage:
#   ./scripts/tests/test-scripts.sh           # Run all tests
#   ./scripts/tests/test-scripts.sh --quick   # Syntax checks only
#   ./scripts/tests/test-scripts.sh --verbose # Show all output
#

set -euo pipefail

# Get the directory containing this test script
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(dirname "$TEST_DIR")"
I3_HOME="$(dirname "$SCRIPTS_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Options
QUICK_MODE=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick) QUICK_MODE=true; shift ;;
        --verbose|-v) VERBOSE=true; shift ;;
        --help|-h)
            echo "Usage: $0 [--quick] [--verbose]"
            echo "  --quick    Only run syntax checks"
            echo "  --verbose  Show detailed output"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# =============================================================================
# Test Framework Functions
# =============================================================================

log_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
}

log_info() {
    [[ "$VERBOSE" == "true" ]] && echo -e "${YELLOW}[INFO]${NC} $1"
}

run_test() {
    local name="$1"
    local cmd="$2"
    TESTS_RUN=$((TESTS_RUN + 1))
    
    log_test "$name"
    
    if eval "$cmd" >/dev/null 2>&1; then
        log_pass "$name"
        return 0
    else
        log_fail "$name"
        if [[ "$VERBOSE" == "true" ]]; then
            echo "  Command: $cmd"
            eval "$cmd" 2>&1 | sed 's/^/  /'
        fi
        return 1
    fi
}

assert_equals() {
    local expected="$1"
    local actual="$2"
    local name="${3:-assertion}"
    TESTS_RUN=$((TESTS_RUN + 1))
    
    if [[ "$expected" == "$actual" ]]; then
        log_pass "$name"
        return 0
    else
        log_fail "$name: expected '$expected', got '$actual'"
        return 1
    fi
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local name="${3:-assertion}"
    TESTS_RUN=$((TESTS_RUN + 1))
    
    if [[ "$haystack" == *"$needle"* ]]; then
        log_pass "$name"
        return 0
    else
        log_fail "$name: '$haystack' does not contain '$needle'"
        return 1
    fi
}

assert_file_exists() {
    local file="$1"
    local name="${2:-file exists: $file}"
    TESTS_RUN=$((TESTS_RUN + 1))
    
    if [[ -f "$file" ]]; then
        log_pass "$name"
        return 0
    else
        log_fail "$name"
        return 1
    fi
}

# =============================================================================
# Test: common.sh exists and is valid
# =============================================================================

test_common_sh() {
    echo ""
    echo "=========================================="
    echo "  Testing common.sh"
    echo "=========================================="
    
    local common_sh="$SCRIPTS_DIR/common.sh"
    
    # Test file exists
    assert_file_exists "$common_sh" "common.sh exists"
    
    # Test syntax is valid
    run_test "common.sh has valid syntax" "bash -n '$common_sh'"
    
    # Test it can be sourced
    run_test "common.sh can be sourced" "source '$common_sh'"
    
    # Test platform detection works
    if source "$common_sh" 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        if [[ -n "${PLATFORM:-}" ]]; then
            log_pass "PLATFORM is set: $PLATFORM"
        else
            log_fail "PLATFORM is not set after sourcing common.sh"
        fi
        
        # Test sed_i function exists
        TESTS_RUN=$((TESTS_RUN + 1))
        if declare -f sed_i >/dev/null 2>&1; then
            log_pass "sed_i function is defined"
        else
            log_fail "sed_i function is not defined"
        fi
        
        # Test get_script_dir function exists
        TESTS_RUN=$((TESTS_RUN + 1))
        if declare -f get_script_dir >/dev/null 2>&1; then
            log_pass "get_script_dir function is defined"
        else
            log_fail "get_script_dir function is not defined"
        fi
    fi
}

# =============================================================================
# Test: All scripts have valid bash syntax
# =============================================================================

test_syntax() {
    echo ""
    echo "=========================================="
    echo "  Testing Script Syntax"
    echo "=========================================="
    
    # Scripts in root and subfolders (relative to SCRIPTS_DIR)
    local scripts=(
        "auto-ingest.sh"
        "batch-ingest.sh"
        "enrich.sh"
        "harvest.sh"
        "ingest.sh"
        "jsonl.sh"
        "mapping.sh"
        "remap.sh"
        "s3-sync.sh"
        "communication/notify-harvest-failure.sh"
        "communication/schedule.sh"
        "communication/send-ingest-email.sh"
        "delete/delete-by-id.sh"
        "delete/delete-from-jsonl.sh"
        "harvest/community-webs-export.sh"
        "harvest/community-webs-ingest.sh"
        "harvest/fix-si.sh"
        "harvest/harvest-va.sh"
        "harvest/nara-ingest.sh"
        "status/check-jsonl-sync.sh"
        "status/ingest-status.sh"
        "status/monitor-pipeline.sh"
    )
    
    for script in "${scripts[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        if [[ -f "$script_path" ]]; then
            run_test "Syntax: $script" "bash -n '$script_path'"
        else
            log_skip "Syntax: $script (file not found)"
        fi
    done
}

# =============================================================================
# Test: Scripts source common.sh
# =============================================================================

test_common_sourcing() {
    echo ""
    echo "=========================================="
    echo "  Testing common.sh Sourcing"
    echo "=========================================="
    
    local common_sh="$SCRIPTS_DIR/common.sh"
    
    if [[ ! -f "$common_sh" ]]; then
        log_skip "common.sh not found - skipping sourcing tests"
        return
    fi
    
    # Root pipeline scripts and subfolder scripts that source common.sh
    local scripts=(
        "auto-ingest.sh"
        "batch-ingest.sh"
        "enrich.sh"
        "harvest.sh"
        "ingest.sh"
        "jsonl.sh"
        "mapping.sh"
        "remap.sh"
        "communication/send-ingest-email.sh"
        "communication/schedule.sh"
        "delete/delete-by-id.sh"
        "delete/delete-from-jsonl.sh"
        "harvest/community-webs-export.sh"
        "harvest/community-webs-ingest.sh"
        "harvest/fix-si.sh"
        "harvest/harvest-va.sh"
        "harvest/nara-ingest.sh"
        "status/check-jsonl-sync.sh"
    )
    
    for script in "${scripts[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        TESTS_RUN=$((TESTS_RUN + 1))
        
        if [[ ! -f "$script_path" ]]; then
            log_skip "Sourcing: $script (file not found)"
            continue
        fi
        
        # Accept source "$SCRIPTS_ROOT/common.sh" or source "$SCRIPT_DIR/../common.sh" or similar
        if grep -q 'source.*common\.sh\|\..*common\.sh' "$script_path" 2>/dev/null; then
            log_pass "Sourcing: $script sources common.sh"
        else
            log_fail "Sourcing: $script does not source common.sh"
        fi
    done
}

# =============================================================================
# Test: Help/Usage outputs work
# =============================================================================

test_help_outputs() {
    echo ""
    echo "=========================================="
    echo "  Testing Help/Usage Outputs"
    echo "=========================================="
    
    if [[ "$QUICK_MODE" == "true" ]]; then
        log_skip "Help tests (quick mode)"
        return
    fi
    
    # Scripts that should support --help (paths relative to SCRIPTS_DIR)
    local scripts_with_help=(
        "auto-ingest.sh"
        "batch-ingest.sh"
        "delete/delete-by-id.sh"
        "delete/delete-from-jsonl.sh"
        "ingest.sh"
        "harvest/nara-ingest.sh"
        "communication/schedule.sh"
    )
    
    for script in "${scripts_with_help[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        TESTS_RUN=$((TESTS_RUN + 1))
        
        if [[ ! -f "$script_path" ]]; then
            log_skip "Help: $script (file not found)"
            continue
        fi
        
        # Try --help (should exit 0 or 1, not crash)
        if "$script_path" --help >/dev/null 2>&1; then
            log_pass "Help: $script --help works"
        elif [[ $? -eq 1 ]]; then
            # Exit 1 is acceptable for help (some scripts do this)
            log_pass "Help: $script --help works (exit 1)"
        else
            log_fail "Help: $script --help failed"
        fi
    done
}

# =============================================================================
# Test: Cross-platform functions work
# =============================================================================

test_cross_platform() {
    echo ""
    echo "=========================================="
    echo "  Testing Cross-Platform Functions"
    echo "=========================================="
    
    local common_sh="$SCRIPTS_DIR/common.sh"
    
    if [[ ! -f "$common_sh" ]]; then
        log_skip "common.sh not found - skipping cross-platform tests"
        return
    fi
    
    # Source common.sh in a subshell to test functions
    (
        source "$common_sh"
        
        # Test sed_i with a temp file
        local tmpfile
        tmpfile=$(mktemp)
        echo "hello world" > "$tmpfile"
        
        if sed_i "s/world/universe/" "$tmpfile" 2>/dev/null; then
            if grep -q "hello universe" "$tmpfile"; then
                echo "PASS: sed_i works correctly"
            else
                echo "FAIL: sed_i did not modify file correctly"
                exit 1
            fi
        else
            echo "FAIL: sed_i command failed"
            exit 1
        fi
        
        rm -f "$tmpfile"
        
        # Test get_script_dir
        local dir
        dir=$(get_script_dir)
        if [[ -n "$dir" && -d "$dir" ]]; then
            echo "PASS: get_script_dir returned valid directory: $dir"
        else
            echo "FAIL: get_script_dir returned invalid: $dir"
            exit 1
        fi
    )
    
    local result=$?
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ $result -eq 0 ]]; then
        log_pass "Cross-platform functions work"
    else
        log_fail "Cross-platform functions failed"
    fi
}

# =============================================================================
# Test: Environment variable defaults
# =============================================================================

test_env_defaults() {
    echo ""
    echo "=========================================="
    echo "  Testing Environment Variable Defaults"
    echo "=========================================="
    
    local common_sh="$SCRIPTS_DIR/common.sh"
    
    if [[ ! -f "$common_sh" ]]; then
        log_skip "common.sh not found - skipping env tests"
        return
    fi
    
    # Test in a clean subshell with unset variables
    # Use bash explicitly to ensure clean environment
    bash -c "
        unset I3_HOME DPLA_DATA I3_CONF JAVA_HOME
        source '$common_sh'
        
        # I3_HOME should be set
        if [[ -n \"\${I3_HOME:-}\" ]]; then
            echo \"I3_HOME=\$I3_HOME\"
        else
            echo \"FAIL: I3_HOME not set\"
            exit 1
        fi
        
        # DPLA_DATA should be set
        if [[ -n \"\${DPLA_DATA:-}\" ]]; then
            echo \"DPLA_DATA=\$DPLA_DATA\"
        else
            echo \"FAIL: DPLA_DATA not set\"
            exit 1
        fi
        
        # PLATFORM should be set
        if [[ -n \"\${PLATFORM:-}\" ]]; then
            echo \"PLATFORM=\$PLATFORM\"
        else
            echo \"FAIL: PLATFORM not set\"
            exit 1
        fi
    "
    
    local result=$?
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ $result -eq 0 ]]; then
        log_pass "Environment defaults are set correctly"
    else
        log_fail "Environment defaults failed"
    fi
}

# =============================================================================
# Test: No hardcoded /Users/scott paths
# =============================================================================

test_no_hardcoded_paths() {
    echo ""
    echo "=========================================="
    echo "  Testing for Hardcoded Paths"
    echo "=========================================="
    
    local scripts=(
        "auto-ingest.sh"
        "batch-ingest.sh"
        "enrich.sh"
        "harvest.sh"
        "ingest.sh"
        "jsonl.sh"
        "mapping.sh"
        "remap.sh"
        "s3-sync.sh"
    )
    
    for script in "${scripts[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        TESTS_RUN=$((TESTS_RUN + 1))
        
        if [[ ! -f "$script_path" ]]; then
            log_skip "Hardcoded paths: $script (file not found)"
            continue
        fi
        
        # Check for hardcoded /Users/scott (excluding comments)
        if grep -v '^[[:space:]]*#' "$script_path" | grep -q '/Users/scott' 2>/dev/null; then
            log_fail "Hardcoded paths: $script contains /Users/scott"
            if [[ "$VERBOSE" == "true" ]]; then
                grep -n '/Users/scott' "$script_path" | head -5 | sed 's/^/  /'
            fi
        else
            log_pass "Hardcoded paths: $script is clean"
        fi
    done
}

# =============================================================================
# Test: run_entry and kill_tree functions exist in common.sh
# =============================================================================

test_common_functions() {
    echo ""
    echo "=========================================="
    echo "  Testing common.sh Functions"
    echo "=========================================="
    
    local common_sh="$SCRIPTS_DIR/common.sh"
    
    if [[ ! -f "$common_sh" ]]; then
        log_skip "common.sh not found - skipping function tests"
        return
    fi
    
    # Source common.sh in a subshell
    (
        source "$common_sh"
        
        # Test kill_tree exists
        if declare -f kill_tree >/dev/null 2>&1; then
            echo "PASS: kill_tree"
        else
            echo "FAIL: kill_tree not defined"
            exit 1
        fi
        
        # Test run_entry exists
        if declare -f run_entry >/dev/null 2>&1; then
            echo "PASS: run_entry"
        else
            echo "FAIL: run_entry not defined"
            exit 1
        fi
        
        # Test run_ingest_remap exists
        if declare -f run_ingest_remap >/dev/null 2>&1; then
            echo "PASS: run_ingest_remap"
        else
            echo "FAIL: run_ingest_remap not defined"
            exit 1
        fi
        
        # Test find_latest_data exists
        if declare -f find_latest_data >/dev/null 2>&1; then
            echo "PASS: find_latest_data"
        else
            echo "FAIL: find_latest_data not defined"
            exit 1
        fi
    )
    
    local result=$?
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ $result -eq 0 ]]; then
        log_pass "common.sh has kill_tree, run_entry, run_ingest_remap, find_latest_data"
    else
        log_fail "common.sh missing required functions"
    fi
}

# =============================================================================
# Test: --output path convention (must be $DPLA_DATA, not $DPLA_DATA/$PROVIDER)
# =============================================================================

test_output_path_convention() {
    echo ""
    echo "=========================================="
    echo "  Testing --output Path Convention"
    echo "=========================================="
    
    # These scripts must use OUTPUT="$DPLA_DATA" (not $DPLA_DATA/$PROVIDER)
    local scripts=(
        "mapping.sh"
        "enrich.sh"
        "harvest.sh"
        "remap.sh"
        "jsonl.sh"
    )
    
    for script in "${scripts[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        TESTS_RUN=$((TESTS_RUN + 1))
        
        if [[ ! -f "$script_path" ]]; then
            log_skip "Output path: $script (file not found)"
            continue
        fi
        
        # Check for the WRONG pattern: OUTPUT="$DPLA_DATA/$PROVIDER"
        if grep -v '^[[:space:]]*#' "$script_path" | grep -q 'OUTPUT=.*\$DPLA_DATA/\$PROVIDER' 2>/dev/null; then
            log_fail "Output path: $script has OUTPUT=\$DPLA_DATA/\$PROVIDER (should be \$DPLA_DATA)"
            if [[ "$VERBOSE" == "true" ]]; then
                grep -n 'OUTPUT=.*\$DPLA_DATA/\$PROVIDER' "$script_path" | head -3 | sed 's/^/  /'
            fi
        else
            log_pass "Output path: $script uses correct convention"
        fi
    done

    # ingest.sh: HarvestEntry and IngestRemap must receive --output=$DPLA_DATA (not HARVEST_DIR/PROVIDER_DATA)
    local ingest_sh="$SCRIPTS_DIR/ingest.sh"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ -f "$ingest_sh" ]]; then
        local fail=0
        if grep -v '^[[:space:]]*#' "$ingest_sh" | grep -q '--output=.*\$HARVEST_DIR\|--output=.*HARVEST_DIR' 2>/dev/null; then
            log_fail "Output path: ingest.sh harvest uses --output=\$HARVEST_DIR (should be \$DPLA_DATA)"
            fail=1
        fi
        if grep -v '^[[:space:]]*#' "$ingest_sh" | grep 'run_ingest_remap' | grep -q '\$PROVIDER_DATA' 2>/dev/null; then
            log_fail "Output path: ingest.sh remap passes \$PROVIDER_DATA as output (should be \$DPLA_DATA)"
            fail=1
        fi
        if [[ $fail -eq 0 ]]; then
            log_pass "Output path: ingest.sh uses correct convention (\$DPLA_DATA for harvest and remap)"
        fi
    else
        log_skip "Output path: ingest.sh (file not found)"
    fi
}

# =============================================================================
# Test: jsonl.sh has lock mechanism
# =============================================================================

test_jsonl_lock_mechanism() {
    echo ""
    echo "=========================================="
    echo "  Testing jsonl.sh Lock Mechanism"
    echo "=========================================="
    
    local jsonl_sh="$SCRIPTS_DIR/jsonl.sh"
    
    if [[ ! -f "$jsonl_sh" ]]; then
        log_skip "jsonl.sh not found"
        return
    fi
    
    # Check for LOCK_FILE variable
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -q 'LOCK_FILE=' "$jsonl_sh" 2>/dev/null; then
        log_pass "jsonl.sh has LOCK_FILE variable"
    else
        log_fail "jsonl.sh missing LOCK_FILE variable"
    fi
    
    # Check for cleanup trap
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -q 'trap cleanup EXIT' "$jsonl_sh" 2>/dev/null; then
        log_pass "jsonl.sh has cleanup trap"
    else
        log_fail "jsonl.sh missing cleanup trap"
    fi
    
    # Check for kill_tree usage
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -q 'kill_tree' "$jsonl_sh" 2>/dev/null; then
        log_pass "jsonl.sh uses kill_tree"
    else
        log_fail "jsonl.sh missing kill_tree usage"
    fi
}

# =============================================================================
# Test: Scripts use run_entry instead of raw sbt
# =============================================================================

test_scripts_use_run_entry() {
    echo ""
    echo "=========================================="
    echo "  Testing Scripts Use run_entry"
    echo "=========================================="
    
    # These scripts should use run_entry (not raw sbt invocation)
    local scripts=(
        "mapping.sh"
        "enrich.sh"
        "harvest.sh"
        "jsonl.sh"
    )
    
    for script in "${scripts[@]}"; do
        local script_path="$SCRIPTS_DIR/$script"
        TESTS_RUN=$((TESTS_RUN + 1))
        
        if [[ ! -f "$script_path" ]]; then
            log_skip "run_entry: $script (file not found)"
            continue
        fi
        
        if grep -q 'run_entry' "$script_path" 2>/dev/null; then
            log_pass "run_entry: $script uses run_entry"
        else
            log_fail "run_entry: $script does not use run_entry"
        fi
    done
    
    # remap.sh should use run_ingest_remap (which calls run_entry internally)
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ -f "$SCRIPTS_DIR/remap.sh" ]]; then
        if grep -q 'run_ingest_remap' "$SCRIPTS_DIR/remap.sh" 2>/dev/null; then
            log_pass "run_entry: remap.sh uses run_ingest_remap"
        else
            log_fail "run_entry: remap.sh does not use run_ingest_remap"
        fi
    else
        log_skip "run_entry: remap.sh (file not found)"
    fi
}

# =============================================================================
# Test: Referenced script paths exist (callers/docs reference these; no invocation)
# =============================================================================

test_referenced_script_paths() {
    echo ""
    echo "=========================================="
    echo "  Referenced Script Paths Exist"
    echo "=========================================="
    
    # Paths referenced by HarvestExecutor, orchestrator, docs, skills (relative to SCRIPTS_DIR)
    local paths=(
        "common.sh"
        "ingest.sh"
        "harvest.sh"
        "remap.sh"
        "mapping.sh"
        "enrich.sh"
        "jsonl.sh"
        "s3-sync.sh"
        "communication/notify-harvest-failure.sh"
        "communication/send-harvest-failure-email.py"
        "communication/send-ingest-email.sh"
        "communication/schedule.sh"
        "status/ingest-status.sh"
        "status/check-jsonl-sync.sh"
        "harvest/nara-ingest.sh"
        "harvest/community-webs-ingest.sh"
        "harvest/community-webs-export.sh"
        "harvest/fix-si.sh"
        "delete/delete-by-id.sh"
        "delete/delete-from-jsonl.sh"
    )
    
    for rel in "${paths[@]}"; do
        local script_path="$SCRIPTS_DIR/$rel"
        TESTS_RUN=$((TESTS_RUN + 1))
        if [[ -f "$script_path" ]]; then
            log_pass "Exists: $rel"
        else
            log_fail "Missing: $rel"
        fi
    done
}

# =============================================================================
# Test: Deleted NARA-specific scripts no longer exist
# =============================================================================

test_nara_scripts_deleted() {
    echo ""
    echo "=========================================="
    echo "  Testing NARA Scripts Deleted"
    echo "=========================================="
    
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ ! -f "$SCRIPTS_DIR/run-nara-jsonl.sh" ]]; then
        log_pass "run-nara-jsonl.sh is deleted"
    else
        log_fail "run-nara-jsonl.sh still exists (should be deleted)"
    fi
    
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ ! -f "$SCRIPTS_DIR/monitor-nara-jsonl.sh" ]]; then
        log_pass "monitor-nara-jsonl.sh is deleted"
    else
        log_fail "monitor-nara-jsonl.sh still exists (should be deleted)"
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo "=========================================="
    echo "  DPLA Ingestion Scripts Test Suite"
    echo "=========================================="
    echo "Scripts directory: $SCRIPTS_DIR"
    echo "Platform: ${OSTYPE:-unknown}"
    echo "Quick mode: $QUICK_MODE"
    echo "Verbose: $VERBOSE"
    
    # Run tests
    test_common_sh
    test_syntax
    test_referenced_script_paths
    
    if [[ "$QUICK_MODE" != "true" ]]; then
        test_common_sourcing
        test_common_functions
        test_help_outputs
        test_cross_platform
        test_env_defaults
        test_no_hardcoded_paths
        test_output_path_convention
        test_jsonl_lock_mechanism
        test_scripts_use_run_entry
        test_nara_scripts_deleted
    fi
    
    # Summary
    echo ""
    echo "=========================================="
    echo "  Test Summary"
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
}

main "$@"
