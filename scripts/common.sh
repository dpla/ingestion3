#!/usr/bin/env bash
#
# common.sh - Shared configuration and utilities for DPLA ingestion scripts
#
# This file provides cross-platform compatible utilities and configuration
# that work on both macOS and Ubuntu Linux.
#
# Usage: Source this file at the top of other scripts:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/common.sh"
#
# Provided variables:
#   PLATFORM       - "macos" or "linux" or "unknown"
#   I3_HOME        - Ingestion3 root directory
#   DPLA_DATA      - Data output directory
#   I3_CONF        - Path to i3.conf configuration
#   JAVA_HOME      - Java installation directory
#   SBT_OPTS       - JVM options for sbt
#
# Provided functions:
#   get_script_dir    - Get directory containing the calling script (portable)
#   sed_i             - Portable in-place sed (works on macOS and Linux)
#   setup_java        - Configure Java environment
#   setup_colors      - Define color variables for output
#   log_info/warn/error/success - Logging functions
#   kill_tree         - Recursively kill a process tree (prevents orphan JVMs)
#   run_entry         - Run any Scala entry class via JAR or sbt
#   run_ingest_remap  - Convenience wrapper for IngestRemap entry point
#   write_hub_status  - Write per-hub .status file (for ingest-status.sh)
#   find_latest_data  - Find most recent timestamped data directory
#   require_command   - Check that a command exists, exit if not
#

# Prevent double-sourcing
[[ -n "${_COMMON_SH_LOADED:-}" ]] && return 0
_COMMON_SH_LOADED=1

# =============================================================================
# Platform Detection
# =============================================================================

detect_platform() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "linux"
    else
        echo "unknown"
    fi
}

PLATFORM=$(detect_platform)
export PLATFORM

# =============================================================================
# Portable Utility Functions
# =============================================================================

# Get the directory containing the calling script
# This is a portable replacement for: $(dirname "$(readlink -f "$0")")
# Works on both macOS (no readlink -f) and Linux
#
# Usage from other scripts:
#   SCRIPT_DIR=$(get_script_dir)
#
# Note: When called from get_i3_home(), we want common.sh's dir (BASH_SOURCE[0])
get_script_dir() {
    # Determine which BASH_SOURCE index to use
    # - When called from another script: use BASH_SOURCE[1] (the caller)
    # - When called internally or directly: use BASH_SOURCE[0] (this file)
    local source
    if [[ ${#BASH_SOURCE[@]} -gt 1 && -n "${BASH_SOURCE[1]:-}" ]]; then
        source="${BASH_SOURCE[1]}"
    else
        source="${BASH_SOURCE[0]}"
    fi

    local dir

    # Resolve symlinks
    while [[ -L "$source" ]]; do
        dir=$(cd -P "$(dirname "$source")" && pwd)
        source=$(readlink "$source")
        # Handle relative symlinks
        [[ "$source" != /* ]] && source="$dir/$source"
    done

    cd -P "$(dirname "$source")" && pwd
}

# Get the directory where common.sh is located (always BASH_SOURCE[0])
get_common_dir() {
    local source="${BASH_SOURCE[0]}"
    local dir

    while [[ -L "$source" ]]; do
        dir=$(cd -P "$(dirname "$source")" && pwd)
        source=$(readlink "$source")
        [[ "$source" != /* ]] && source="$dir/$source"
    done

    cd -P "$(dirname "$source")" && pwd
}

# Portable in-place sed
# macOS requires: sed -i '' 's/foo/bar/' file
# Linux requires: sed -i 's/foo/bar/' file
sed_i() {
    if [[ "$PLATFORM" == "macos" ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

# =============================================================================
# Java Configuration
# =============================================================================

# Detect Java installation
detect_java_home() {
    # If JAVA_HOME is already set and valid, use it
    if [[ -n "${JAVA_HOME:-}" && -d "$JAVA_HOME" ]]; then
        echo "$JAVA_HOME"
        return 0
    fi

    local java_home=""

    if [[ "$PLATFORM" == "macos" ]]; then
        # macOS: use java_home helper
        if [[ -x /usr/libexec/java_home ]]; then
            java_home=$(/usr/libexec/java_home 2>/dev/null || true)
        fi
        # Fallback to common locations
        if [[ -z "$java_home" || ! -d "$java_home" ]]; then
            for dir in \
                "$HOME/Library/Java/JavaVirtualMachines"/*/Contents/Home \
                /Library/Java/JavaVirtualMachines/*/Contents/Home \
                /opt/homebrew/opt/openjdk*/libexec/openjdk.jdk/Contents/Home
            do
                if [[ -d "$dir" ]]; then
                    java_home="$dir"
                    break
                fi
            done
        fi
    else
        # Linux: check common locations
        for dir in \
            /usr/lib/jvm/java-*-openjdk-amd64 \
            /usr/lib/jvm/java-*-openjdk \
            /usr/lib/jvm/default-java \
            /opt/java/openjdk \
            /usr/java/latest
        do
            if [[ -d "$dir" ]]; then
                java_home="$dir"
                break
            fi
        done

        # Try to find from java command
        if [[ -z "$java_home" ]] && command -v java >/dev/null 2>&1; then
            local java_bin
            java_bin=$(readlink -f "$(which java)" 2>/dev/null || true)
            if [[ -n "$java_bin" ]]; then
                java_home=$(dirname "$(dirname "$java_bin")")
            fi
        fi
    fi

    echo "$java_home"
}

# Setup Java environment variables
setup_java() {
    local memory="${1:-8g}"

    # Detect JAVA_HOME if not set
    if [[ -z "${JAVA_HOME:-}" ]]; then
        JAVA_HOME=$(detect_java_home)
    fi

    if [[ -z "$JAVA_HOME" || ! -d "$JAVA_HOME" ]]; then
        log_error "Could not find Java installation"
        log_error "Please set JAVA_HOME environment variable"
        return 1
    fi

    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"

    # Set SBT options based on memory parameter.
    # If SBT_OPTS is already set (e.g. by the orchestrator's resource budget),
    # respect that value instead of overriding it.
    if [[ -z "${SBT_OPTS:-}" ]]; then
        export SBT_OPTS="-Xms2g -Xmx${memory} -XX:+UseG1GC"
    else
        log_debug "SBT_OPTS already set: $SBT_OPTS (not overriding)"
    fi

    return 0
}

# =============================================================================
# Path Configuration
# =============================================================================

# Get the ingestion3 root directory
# Can be overridden with I3_HOME environment variable
get_i3_home() {
    if [[ -n "${I3_HOME:-}" ]]; then
        echo "$I3_HOME"
    else
        # Determine from common.sh's location (this script is in $I3_HOME/scripts/)
        local common_dir
        common_dir=$(get_common_dir 2>/dev/null || echo "")

        if [[ -n "$common_dir" && -d "$common_dir" ]]; then
            dirname "$common_dir"
        else
            # Fallback to common default location
            echo "$HOME/dpla/code/ingestion3"
        fi
    fi
}

# Initialize paths with sensible defaults
# These can all be overridden with environment variables
init_paths() {
    # I3_HOME: Ingestion3 repository root
    I3_HOME="${I3_HOME:-$(get_i3_home)}"
    export I3_HOME

    # Load .env when present so SLACK_WEBHOOK, JAVA_HOME, etc. are set for
    # pipeline scripts and harvest-failure notifications.
    if [ -f "${I3_HOME}/.env" ] && [ -r "${I3_HOME}/.env" ]; then
        set -a
        # shellcheck source=/dev/null
        source "${I3_HOME}/.env"
        set +a
    fi

    # DPLA_DATA: Where harvested/processed data is stored
    DPLA_DATA="${DPLA_DATA:-$HOME/dpla/data}"
    export DPLA_DATA

    # I3_CONF: Configuration file location
    I3_CONF="${I3_CONF:-$HOME/dpla/code/ingestion3-conf/i3.conf}"
    export I3_CONF

    # SPARK_MASTER: Spark execution mode for pipeline (mapping/enrichment/jsonl).
    # local[4] is a safe default for IngestRemap on typical workstations (e.g. M3 Pro 18GB);
    # override with SPARK_MASTER=local[6] for more parallelism when memory allows.
    SPARK_MASTER="${SPARK_MASTER:-local[4]}"
    export SPARK_MASTER

    # AWS credentials: strip anything injected by external runners (e.g. SSM
    # agent sets AWS_SHARED_CREDENTIALS_FILE to its own creds file, which
    # would make the AWS CLI use the SSM role instead of the EC2 instance
    # role). Unsetting these forces the CLI to fall back to IMDS.
    unset AWS_SHARED_CREDENTIALS_FILE AWS_ACCESS_KEY_ID \
          AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

    # AWS_PROFILE: only set if explicitly provided — EC2 uses an instance role
    # and does not need (or support) a named profile. Setting it to "dpla" on
    # EC2 causes all aws CLI calls to fail with "profile not found".
    if [[ -n "${AWS_PROFILE:-}" ]]; then
        export AWS_PROFILE
    else
        unset AWS_PROFILE
    fi
}

# =============================================================================
# Slack Notifications
# =============================================================================
#
# Send a Slack notification to #tech-alerts.
# Reads SLACK_BOT_TOKEN and SLACK_CHANNEL from the environment (.env file).
# Silently skips if SLACK_BOT_TOKEN is not configured.
#
# Usage: slack_notify "message text"
#
slack_notify() {
    # Interpret \n escape sequences so callers can use "\n" for line breaks.
    local msg
    msg=$(printf '%b' "$1")
    local token="${SLACK_BOT_TOKEN:-${SLACK_TOKEN:-}}"
    local channel="${SLACK_CHANNEL:-C02HEU2L3}"
    [[ -z "$token" ]] && return 0
    local payload
    payload=$(python3 -c "import json,sys; print(json.dumps({'channel':sys.argv[1],'text':sys.argv[2]}))" \
        "$channel" "$msg") || return 0
    curl -s -X POST "https://slack.com/api/chat.postMessage" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload" > /dev/null || true
}

# Start a background heartbeat that sends progress updates to Slack.
# Fires once after INTERVAL seconds, then every INTERVAL seconds until stopped.
# Tries to extract a "Fetched X of Y" or "Harvested N" progress line from the log.
#
# Usage: start_heartbeat <provider> <step> <log_file> [interval_seconds]
#   Sets HEARTBEAT_PID for use by stop_heartbeat.
#
HEARTBEAT_PID=""
HEARTBEAT_STEP_START=""

start_heartbeat() {
    local provider="$1"
    local step="$2"
    local log_file="$3"
    local interval="${4:-10800}"  # default 3 hours
    HEARTBEAT_STEP_START=$(date +%s)
    (
        sleep "$interval"
        while true; do
            local progress=""
            if [[ -f "$log_file" ]]; then
                progress=$(grep -oE "Fetched [0-9,]+ of [0-9,]+" "$log_file" 2>/dev/null | tail -1 || true)
                [[ -z "$progress" ]] && \
                    progress=$(grep -oE "Harvested [0-9,]+ records?" "$log_file" 2>/dev/null | tail -1 || true)
            fi
            local elapsed=$(( $(date +%s) - HEARTBEAT_STEP_START ))
            local elapsed_str="$((elapsed/3600))h$((elapsed%3600/60))m elapsed"
            if [[ -n "$progress" ]]; then
                slack_notify ":hourglass: *$provider $step in progress* — $progress | $elapsed_str"
            else
                slack_notify ":hourglass: *$provider $step in progress* — $elapsed_str"
            fi
            sleep "$interval"
        done
    ) &
    HEARTBEAT_PID=$!
}

# Stop the background heartbeat started by start_heartbeat.
stop_heartbeat() {
    if [[ -n "${HEARTBEAT_PID:-}" ]]; then
        kill "$HEARTBEAT_PID" 2>/dev/null || true
        wait "$HEARTBEAT_PID" 2>/dev/null || true
        HEARTBEAT_PID=""
    fi
}

# =============================================================================
# Output and Logging
# =============================================================================

# Color codes (will be empty if NO_COLOR is set or not a terminal)
setup_colors() {
    if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
        RED='\033[0;31m'
        GREEN='\033[0;32m'
        YELLOW='\033[1;33m'
        BLUE='\033[0;34m'
        CYAN='\033[0;36m'
        NC='\033[0m'  # No Color
    else
        RED=''
        GREEN=''
        YELLOW=''
        BLUE=''
        CYAN=''
        NC=''
    fi
    export RED GREEN YELLOW BLUE CYAN NC
}

# Logging functions
log_info() {
    echo -e "${BLUE:-}[INFO]${NC:-} $*"
}

log_warn() {
    echo -e "${YELLOW:-}[WARN]${NC:-} $*" >&2
}

log_error() {
    echo -e "${RED:-}[ERROR]${NC:-} $*" >&2
}

log_success() {
    echo -e "${GREEN:-}[SUCCESS]${NC:-} $*"
}

log_debug() {
    [[ "${DEBUG:-false}" == "true" ]] && echo -e "${CYAN:-}[DEBUG]${NC:-} $*"
}

# Print a step header
print_step() {
    echo -e "${BLUE:-}==>${NC:-} ${GREEN:-}$1${NC:-}"
}

# Print an error and exit
die() {
    log_error "$@"
    exit 1
}

# Abort if free disk space on the data partition is below a threshold.
# Catches the most common cause of silent Java/Spark crashes mid-step.
#
# Usage: check_disk_space <min_gb>
#   Exits with an error if available space is below min_gb.
#   Falls back silently if df output cannot be parsed (non-fatal).
#
check_disk_space() {
    local min_gb="${1:-20}"
    local data_dir="${DPLA_DATA:-/home/ec2-user/data}"
    local available_gb
    # df -BG is GNU/Linux-only (EC2); macOS df lacks the -B flag and exits non-zero.
    # Use || true so the non-zero exit doesn't propagate through the command substitution
    # under set -eo pipefail — the empty result is caught by the [[ -z ]] check below.
    available_gb=$(df -BG "$data_dir" 2>/dev/null | awk 'NR==2 {gsub(/G/,"",$4); print $4}') || true
    if [[ -z "$available_gb" || ! "$available_gb" =~ ^[0-9]+$ ]]; then
        return 0  # Can't parse — skip check rather than fail
    fi
    if (( available_gb < min_gb )); then
        die "Insufficient disk space: ${available_gb}GB free on $data_dir (need ${min_gb}GB). Free up space before continuing."
    fi
    log_info "Disk check: ${available_gb}GB free (need ${min_gb}GB) ✓"
}

# =============================================================================
# Process Management
# =============================================================================

# Recursively kill a process and all its descendants.
# Prevents orphan JVMs when sbt forks a child Java process.
#
# Usage: kill_tree <pid>
#
kill_tree() {
    local pid=$1
    local children
    children=$(pgrep -P "$pid" 2>/dev/null) || true
    for child in $children; do
        kill_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
}

# =============================================================================
# Entry-Point Runner (JAR or sbt)
# =============================================================================
#
# Run any Scala entry class via assembly JAR (preferred) or sbt (fallback).
# Uses JAR automatically when found at the standard path; no flag required.
# Falls back to sbt when no JAR exists.
#
# The JAR path can be overridden with I3_JAR env var.
# Memory is controlled by SBT_OPTS (set via setup_java).
# SPARK_MASTER is taken from the environment (set in init_paths).
#
# Usage: run_entry <fully.qualified.EntryClass> [--arg1=val1 --arg2=val2 ...]
#
# Example:
#   run_entry dpla.ingestion3.entries.ingest.JsonlEntry \
#       --input="$INPUT" --output="$OUTPUT" --name=nara --sparkMaster=local[4]
#
run_entry() {
    local entry_class="$1"
    shift
    local jar="${I3_JAR:-$I3_HOME/target/scala-2.13/ingestion3-assembly-0.0.1.jar}"

    # Ensure JAR exists and is current so "harvest indiana" (and any pipeline run) uses latest code.
    if [[ ! -f "$jar" ]]; then
        log_info "JAR not found; building with sbt assembly..."
        (cd "$I3_HOME" && sbt -java-home "${JAVA_HOME:-}" assembly) || die "sbt assembly failed"
    elif [[ -d "$I3_HOME/src/main/scala" ]] && [[ -n "$(find "$I3_HOME/src/main/scala" -name "*.scala" -newer "$jar" 2>/dev/null | head -1)" ]]; then
        log_info "Scala sources newer than JAR; rebuilding with sbt assembly..."
        (cd "$I3_HOME" && sbt -java-home "${JAVA_HOME:-}" assembly) || die "sbt assembly failed"
    fi

    if [[ -f "$jar" ]]; then
        log_info "Running $entry_class via JAR"
        # SBT_OPTS contains -Xms/-Xmx/-XX flags set by setup_java()
        # --add-opens flags are needed for Java 9+ (strong encapsulation);
        # Java 8 does not recognize them and will fail.
        local add_opens=()
        local java_major
        java_major=$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+)[".].*/\1/')
        if [[ "$java_major" -ge 9 ]] 2>/dev/null; then
            add_opens=(
                --add-opens=java.base/java.nio=ALL-UNNAMED
                --add-opens=java.base/java.lang=ALL-UNNAMED
                --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
                --add-opens=java.base/java.util=ALL-UNNAMED
                --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
                --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
            )
        fi
        # shellcheck disable=SC2086
        java $SBT_OPTS "${add_opens[@]}" \
            -cp "$jar" "$entry_class" "$@"
    else
        log_info "Running $entry_class via sbt (JAR still missing after build)"
        local sbt_args="$*"
        (cd "$I3_HOME" && sbt -java-home "${JAVA_HOME:-}" "runMain $entry_class $sbt_args")
    fi
}

# Convenience wrapper: run IngestRemap (mapping → enrichment → jsonl)
#
# Usage: run_ingest_remap <input> <output> <conf> <provider_name>
#
run_ingest_remap() {
    local input="$1" output="$2" conf="$3" name="$4"
    run_entry dpla.ingestion3.entries.ingest.IngestRemap \
        --input="$input" --output="$output" --conf="$conf" --name="$name" \
        --sparkMaster="$SPARK_MASTER"
}

# Write per-hub status file so ingest-status.sh can show progress for manual runs
# Usage: write_hub_status <hub> <status> [--error=msg] [--records=N]
write_hub_status() {
    local hub="$1" status="$2"
    shift 2
    local py="${I3_HOME}/venv/bin/python"
    [[ -x "$py" ]] || py="python3"
    "$py" -m scheduler.orchestrator.write_status "$hub" "$status" \
        --status-dir="$I3_HOME/logs/status" "$@"
}

# =============================================================================
# Manifest Record Count Reader
# =============================================================================
#
# Extract the numeric record count from a _MANIFEST file.
# Accepts a file path, or reads from stdin when passed "-" or /dev/stdin.
# Always returns a non-negative integer; returns 0 on any parse failure.
#
# Usage: read_manifest_count <path>
# Example: read_manifest_count /path/to/jsonl/_MANIFEST
#          aws s3 cp s3://bucket/path/_MANIFEST - | read_manifest_count /dev/stdin
#
read_manifest_count() {
    local raw
    raw=$(grep "^Record count:" "${1:--}" 2>/dev/null | awk '{print $NF}' | tr -d ',')
    [[ "${raw}" =~ ^[0-9]+$ ]] && echo "$raw" || echo "0"
}

# Latest Data Directory Finder
# =============================================================================
#
# Find the most recent timestamped directory for a provider's pipeline step.
# Directories follow the naming convention: YYYYMMDD_HHMMSS-provider-*
#
# Usage: find_latest_data <provider> <step>
# Example: find_latest_data nara enrichment
#          → /path/to/data/nara/enrichment/20260210_041223-nara-MAP4_0.EnrichRecord.avro
#
find_latest_data() {
    local provider="$1"
    local step="$2"
    local data_dir="$DPLA_DATA/$provider/$step"

    if [[ ! -d "$data_dir" ]]; then
        return 1
    fi

    # Walk candidates newest-first, returning the first one with a _MANIFEST.
    # This skips dirs left by failed runs (which produce no manifest).
    while IFS= read -r candidate; do
        candidate="${candidate%/}"
        if [[ -f "$candidate/_MANIFEST" ]]; then
            echo "$candidate"
            return 0
        fi
    done < <(ls -1d "$data_dir"/*/ 2>/dev/null | sort -r)

    return 1
}

# =============================================================================
# Validation Helpers
# =============================================================================

# Check that a command exists
require_command() {
    local cmd="$1"
    local msg="${2:-Required command '$cmd' not found}"

    if ! command -v "$cmd" >/dev/null 2>&1; then
        die "$msg"
    fi
}

# Check that a file exists
require_file() {
    local file="$1"
    local msg="${2:-Required file not found: $file}"

    if [[ ! -f "$file" ]]; then
        die "$msg"
    fi
}

# Check that a directory exists
require_dir() {
    local dir="$1"
    local msg="${2:-Required directory not found: $dir}"

    if [[ ! -d "$dir" ]]; then
        die "$msg"
    fi
}

# =============================================================================
# Hub/Provider Helpers
# =============================================================================

# Resolve local hub key to S3 prefix with alias compatibility.
# Set I3_STRICT_HUB_NAMES=1 to disable aliases.
resolve_s3_prefix() {
    local hub="$1"
    if [[ "${I3_STRICT_HUB_NAMES:-}" == "1" ]]; then
        echo "$hub"
        return
    fi

    case "$hub" in
        hathi) echo "hathitrust" ;;
        tn) echo "tennessee" ;;
        *) echo "$hub" ;;
    esac
}

# Get provider name from i3.conf
get_provider_name() {
    local hub="$1"
    grep "^${hub}\.provider" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo "$hub"
}

# Get hub email from i3.conf
get_hub_email() {
    local hub="$1"
    grep "^${hub}\.email" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo ""
}

# Get harvest type from i3.conf
get_harvest_type() {
    local hub="$1"
    grep "^${hub}\.harvest\.type" "$I3_CONF" 2>/dev/null | sed 's/.*= *"//' | sed 's/".*//' || echo "oai"
}

# =============================================================================
# Initialization
# =============================================================================

# Auto-initialize when sourced (can be disabled with COMMON_NO_INIT=1)
if [[ -z "${COMMON_NO_INIT:-}" ]]; then
    setup_colors
    init_paths
fi
