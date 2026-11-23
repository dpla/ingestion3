#!/usr/bin/env bash
# prune-hub-data.sh - Free disk on the ingest box by deleting a hub's old runs
#
# Usage: ./scripts/prune-hub-data.sh <hub> [--dry-run] [--keep N]
#
# For each of $DPLA_DATA/<hub>/{harvest,mapping,enrichment,jsonl}:
#   - keeps the newest N complete runs (dirs with _SUCCESS; default 2)
#   - deletes incomplete runs (no _SUCCESS) outright
#   - deletes older complete runs once they're in S3 (_SUCCESS in
#     s3://dpla-master-dataset/<prefix>/<stage>/<run>/)
#   - harvest/mapping/enrichment runs that aren't in S3 are synced first, then
#     deleted; if the sync fails the run is kept
#   - jsonl runs that aren't in S3 are kept and never uploaded here: the
#     indexer reads the newest JSONL snapshot in S3, and a snapshot that isn't
#     there was held back on purpose (safety check, --skip-s3-sync)
#
# --dry-run prints what would happen (WOULD DELETE / WOULD SYNC + DELETE) and
# changes nothing. NARA has its own prune (launch_nara.py step 1b).
#
# Called by ingest.sh --prune, which 3_launch_ingest.py adds after showing you
# the dry run and asking.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

STAGES="harvest mapping enrichment jsonl"
RUN_RE='^[0-9]{8}_[0-9]{6}-'

usage() {
    echo "Usage: $0 <hub> [--dry-run] [--keep N]"
    echo ""
    echo "Deletes a hub's old runs from \$DPLA_DATA/<hub>/{harvest,mapping,enrichment,jsonl}."
    echo "Keeps the newest N complete runs per step (default 2). Older runs are deleted once"
    echo "they're in S3; harvest/mapping/enrichment runs not in S3 are synced first."
    echo ""
    echo "  --dry-run   Show what would be deleted; change nothing"
    echo "  --keep N    Complete runs to keep per step (default 2, minimum 1)"
    exit 1
}

[[ $# -ge 1 && "$1" != "--help" && "$1" != "-h" ]] || usage
HUB="$1"
shift
DRY_RUN=false
KEEP=2
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --keep)
            KEEP="${2:-}"
            [[ "$KEEP" =~ ^[1-9][0-9]*$ ]] || die "--keep needs a whole number of 1 or more"
            shift 2
            ;;
        *) log_error "Unknown option: $1"; usage ;;
    esac
done

[[ "$HUB" =~ ^[a-z0-9_-]+$ ]] || die "Invalid hub name: $HUB"
[[ "$HUB" != "nara" ]] || die "NARA has its own prune (launch_nara.py step 1b); not pruning it here."

HUB_DIR="$DPLA_DATA/$HUB"
if [[ ! -d "$HUB_DIR" ]]; then
    echo "Nothing to prune: $HUB_DIR doesn't exist"
    exit 0
fi

# Don't touch a hub that has a pipeline step running (Spark entry points all
# take --name=<hub>). $PPID is ingest.sh when it calls us with --prune.
busy=$(pgrep -u "$(id -u)" -f -- "--name=${HUB}( |\$)|^bash [^ ]*scripts/(ingest|harvest|remap|mapping|enrich|jsonl)\.sh ${HUB}( |\$)" \
    | grep -v -x -e "$PPID" -e "$$" || true)
if [[ -n "$busy" ]]; then
    echo "SKIP: a $HUB pipeline step is running (pid ${busy//$'\n'/, })"
    exit 0
fi

delete_dir() {  # $1 = dir, $2 = why
    if [[ "$DRY_RUN" == true ]]; then
        echo "WOULD DELETE: $1 ($2)"
    else
        rm -rf "$1" && echo "DELETED: $1 ($2)"
    fi
}

failed=0
for stage in $STAGES; do
    stage_dir="$HUB_DIR/$stage"
    [[ -d "$stage_dir" ]] || continue

    runs=$(ls -1 "$stage_dir" | grep -E "$RUN_RE" | sort || true)
    [[ -n "$runs" ]] || continue

    keep=$(for r in $runs; do if [[ -f "$stage_dir/$r/_SUCCESS" ]]; then echo "$r"; fi; done | tail -n "$KEEP")
    if [[ -z "$keep" ]]; then
        echo "KEEP ALL (no complete run found): $stage_dir"
        continue
    fi
    for r in $keep; do echo "KEEP (newest $KEEP): $stage_dir/$r"; done

    in_s3=""
    s3_ok=true
    if ! in_s3=$(list_s3_complete_runs "$HUB" "$stage"); then
        s3_ok=false
    fi

    for r in $runs; do
        d="$stage_dir/$r"
        [[ -d "$d" ]] || continue
        if printf '%s\n' "$keep" | grep -qxF "$r"; then continue; fi

        if [[ ! -f "$d/_SUCCESS" ]]; then
            delete_dir "$d" "incomplete run"
        elif [[ "$s3_ok" != true ]]; then
            echo "KEEP (couldn't list S3): $d"
            failed=1
        elif printf '%s\n' "$in_s3" | grep -qxF "$r"; then
            delete_dir "$d" "in S3"
        elif [[ "$stage" == "jsonl" ]]; then
            echo "KEEP (not in S3; JSONL is never uploaded by prune): $d"
        elif [[ "$DRY_RUN" == true ]]; then
            echo "WOULD SYNC + DELETE: $d (not in S3 yet)"
        elif sync_run_to_s3 "$HUB" "$stage" "$d"; then
            rm -rf "$d" && echo "SYNCED + DELETED: $d"
        else
            echo "KEEP (S3 sync failed): $d"
            failed=1
        fi
    done
done

df -h "$DPLA_DATA" | tail -1
exit "$failed"
