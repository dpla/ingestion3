#!/usr/bin/env bash
#
# fix-si.sh - Preprocess Smithsonian data files
#
# Smithsonian data requires preprocessing before harvest:
#   1. Recompress gzip files (SI exports have gzip compatibility issues)
#   2. Run xmll to shred large XML files into line-delimited format
#
# Usage:
#   ./scripts/fix-si.sh <folder-name>
#   ./scripts/fix-si.sh 20260201
#   ./scripts/fix-si.sh --list     # List available folders
#
# The folder should be a subdirectory of $DPLA_DATA/smithsonian/originalRecords/
#

set -euo pipefail

# Source common configuration (common.sh is in scripts/ root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# Configuration
DATA_PATH="${DPLA_DATA}/smithsonian/originalRecords"
XMLL_JAR="${XMLL_JAR:-$HOME/dpla/code/xmll/target/scala-2.13/xmll-assembly-0.1.jar}"
PARALLEL_JOBS="${PARALLEL_JOBS:-8}"

usage() {
    echo "Usage: $0 <folder-name>"
    echo "       $0 --list"
    echo ""
    echo "Preprocess Smithsonian data files for harvest."
    echo ""
    echo "Arguments:"
    echo "  <folder-name>  Name of folder in $DATA_PATH"
    echo "  --list         List available folders"
    echo ""
    echo "Environment variables:"
    echo "  DPLA_DATA      Data directory (default: ~/dpla/data)"
    echo "  XMLL_JAR       Path to xmll jar (default: ~/dpla/code/xmll/target/scala-2.13/xmll-assembly-0.1.jar)"
    echo "  PARALLEL_JOBS  Number of parallel jobs (default: 8)"
    echo ""
    echo "Example:"
    echo "  $0 20260201"
    exit 1
}

list_folders() {
    echo "Available folders in $DATA_PATH:"
    echo ""
    if [[ -d "$DATA_PATH" ]]; then
        ls -1 "$DATA_PATH" 2>/dev/null | while read -r dir; do
            if [[ -d "$DATA_PATH/$dir" ]]; then
                local count
                count=$(find "$DATA_PATH/$dir" -maxdepth 1 -name "*.xml.gz" 2>/dev/null | wc -l | tr -d ' ')
                echo "  $dir ($count .xml.gz files)"
            fi
        done
    else
        log_error "Data path does not exist: $DATA_PATH"
        exit 1
    fi
}

# Parse arguments
if [[ $# -lt 1 ]]; then
    usage
fi

case "$1" in
    --list|-l)
        list_folders
        exit 0
        ;;
    --help|-h)
        usage
        ;;
    -*)
        log_error "Unknown option: $1"
        usage
        ;;
esac

FOLDER="$1"
FOLDER_PATH="$DATA_PATH/$FOLDER"

# Validation
if [[ ! -d "$FOLDER_PATH" ]]; then
    log_error "Folder not found: $FOLDER_PATH"
    echo ""
    list_folders
    exit 1
fi

if [[ ! -f "$XMLL_JAR" ]]; then
    log_error "xmll jar not found: $XMLL_JAR"
    log_info "Clone https://github.com/dpla/xmll and run 'sbt assembly'"
    exit 1
fi

# Count input files
INPUT_COUNT=$(find "$FOLDER_PATH" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l | tr -d ' ')

if [[ "$INPUT_COUNT" -eq 0 ]]; then
    log_error "No .xml.gz files found in $FOLDER_PATH"
    exit 1
fi

echo ""
echo "=============================================="
echo "  Smithsonian Data Preprocessing"
echo "=============================================="
echo "Folder:       $FOLDER"
echo "Path:         $FOLDER_PATH"
echo "Input files:  $INPUT_COUNT .xml.gz files"
echo "XMLL jar:     $XMLL_JAR"
echo "Parallel:     $PARALLEL_JOBS jobs"
echo "=============================================="
echo ""

# Create working directories
FIXED_DIR="$FOLDER_PATH/fixed"
XMLL_DIR="$FOLDER_PATH/xmll"
mkdir -p "$FIXED_DIR" "$XMLL_DIR"

# Step 1: Recompress gzip files
print_step "Step 1/3: Recompressing gzip files..."

recompress_count=0
for f in "$FOLDER_PATH"/*.xml.gz; do
    if [[ -f "$f" ]]; then
        basename=$(basename "$f")
        if [[ ! -f "$FIXED_DIR/$basename" ]]; then
            log_info "Recompressing: $basename"
            if gunzip -dck "$f" | gzip > "$FIXED_DIR/$basename" 2>/dev/null; then
                recompress_count=$((recompress_count + 1))
            else
                log_warn "Failed to recompress $basename, copying original"
                cp "$f" "$FIXED_DIR/$basename"
                recompress_count=$((recompress_count + 1))
            fi
        fi
    fi
done
log_success "Recompressed $recompress_count files"

# Step 2: Run xmll
print_step "Step 2/3: Running xmll shredder..."

xmll_count=0
for f in "$FIXED_DIR"/*.xml.gz; do
    if [[ -f "$f" ]]; then
        basename=$(basename "$f")
        if [[ ! -f "$XMLL_DIR/$basename" ]]; then
            log_info "Processing: $basename"
            if java -Xmx4g -jar "$XMLL_JAR" doc "$f" "$XMLL_DIR/$basename" 2>&1; then
                xmll_count=$((xmll_count + 1))
            else
                log_warn "xmll failed for $basename"
            fi
        fi
    fi
done
log_success "Processed $xmll_count files with xmll"

# Step 3: Clean up and move files
print_step "Step 3/3: Cleaning up..."

# Remove fixed directory (intermediate)
rm -rf "$FIXED_DIR"

# Move xmll output to replace original files
if [[ -d "$XMLL_DIR" ]] && [[ -n "$(ls -A "$XMLL_DIR" 2>/dev/null)" ]]; then
    # Backup original files by moving them to a backup dir
    BACKUP_DIR="$FOLDER_PATH/original_backup"
    mkdir -p "$BACKUP_DIR"
    mv "$FOLDER_PATH"/*.xml.gz "$BACKUP_DIR/" 2>/dev/null || true

    # Move processed files
    mv "$XMLL_DIR"/*.xml.gz "$FOLDER_PATH/"
    rm -rf "$XMLL_DIR"

    log_success "Processed files moved to $FOLDER_PATH"
    log_info "Original files backed up to $BACKUP_DIR"
else
    log_warn "No xmll output files found"
fi

OUTPUT_COUNT=$(find "$FOLDER_PATH" -maxdepth 1 -name "*.xml.gz" -type f 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "=============================================="
echo "  Preprocessing Complete"
echo "=============================================="
echo "Output files: $OUTPUT_COUNT .xml.gz files"
echo ""
echo "Next steps:"
echo "  1. Update i3.conf: smithsonian.harvest.endpoint = \"$FOLDER_PATH/\""
echo "  2. Run harvest: ./scripts/harvest.sh smithsonian"
echo ""
