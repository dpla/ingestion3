#!/usr/bin/env bash
# Acquire National Gallery of Art Open Data for NgaFileHarvester.
#
# NGA publishes its full collection dataset as CC0 CSV files in a single GitHub
# repository (https://github.com/NationalGalleryOfArt/opendata), refreshed daily.
# Modeled on the Digital Virginias GitHub-sourced flow (harvest-va.sh): this
# script shallow-clones (or updates) the repo and points the harvest endpoint at
# its data/ directory, where NgaFileHarvester reads the CSVs.
#
# Usage: ./scripts/harvest/harvest-nga.sh [--output=DIR] [--fresh]
#   --output=DIR  Where to clone the repo (default: $DPLA_DATA/nga/opendata,
#                 or ~/nga-opendata if DPLA_DATA is unset)
#   --fresh       Remove any existing clone and clone from scratch
#
# Requirements: git. (The repo is public; no authentication needed.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=/dev/null
source "$SCRIPTS_ROOT/common.sh"

REPO_URL="https://github.com/NationalGalleryOfArt/opendata.git"

usage() {
  cat <<'EOF'
Acquire National Gallery of Art Open Data for NgaFileHarvester.

Shallow-clones (or updates) github.com/NationalGalleryOfArt/opendata and points
the harvest endpoint at its data/ directory, where NgaFileHarvester reads the
CSVs. Modeled on the Digital Virginias flow (harvest-va.sh).

Usage: ./scripts/harvest/harvest-nga.sh [--output=DIR] [--fresh]
  --output=DIR  Where to clone the repo (default: $DPLA_DATA/nga/opendata,
                or ~/nga-opendata if DPLA_DATA is unset)
  --fresh       Remove any existing clone and clone from scratch

Requirements: git. (The repo is public; no authentication needed.)
EOF
}

# CSV files NgaFileHarvester joins into each object's record. The clone contains
# more tables than these; the harvester ignores the rest.
REQUIRED_CSVS=(
  objects.csv
  published_images.csv
  objects_constituents.csv
  constituents.csv
  objects_terms.csv
  objects_text_entries.csv
)

# --- Parse args -------------------------------------------------------------
if [[ -n "${DPLA_DATA:-}" ]]; then
  OUTPUT_DIR="${DPLA_DATA%/}/nga/opendata"
else
  OUTPUT_DIR="$HOME/nga-opendata"
fi
FRESH=0

for arg in "$@"; do
  case "$arg" in
    --output=*) OUTPUT_DIR="${arg#*=}" ;;
    --fresh)    FRESH=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $arg (see --help)" ;;
  esac
done

DATA_DIR="$OUTPUT_DIR/data"

require_command git "git is required to clone the NGA opendata repository"

log_info "NGA Open Data acquisition"
log_info "Clone target: $OUTPUT_DIR"

# --- Clone or update --------------------------------------------------------
if [[ "$FRESH" -eq 1 && -d "$OUTPUT_DIR" ]]; then
  log_info "--fresh: removing existing clone at $OUTPUT_DIR"
  rm -rf "$OUTPUT_DIR"
fi

if [[ -d "$OUTPUT_DIR/.git" ]]; then
  log_info "Existing clone found; updating (git pull --ff-only)"
  if ! git -C "$OUTPUT_DIR" pull --ff-only --depth 1 2>/dev/null; then
    log_warn "Shallow pull failed; re-cloning from scratch"
    rm -rf "$OUTPUT_DIR"
    git clone --depth 1 "$REPO_URL" "$OUTPUT_DIR"
  fi
else
  [[ -e "$OUTPUT_DIR" ]] && die "Output path exists but is not a git clone: $OUTPUT_DIR (use --fresh to replace)"
  mkdir -p "$(dirname "$OUTPUT_DIR")"
  log_info "Cloning $REPO_URL (shallow)"
  git clone --depth 1 "$REPO_URL" "$OUTPUT_DIR"
fi

# --- Verify the harvester's required CSVs are present and non-empty ---------
require_dir "$DATA_DIR" "NGA data directory not found after clone: $DATA_DIR"

missing=()
for csv in "${REQUIRED_CSVS[@]}"; do
  if [[ ! -s "$DATA_DIR/$csv" ]]; then
    missing+=("$csv")
  fi
done
if [[ "${#missing[@]}" -gt 0 ]]; then
  die "NGA clone is missing required CSV file(s): ${missing[*]}"
fi

log_success "NGA data ready in $DATA_DIR"
log_info "Files:"
for csv in "${REQUIRED_CSVS[@]}"; do
  # Portable human-readable size (BSD/macOS ls has no --si; awk keeps it POSIX).
  size=$(wc -c < "$DATA_DIR/$csv" | awk '{printf "%.1f MB", $1/1048576}')
  log_info "  $csv ($size)"
done

cat <<EOF

Set the harvest endpoint in i3.conf (ingestion3-conf) to the data/ directory:

  nga.status = test
  nga.provider = "National Gallery of Art"
  nga.harvest.type = "file"
  nga.harvest.endpoint = "$DATA_DIR"

Then harvest with:  ./scripts/harvest.sh nga
EOF
