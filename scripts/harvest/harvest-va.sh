#!/usr/bin/env bash
# Clone dplava GitHub repositories and package them as ZIP files for VaFileHarvester
#
# The dplava GitHub organization (https://github.com/dplava) contains QDC metadata
# repositories for Digital Virginias institutions.
#
# Usage: ./scripts/harvest-va.sh [output_directory]
#   output_directory: Where to clone repos and create ZIPs (default: ~/dplava-harvest)
#
# Requirements: gh (GitHub CLI) must be installed and authenticated

set -e

# Source common configuration (common.sh is in scripts/ root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# Configuration
OUTPUT_DIR="${1:-$HOME/dplava-harvest}"
INPUT_DIR="$OUTPUT_DIR/input"
GITHUB_ORG="dplava"

# Repositories to exclude (not metadata)
EXCLUDE_REPOS="transformation-scripts"

# Check for gh CLI
if ! command -v gh &> /dev/null; then
  echo "Error: GitHub CLI (gh) is required but not installed."
  echo "Install it with: brew install gh"
  exit 1
fi

# Fetch repository list dynamically from GitHub
echo "Fetching repository list from github.com/$GITHUB_ORG..."
REPOS=($(gh repo list "$GITHUB_ORG" --limit 100 --json name -q '.[].name' | grep -v -E "^($EXCLUDE_REPOS)$"))

echo "=== Digital Virginias Harvest Preparation ==="
echo "Output directory: $OUTPUT_DIR"
echo "Found ${#REPOS[@]} repositories: ${REPOS[*]}"
echo ""

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$INPUT_DIR"

cd "$OUTPUT_DIR"

# Clone repositories
echo "=== Cloning repositories ==="
for repo in "${REPOS[@]}"; do
  if [ -d "$repo" ]; then
    echo "Updating $repo..."
    cd "$repo"
    git pull --ff-only
    cd ..
  else
    echo "Cloning $repo..."
    git clone --depth 1 "https://github.com/$GITHUB_ORG/${repo}.git"
  fi
done

echo ""
echo "=== Creating ZIP files ==="

# Create ZIP files (excluding .git directories)
for repo in "${REPOS[@]}"; do
  if [ -d "$repo" ]; then
    echo "Zipping $repo..."
    zip -rq "$INPUT_DIR/${repo}.zip" "$repo" -x "*.git*"
  else
    echo "Warning: $repo directory not found, skipping..."
  fi
done

echo ""
echo "=== Summary ==="
echo "ZIP files created in: $INPUT_DIR"
ls -lh "$INPUT_DIR"

echo ""
echo "=== i3.conf configuration ==="
echo "Ensure your i3.conf has:"
echo ""
echo "  virginias.harvest.endpoint = \"$INPUT_DIR\""
echo ""
echo "Done!"
