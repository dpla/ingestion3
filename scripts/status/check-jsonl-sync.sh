#!/usr/bin/env bash
#
# check-jsonl-sync.sh
# Compares local JSONL data with S3 to identify missing syncs
#
# Usage: ./check-jsonl-sync.sh [--data-dir DIR] [--profile PROFILE]
#
# Options:
#   --data-dir DIR     Local data directory (default: ~/dpla/data)
#   --profile PROFILE  AWS profile to use (default: dpla)
#

# Don't use set -e as aws commands may return non-zero for missing paths

# Source common configuration (common.sh is in scripts/ root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPTS_ROOT/common.sh"

# Default values (use DPLA_DATA from common.sh)
DATA_DIR="${DPLA_DATA:-$HOME/dpla/data}"
S3_BUCKET="dpla-master-dataset"

# Function to get S3 hub name (handles local -> S3 name mappings)
get_s3_hub_name() {
  local hub=$1
  case $hub in
    tn) echo "tennessee" ;;
    *) echo "$hub" ;;
  esac
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    --profile)
      AWS_PROFILE="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--data-dir DIR] [--profile PROFILE]"
      echo ""
      echo "Options:"
      echo "  --data-dir DIR     Local data directory (default: ~/dpla/data)"
      echo "  --profile PROFILE  AWS profile to use (default: dpla)"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "JSONL Sync Status Check"
echo "========================================"
echo "Data directory: $DATA_DIR"
echo "AWS profile: $AWS_PROFILE"
echo "S3 bucket: $S3_BUCKET"
echo "========================================"
echo ""

missing_count=0
synced_count=0
missing_list=""

# Get list of hubs with jsonl directories
for hub_dir in "$DATA_DIR"/*/; do
  hub=$(basename "$hub_dir")
  jsonl_dir="$hub_dir/jsonl"

  # Skip if no jsonl directory
  if [[ ! -d "$jsonl_dir" ]]; then
    continue
  fi

  # Get S3 hub name (use mapping if exists, otherwise use local name)
  s3_hub=$(get_s3_hub_name "$hub")

  echo -e "${YELLOW}--- $hub (S3: $s3_hub) ---${NC}"

  # Check each jsonl directory
  for local_dir in "$jsonl_dir"/*/; do
    if [[ ! -d "$local_dir" ]]; then
      continue
    fi

    dir_name=$(basename "$local_dir")
    s3_path="s3://$S3_BUCKET/$s3_hub/jsonl/$dir_name/"

    if aws s3 ls "$s3_path" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
      echo -e "  ${GREEN}✓${NC} $dir_name"
      synced_count=$((synced_count + 1))
    else
      echo -e "  ${RED}✗ MISSING:${NC} $dir_name"
      missing_count=$((missing_count + 1))
      missing_list+="  $hub: $dir_name\n"
    fi
  done
done

echo ""
echo "========================================"
echo "Summary"
echo "========================================"
echo -e "Synced: ${GREEN}$synced_count${NC}"
echo -e "Missing: ${RED}$missing_count${NC}"

if [[ $missing_count -gt 0 ]]; then
  echo ""
  echo -e "${RED}Missing syncs:${NC}"
  echo -e "$missing_list"
  echo ""
  echo "To sync missing data, run:"
  echo "  aws s3 sync <local_path> <s3_path> --profile $AWS_PROFILE"
  exit 1
else
  echo ""
  echo -e "${GREEN}All JSONL data is synced!${NC}"
  exit 0
fi
