#!/usr/bin/env bash
# Show the most recent S3 data for a hub across harvest, mapping, and jsonl stages
# Usage: s3-latest.sh <hub>

set -euo pipefail

hub="${1:?Usage: s3-latest.sh <hub>}"

for stage in harvest mapping jsonl; do
  latest=$(aws s3 ls "s3://dpla-master-dataset/${hub}/${stage}/" --profile dpla 2>/dev/null \
    | awk '{print $NF}' \
    | grep -E '^[0-9]{8}_[0-9]{6}-' \
    | sort \
    | tail -1 || true)
  printf "%-12s %s\n" "${stage}:" "${latest:-(none)}"
done
