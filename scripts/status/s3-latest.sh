#!/usr/bin/env bash
# Show the most recent S3 data for a hub across harvest, mapping, and jsonl stages
# Usage: s3-latest.sh <hub>
# Uses AWS_PROFILE when set; otherwise ambient credentials (e.g. the EC2 instance role).

set -euo pipefail

hub="${1:?Usage: s3-latest.sh <hub>}"

for stage in harvest mapping jsonl; do
  latest=$(aws s3 ls "s3://dpla-master-dataset/${hub}/${stage}/" ${AWS_PROFILE:+--profile "$AWS_PROFILE"} 2>/dev/null \
    | awk '{print $NF}' \
    | grep -E '^[0-9]{8}_[0-9]{6}-' \
    | sort \
    | tail -1 || true)
  printf "%-12s %s\n" "${stage}:" "${latest:-(none)}"
done
