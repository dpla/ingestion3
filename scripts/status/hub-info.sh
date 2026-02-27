#!/usr/bin/env bash
# Show key i3.conf config for a hub
# Usage: hub-info.sh <hub>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

hub="${1:?Usage: hub-info.sh <hub>}"
conf="$I3_CONF"

provider="$(get_provider_name "$hub")"
harvest_type="$(get_harvest_type "$hub")"
email="$(get_hub_email "$hub")"

endpoint="$(grep -E "^${hub}\.harvest\.endpoint" "$conf" 2>/dev/null | sed -E "s/.*= *\"(.*)\".*/\1/" || true)"
sched_months="$(grep -E "^${hub}\.schedule\.months" "$conf" 2>/dev/null | sed -E "s/.*= *//" || true)"
sched_freq="$(grep -E "^${hub}\.schedule\.frequency" "$conf" 2>/dev/null | sed -E "s/.*= *\"(.*)\".*/\1/" || true)"
sched_status="$(grep -E "^${hub}\.schedule\.status" "$conf" 2>/dev/null | sed -E "s/.*= *\"(.*)\".*/\1/" || true)"
setlist="$(grep -E "^${hub}\.harvest\.setlist" "$conf" 2>/dev/null | sed -E "s/.*= *\"(.*)\".*/\1/" || true)"

echo "hub:               $hub"
echo "provider:          ${provider:-$hub}"
echo "harvest.type:      ${harvest_type:-oai}"
echo "harvest.endpoint:  ${endpoint:-(not set)}"
echo "email:             ${email:-(not set)}"
echo "schedule.months:   ${sched_months:-(not set)}"
echo "schedule.frequency:${sched_freq:-(not set)}"
echo "schedule.status:   ${sched_status:-(not set)}"

if [[ -n "${setlist:-}" ]]; then
  set_count=$(printf "%s" "$setlist" | awk -F, '{print NF}')
  echo "harvest.setlist:   ${set_count} sets"
else
  echo "harvest.setlist:   (none)"
fi
