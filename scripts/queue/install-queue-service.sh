#!/usr/bin/env bash
#
# install-queue-service.sh - Install and start the ingest queue's systemd unit
# on the ingest EC2 (issue #786). Run once, as root (e.g. through SSM without
# the sudo -u ec2-user wrapper). Safe to re-run after updating the unit file.
#
# Usage: install-queue-service.sh [--status]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_NAME="dpla-ingest-queue"
UNIT_SRC="$SCRIPT_DIR/$UNIT_NAME.service"
UNIT_DST="/etc/systemd/system/$UNIT_NAME.service"

if [ "${1:-}" = "--status" ]; then
    systemctl status "$UNIT_NAME" --no-pager || true
    exit 0
fi
case "${1:-}" in -h|--help) sed -n '3,7p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac

[ "$(id -u)" -eq 0 ] || { echo "Run as root (sudo)." >&2; exit 1; }
command -v systemctl >/dev/null 2>&1 || { echo "systemctl not found" >&2; exit 1; }
[ -f "$UNIT_SRC" ] || { echo "Unit file not found: $UNIT_SRC" >&2; exit 1; }

install -m 0644 "$UNIT_SRC" "$UNIT_DST"
systemctl daemon-reload
systemctl enable "$UNIT_NAME"
systemctl restart "$UNIT_NAME"
systemctl is-active --quiet "$UNIT_NAME" && echo "INSTALLED: $UNIT_NAME is running" \
    || { echo "INSTALLED, but $UNIT_NAME is not running:" >&2; systemctl status "$UNIT_NAME" --no-pager >&2 || true; exit 1; }
