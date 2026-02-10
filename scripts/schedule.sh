#!/usr/bin/env bash

# schedule.sh - Query DPLA hub ingest schedules from i3.conf
#
# Usage:
#   ./scripts/schedule.sh              # Show full year schedule
#   ./scripts/schedule.sh feb          # Show hubs scheduled for February
#   ./scripts/schedule.sh february     # Show hubs scheduled for February
#   ./scripts/schedule.sh 2            # Show hubs scheduled for month 2
#   ./scripts/schedule.sh virginias    # Show schedule for a specific hub

# Source common configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Verify I3_CONF exists
if [[ ! -f "$I3_CONF" ]]; then
    die "Could not find i3.conf at $I3_CONF"
fi

# Convert month name/number to month number
get_month_number() {
    local arg="$1"
    local lower_arg
    lower_arg=$(echo "$arg" | tr '[:upper:]' '[:lower:]')

    case "$lower_arg" in
        1|jan|january) echo 1 ;;
        2|feb|february) echo 2 ;;
        3|mar|march) echo 3 ;;
        4|apr|april) echo 4 ;;
        5|may) echo 5 ;;
        6|jun|june) echo 6 ;;
        7|jul|july) echo 7 ;;
        8|aug|august) echo 8 ;;
        9|sep|sept|september) echo 9 ;;
        10|oct|october) echo 10 ;;
        11|nov|november) echo 11 ;;
        12|dec|december) echo 12 ;;
        *) echo "" ;;
    esac
}

# Get month name from number
get_month_name() {
    case "$1" in
        1) echo "January" ;;
        2) echo "February" ;;
        3) echo "March" ;;
        4) echo "April" ;;
        5) echo "May" ;;
        6) echo "June" ;;
        7) echo "July" ;;
        8) echo "August" ;;
        9) echo "September" ;;
        10) echo "October" ;;
        11) echo "November" ;;
        12) echo "December" ;;
    esac
}

# Get all hubs with schedule properties (not commented out)
get_scheduled_hubs() {
    grep '\.schedule\.frequency' "$I3_CONF" | \
        grep -v '^#' | \
        grep -v 'as-needed' | \
        sed 's/\.schedule\.frequency.*//' | \
        sort -u
}

# Get hub provider name
get_provider_name() {
    local hub="$1"
    grep "^${hub}\.provider" "$I3_CONF" | \
        head -1 | \
        sed 's/.*= *"//' | sed 's/".*//'
}

# Get hub frequency
get_frequency() {
    local hub="$1"
    grep "^${hub}\.schedule\.frequency" "$I3_CONF" | \
        grep -v '^#' | \
        head -1 | \
        sed 's/.*= *"//' | sed 's/".*//'
}

# Get hub months array as space-separated list
get_months() {
    local hub="$1"
    grep "^${hub}\.schedule\.months" "$I3_CONF" | \
        grep -v '^#' | \
        head -1 | \
        sed 's/.*= *\[//' | sed 's/\].*//' | \
        sed 's/,/ /g' | sed 's/  */ /g' | sed 's/^ *//' | sed 's/ *$//'
}

# Get on-hold hubs
get_onhold_hubs() {
    grep '^# *[a-z].*\.schedule\.status.*on-hold' "$I3_CONF" | \
        sed 's/^# *//' | sed 's/\.schedule\.status.*//' | \
        sort -u
}

# Get as-needed hubs
get_asneeded_hubs() {
    grep '\.schedule\.frequency.*as-needed' "$I3_CONF" | \
        grep -v '^#' | \
        sed 's/\.schedule\.frequency.*//' | \
        sort -u
}

# Check if hub is scheduled for a month
is_scheduled_for_month() {
    local hub="$1"
    local target_month="$2"
    local months
    months=$(get_months "$hub")

    # Check each month in the list
    for m in $months; do
        if [[ "$m" == "$target_month" ]]; then
            return 0
        fi
    done
    return 1
}

# Show hubs for a specific month
show_month() {
    local month_num="$1"
    local month_name
    month_name=$(get_month_name "$month_num")
    local count=0
    local output=""

    while IFS= read -r hub; do
        [[ -z "$hub" ]] && continue
        if is_scheduled_for_month "$hub" "$month_num"; then
            local provider freq
            provider=$(get_provider_name "$hub")
            freq=$(get_frequency "$hub")
            output="${output}$(printf "  %-18s %-35s %s\n" "$hub" "$provider" "$freq")"$'\n'
            ((count++))
        fi
    done < <(get_scheduled_hubs)

    echo "$month_name Ingests ($count hubs)"
    echo "$(printf '%0.s-' {1..40})"
    echo -n "$output"
}

# Show schedule for a specific hub
show_hub() {
    local hub="$1"
    local provider freq months

    # Check if hub exists
    if ! grep -q "^${hub}\.provider" "$I3_CONF"; then
        # Check if it's on-hold
        if grep -q "^# *${hub}\.schedule\.status" "$I3_CONF"; then
            provider=$(get_provider_name "$hub")
            echo "Hub: $hub"
            echo "Provider: $provider"
            echo "Status: ON HOLD"
            return
        fi
        echo "Error: Hub '$hub' not found"
        exit 1
    fi

    provider=$(get_provider_name "$hub")
    freq=$(get_frequency "$hub")
    months=$(get_months "$hub")

    echo "Hub: $hub"
    echo "Provider: $provider"

    if [[ -z "$freq" ]]; then
        echo "Schedule: Not configured"
        return
    fi

    echo "Frequency: $freq"

    if [[ "$freq" == "as-needed" ]]; then
        echo "Months: As needed (no fixed schedule)"
    elif [[ -n "$months" ]]; then
        # Convert month numbers to names
        local month_names=""
        for m in $months; do
            local name
            name=$(get_month_name "$m")
            if [[ -z "$month_names" ]]; then
                month_names="$name"
            else
                month_names="$month_names, $name"
            fi
        done
        echo "Months: $month_names"
    fi
}

# Show full year schedule
show_year() {
    echo "DPLA Hub Ingest Schedule"
    echo "========================"
    echo

    for month in 1 2 3 4 5 6 7 8 9 10 11 12; do
        local month_name
        month_name=$(get_month_name "$month")
        local hubs=""
        local count=0

        while IFS= read -r hub; do
            [[ -z "$hub" ]] && continue
            if is_scheduled_for_month "$hub" "$month"; then
                if [[ -z "$hubs" ]]; then
                    hubs="$hub"
                else
                    hubs="$hubs, $hub"
                fi
                ((count++))
            fi
        done < <(get_scheduled_hubs)

        echo "$month_name ($count hubs)"
        echo "  $hubs"
        echo
    done

    # Show on-hold hubs
    local onhold=""
    while IFS= read -r hub; do
        [[ -z "$hub" ]] && continue
        if [[ -z "$onhold" ]]; then
            onhold="$hub"
        else
            onhold="$onhold, $hub"
        fi
    done < <(get_onhold_hubs)

    if [[ -n "$onhold" ]]; then
        echo "On Hold: $onhold"
    fi

    # Show as-needed hubs
    local asneeded=""
    while IFS= read -r hub; do
        [[ -z "$hub" ]] && continue
        if [[ -z "$asneeded" ]]; then
            asneeded="$hub"
        else
            asneeded="$asneeded, $hub"
        fi
    done < <(get_asneeded_hubs)

    if [[ -n "$asneeded" ]]; then
        echo "As Needed: $asneeded"
    fi
}

# Main
main() {
    if [[ $# -eq 0 ]]; then
        show_year
    else
        local arg="$1"
        local month_num

        # Try to parse as month
        month_num=$(get_month_number "$arg")
        if [[ -n "$month_num" ]]; then
            show_month "$month_num"
        else
            # Treat as hub name
            show_hub "$arg"
        fi
    fi
}

main "$@"
