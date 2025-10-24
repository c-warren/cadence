#!/bin/bash

# Script to list and fetch all failover events for a domain with size reporting
# Usage: ./test_failover_events_with_sizes.sh <domain_id> <server_log_file>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <domain_id> [server_log_file]"
    echo ""
    echo "If server_log_file is provided, will extract size information from debug logs"
    exit 1
fi

DOMAIN_ID="$1"
LOG_FILE="${2:-}"

echo "========================================="
echo "Testing Failover Events for Domain: $DOMAIN_ID"
echo "========================================="
echo ""

# Common grpcurl parameters
GRPCURL_COMMON=(
    -plaintext
    -import-path ./idls/proto/uber/cadence/api/v1
    -import-path ./idls/proto
    -proto ./idls/proto/uber/cadence/api/v1/service_domain.proto
    -H 'Rpc-Caller: manual-test'
    -H 'Rpc-Service: cadence-frontend'
    -H 'Rpc-Encoding: proto'
    -H 'Context-Ttl-Ms: 60000'
    -H 'yarpc-ttl: 60000'
    -max-time 60
)

# Function to extract size from logs
extract_size_from_logs() {
    local event_id=$1
    if [ -z "$LOG_FILE" ]; then
        echo "  (No log file provided for size extraction)"
        return
    fi

    if [ ! -f "$LOG_FILE" ]; then
        echo "  (Log file not found: $LOG_FILE)"
        return
    fi

    # Search for the debug log entry
    local log_entry=$(grep "DEBUG: Retrieved audit log entry successfully" "$LOG_FILE" | grep "$event_id" | tail -1)

    if [ -z "$log_entry" ]; then
        echo "  (No size info found in logs)"
        return
    fi

    # Extract sizes using sed/grep
    local before_size=$(echo "$log_entry" | grep -o "state_before_size=[0-9]*" | cut -d= -f2)
    local after_size=$(echo "$log_entry" | grep -o "state_after_size=[0-9]*" | cut -d= -f2)

    if [ -n "$before_size" ] && [ -n "$after_size" ]; then
        local total=$((before_size + after_size))
        echo "  Storage Sizes:"
        echo "    - state_before: $before_size bytes ($(echo "scale=2; $before_size/1024" | bc) KB)"
        echo "    - state_after:  $after_size bytes ($(echo "scale=2; $after_size/1024" | bc) KB)"
        echo "    - total:        $total bytes ($(echo "scale=2; $total/1024" | bc) KB)"
    fi
}

# Step 1: List failover events
echo "Step 1: Listing failover events..."
echo "-----------------------------------"

LIST_RESPONSE=$(grpcurl "${GRPCURL_COMMON[@]}" \
    -d "{\"filters\":{\"domain_id\":\"$DOMAIN_ID\"}}" \
    localhost:7833 \
    uber.cadence.api.v1.DomainAPI/ListFailoverHistory)

echo "$LIST_RESPONSE"
echo ""

# Parse the response to extract event IDs and timestamps
if command -v jq &> /dev/null; then
    echo "Using jq to parse JSON..."

    # Extract array of events
    NUM_EVENTS=$(echo "$LIST_RESPONSE" | jq '.failoverEvents | length' 2>/dev/null || echo "0")

    if [ "$NUM_EVENTS" -eq 0 ]; then
        echo "No failover events found."
        exit 0
    fi

    echo "Found $NUM_EVENTS failover event(s)"
    echo ""

    # Step 2: Fetch each failover event
    echo "Step 2: Fetching details for each event..."
    echo "-------------------------------------------"

    for i in $(seq 0 $((NUM_EVENTS - 1))); do
        EVENT_ID=$(echo "$LIST_RESPONSE" | jq -r ".failoverEvents[$i].id")
        CREATED_TIME=$(echo "$LIST_RESPONSE" | jq -r ".failoverEvents[$i].createdTime")

        echo ""
        echo "Event $((i + 1))/$NUM_EVENTS:"
        echo "  ID: $EVENT_ID"
        echo "  Created Time: $CREATED_TIME"
        echo ""

        echo "  Fetching event details..."
        EVENT_RESPONSE=$(grpcurl "${GRPCURL_COMMON[@]}" \
            -d "{\"domain_id\": \"$DOMAIN_ID\", \"failover_event_id\": \"$EVENT_ID\", \"created_time\": \"$CREATED_TIME\"}" \
            localhost:7833 \
            uber.cadence.api.v1.DomainAPI/GetFailoverEvent)

        echo "$EVENT_RESPONSE"
        echo ""

        # Try to extract size info from logs
        extract_size_from_logs "$EVENT_ID"

        echo "  ----------------------------------------"
    done
else
    echo "WARNING: jq not found. Install jq for better JSON parsing."
    echo "Please install jq to use this script."
    exit 1
fi

echo ""
echo "========================================="
echo "Testing complete!"
echo "========================================="

if [ -n "$LOG_FILE" ]; then
    echo ""
    echo "Tip: Check server logs at $LOG_FILE for detailed size information"
fi
