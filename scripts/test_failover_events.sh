#!/bin/bash

# Script to list and fetch all failover events for a domain
# Usage: ./test_failover_events.sh <domain_id>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <domain_id>"
    exit 1
fi

DOMAIN_ID="$1"

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
# The response format is like:
# {
#   "failoverEvents": [
#     {
#       "id": "...",
#       "createdTime": "1234567890123",
#       ...
#     }
#   ]
# }

# Extract events using jq if available, otherwise use grep/sed
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
        grpcurl "${GRPCURL_COMMON[@]}" \
            -d "{\"domain_id\": \"$DOMAIN_ID\", \"failover_event_id\": \"$EVENT_ID\", \"created_time\": \"$CREATED_TIME\"}" \
            localhost:7833 \
            uber.cadence.api.v1.DomainAPI/GetFailoverEvent

        echo ""
        echo "  ----------------------------------------"
    done
else
    echo "WARNING: jq not found. Install jq for better JSON parsing."
    echo "Attempting basic parsing with grep/sed..."
    echo ""

    # Extract event IDs using grep and sed (less reliable but works without jq)
    EVENT_IDS=$(echo "$LIST_RESPONSE" | grep -o '"id": "[^"]*"' | sed 's/"id": "\([^"]*\)"/\1/')
    CREATED_TIMES=$(echo "$LIST_RESPONSE" | grep -o '"createdTime": "[^"]*"' | sed 's/"createdTime": "\([^"]*\)"/\1/')

    if [ -z "$EVENT_IDS" ]; then
        echo "No failover events found."
        exit 0
    fi

    # Convert to arrays
    IFS=$'\n' read -rd '' -a EVENT_ID_ARRAY <<<"$EVENT_IDS" || true
    IFS=$'\n' read -rd '' -a CREATED_TIME_ARRAY <<<"$CREATED_TIMES" || true

    NUM_EVENTS=${#EVENT_ID_ARRAY[@]}
    echo "Found $NUM_EVENTS failover event(s)"
    echo ""

    # Step 2: Fetch each failover event
    echo "Step 2: Fetching details for each event..."
    echo "-------------------------------------------"

    for i in "${!EVENT_ID_ARRAY[@]}"; do
        EVENT_ID="${EVENT_ID_ARRAY[$i]}"
        CREATED_TIME="${CREATED_TIME_ARRAY[$i]}"

        echo ""
        echo "Event $((i + 1))/$NUM_EVENTS:"
        echo "  ID: $EVENT_ID"
        echo "  Created Time: $CREATED_TIME"
        echo ""

        echo "  Fetching event details..."
        grpcurl "${GRPCURL_COMMON[@]}" \
            -d "{\"domain_id\": \"$DOMAIN_ID\", \"failover_event_id\": \"$EVENT_ID\", \"created_time\": \"$CREATED_TIME\"}" \
            localhost:7833 \
            uber.cadence.api.v1.DomainAPI/GetFailoverEvent

        echo ""
        echo "  ----------------------------------------"
    done
fi

echo ""
echo "========================================="
echo "Testing complete!"
echo "========================================="
