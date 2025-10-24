#!/bin/bash

# Script to check storage sizes of domain_audit_table rows
# Usage: ./check_audit_row_sizes.sh <domain_id>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <domain_id>"
    exit 1
fi

DOMAIN_ID="$1"

echo "========================================="
echo "Checking Domain Audit Table Row Sizes"
echo "Domain ID: $DOMAIN_ID"
echo "========================================="
echo ""

# CQL query to get row sizes
# Multi-cluster setup uses cadence_cluster0, cadence_cluster1, cadence_cluster2
KEYSPACES="${CASSANDRA_KEYSPACES:-cadence_cluster0 cadence_cluster1 cadence_cluster2}"

echo "Querying Cassandra for row sizes across all keyspaces..."
echo ""

# Check if we should use docker compose
if docker compose -f docker/dev/cassandra.yml ps cassandra &>/dev/null; then
    echo "Using Docker Compose to query Cassandra..."
    CQLSH_CMD="docker compose -f docker/dev/cassandra.yml exec -T cassandra cqlsh"
else
    echo "Docker Compose not detected, using local cqlsh..."
    CQLSH_CMD="cqlsh ${CASSANDRA_HOST:-127.0.0.1} ${CASSANDRA_PORT:-9042}"
fi

# Track totals across all keyspaces
GRAND_TOTAL_ROWS=0
GRAND_TOTAL_BEFORE=0
GRAND_TOTAL_AFTER=0
GRAND_TOTAL_STORAGE=0

# Query each keyspace
for KEYSPACE in $KEYSPACES; do
    echo "========================================="
    echo "Keyspace: $KEYSPACE"
    echo "========================================="
    echo ""

    # Check if keyspace exists first
    KEYSPACE_EXISTS=$($CQLSH_CMD -e "DESCRIBE KEYSPACE $KEYSPACE;" 2>&1 | grep -c "CREATE KEYSPACE" || echo "0")

    if [ "$KEYSPACE_EXISTS" -eq 0 ]; then
        echo "Keyspace $KEYSPACE does not exist, skipping..."
        echo ""
        continue
    fi

    # Query for individual row sizes
    echo "Individual rows in $KEYSPACE:"
    echo "-----------------------------------"

    # Note: Cassandra doesn't have a built-in blob_size_in_bytes function
    # So we'll just list the events and note that sizes are in debug logs
    $CQLSH_CMD <<EOF
SELECT
    event_id,
    created_time,
    state_before_encoding,
    state_after_encoding,
    operation_type
FROM ${KEYSPACE}.domain_audit_log
WHERE domain_id = ${DOMAIN_ID}
ALLOW FILTERING;
EOF

    echo ""
    echo "Row count for $KEYSPACE:"
    echo "-----------------------------------"

    # Get count
    COUNT_OUTPUT=$($CQLSH_CMD <<EOF
SELECT COUNT(*) as row_count
FROM ${KEYSPACE}.domain_audit_log
WHERE domain_id = ${DOMAIN_ID}
ALLOW FILTERING;
EOF
)

    echo "$COUNT_OUTPUT"
    echo ""

    # Extract count for grand total
    ROW_COUNT=$(echo "$COUNT_OUTPUT" | grep -oE '[0-9]+' | tail -1 || echo "0")
    if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -gt 0 ]; then
        GRAND_TOTAL_ROWS=$((GRAND_TOTAL_ROWS + ROW_COUNT))
        echo "Found $ROW_COUNT row(s) in $KEYSPACE"
    else
        echo "No rows found in $KEYSPACE"
    fi
done

echo ""
echo "========================================="
echo "GRAND TOTAL ACROSS ALL KEYSPACES"
echo "========================================="
echo "Found data in keyspaces for domain $DOMAIN_ID"
echo ""
if [ "$GRAND_TOTAL_ROWS" -gt 0 ]; then
    echo "Total rows found: $GRAND_TOTAL_ROWS"
else
    echo "No rows found across any keyspace"
fi

echo ""
echo "========================================="
echo "Done!"
echo "========================================="
