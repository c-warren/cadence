#!/bin/bash
# Helper script to perform periodic manual failovers during simulation testing
# This script alternates the active cluster every 30 seconds
#
# Usage:
#   ./failover_loop.sh <domain-name> [interval-seconds]
#
# Example:
#   ./failover_loop.sh test-domain-failover-stress 30

set -e

DOMAIN="${1:-test-domain-failover-stress}"
INTERVAL="${2:-30}"
CLUSTER0_ENDPOINT="127.0.0.1:7833"  # Adjust if needed
CLUSTER1_ENDPOINT="127.0.0.1:8833"  # Adjust if needed

echo "========================================="
echo "Failover Loop for Domain: $DOMAIN"
echo "Interval: ${INTERVAL}s"
echo "========================================="
echo ""
echo "This script will alternate active cluster between cluster0 and cluster1"
echo "Press Ctrl+C to stop"
echo ""

# Function to perform failover
failover_to_cluster() {
    local cluster=$1
    local endpoint=$2

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failing over to $cluster..."

    # Using docker exec to run cadence CLI inside the cluster0 container
    # Adjust the container name if different
    docker exec cadence-cluster0 cadence \
        --do "$DOMAIN" \
        --ad "$endpoint" \
        domain update \
        --active_cluster "$cluster" 2>&1 | tee -a failover.log

    if [ $? -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Failover to $cluster successful"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ Failover to $cluster FAILED"
    fi
    echo ""
}

# Function to check backlog
check_backlog() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Checking task backlog..."
    # You can add prometheus/metrics query here if available
    # For now, just log a marker
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Check metrics for task_backlog_per_tl and async_task_dispatch_timeout"
    echo ""
}

# Initialize log file
echo "Failover log started at $(date)" > failover.log

counter=0
current_cluster="cluster1"  # Start by failing over to cluster1

while true; do
    counter=$((counter + 1))
    echo "========== Failover #$counter =========="

    # Alternate between clusters
    if [ "$current_cluster" == "cluster0" ]; then
        current_cluster="cluster1"
        endpoint=$CLUSTER1_ENDPOINT
    else
        current_cluster="cluster0"
        endpoint=$CLUSTER0_ENDPOINT
    fi

    failover_to_cluster "$current_cluster" "$endpoint"
    check_backlog

    echo "Waiting ${INTERVAL}s before next failover..."
    echo ""
    sleep "$INTERVAL"
done
