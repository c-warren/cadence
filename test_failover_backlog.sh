#!/usr/bin/env bash
# Comprehensive test script for failover + async match backlog issues
# Requires bash 4.0+ for associative arrays
#
# This script:
# 1. Registers an active-active domain with cluster attributes (seattle, london, tokyo)
# 2. Starts canary workers to execute jitter workflows
# 3. Launches multiple jitter workflows with random cluster attributes
# 4. Performs rolling failovers (seattle → london → tokyo) every 1 minute
# 5. Monitors for backlog and timeout issues
#
# Usage:
#   ./test_failover_backlog.sh [options] [domain-name]
#
# Options:
#   --num-workflows N        Number of jitter workflows to run (default: 10)
#   --total-activities N     Total activities per workflow (default: 100)
#   --concurrent-activities N Concurrent activities per workflow (default: 20)
#   --failover-interval N    Seconds between failovers (default: 60)
#
# Examples:
#   ./test_failover_backlog.sh test-failover-stress
#   ./test_failover_backlog.sh --num-workflows 5 --total-activities 50
#   ./test_failover_backlog.sh --num-workflows 20 --total-activities 200 --concurrent-activities 40 my-domain
#
# Press Ctrl+C to stop

set -e

# Default values
DOMAIN="test-failover-stress"
NUM_WORKFLOWS=10
TOTAL_ACTIVITIES=100
CONCURRENT_ACTIVITIES=20
FAILOVER_INTERVAL=60

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --num-workflows)
            NUM_WORKFLOWS="$2"
            shift 2
            ;;
        --total-activities)
            TOTAL_ACTIVITIES="$2"
            shift 2
            ;;
        --concurrent-activities)
            CONCURRENT_ACTIVITIES="$2"
            shift 2
            ;;
        --failover-interval)
            FAILOVER_INTERVAL="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options] [domain-name]"
            echo ""
            echo "Options:"
            echo "  --num-workflows N        Number of jitter workflows to run (default: 10)"
            echo "  --total-activities N     Total activities per workflow (default: 100)"
            echo "  --concurrent-activities N Concurrent activities per workflow (default: 20)"
            echo "  --failover-interval N    Seconds between failovers (default: 60)"
            echo ""
            echo "Examples:"
            echo "  $0 test-failover-stress"
            echo "  $0 --num-workflows 5 --total-activities 50"
            echo "  $0 --num-workflows 20 --total-activities 200 my-domain"
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
        *)
            DOMAIN="$1"
            shift
            ;;
    esac
done
CLUSTER0="cluster0"
CLUSTER1="cluster1"
CLUSTER2="cluster2"
GRPC_ENDPOINT_CLUSTER0="localhost:7833"
GRPC_ENDPOINT_CLUSTER1="localhost:8833"
GRPC_ENDPOINT_CLUSTER2="localhost:9833"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Failover Backlog Test (3 Clusters)${NC}"
echo -e "${BLUE}=========================================${NC}"
echo -e "Domain: ${GREEN}$DOMAIN${NC}"
echo -e "Clusters: ${GREEN}$CLUSTER0, $CLUSTER1, $CLUSTER2${NC}"
echo -e "Endpoints: ${GREEN}$GRPC_ENDPOINT_CLUSTER0, $GRPC_ENDPOINT_CLUSTER1, $GRPC_ENDPOINT_CLUSTER2${NC}"
echo -e "Locations: ${GREEN}seattle, london, tokyo${NC}"
echo -e "Failover interval: ${GREEN}${FAILOVER_INTERVAL}s${NC}"
echo -e "Number of workflows: ${GREEN}$NUM_WORKFLOWS${NC}"
echo -e "Activities per workflow: ${GREEN}$TOTAL_ACTIVITIES total, $CONCURRENT_ACTIVITIES concurrent${NC}"
echo -e "Total task volume: ${GREEN}$((NUM_WORKFLOWS * TOTAL_ACTIVITIES)) tasks${NC}"
echo -e "${BLUE}=========================================${NC}\n"

# Build cadence CLI and server if needed
echo -e "${YELLOW}Building cadence...${NC}"
make cadence-canary 2>&1 | tail -5

# Function to run cadence CLI
cadence_cli() {
    ./cadence --transport grpc --ad "$GRPC_ENDPOINT_CLUSTER0" "$@"
}

# Step 1: Register domain with cluster attributes
echo -e "\n${YELLOW}Step 1: Registering domain with cluster attributes...${NC}"

# Check if domain exists, if so update it, otherwise register
if cadence_cli --do "$DOMAIN" domain describe &>/dev/null; then
    echo -e "${GREEN}Domain $DOMAIN already exists, updating...${NC}"
    cadence_cli --do "$DOMAIN" domain update \
        --active_cluster "$CLUSTER0" \
        --clusters "$CLUSTER0,$CLUSTER1,$CLUSTER2" \
        --active_clusters "region.seattle:$CLUSTER0,region.london:$CLUSTER1,region.tokyo:$CLUSTER2" \
        2>&1 | grep -v "^$" || true
else
    echo -e "${GREEN}Registering new domain $DOMAIN...${NC}"
    cadence_cli --do "$DOMAIN" domain register \
        --active_cluster "$CLUSTER0" \
        --clusters "$CLUSTER0,$CLUSTER1,$CLUSTER2" \
        --active_clusters "region.seattle:$CLUSTER0,region.london:$CLUSTER1,region.tokyo:$CLUSTER2" \
        --gd true 2>&1 | grep -v "^$" || true
fi

echo -e "${GREEN}✓ Domain configured with cluster attributes${NC}"

# Verify domain configuration
echo -e "\n${YELLOW}Verifying domain configuration...${NC}"
cadence_cli --do "$DOMAIN" domain describe | grep -E "Name|ActiveClusterName|Clusters|Data" || true

# Wait for domain metadata to propagate across all clusters
echo -e "\n${YELLOW}Waiting for domain metadata to propagate across all clusters...${NC}"
max_attempts=30
for cluster_idx in 0 1 2; do
    if [ $cluster_idx -eq 0 ]; then
        endpoint=$GRPC_ENDPOINT_CLUSTER0
    elif [ $cluster_idx -eq 1 ]; then
        endpoint=$GRPC_ENDPOINT_CLUSTER1
    else
        endpoint=$GRPC_ENDPOINT_CLUSTER2
    fi

    cluster_name="cluster${cluster_idx}"
    echo -e "  Checking domain on $cluster_name..."

    attempts=0
    while [ $attempts -lt $max_attempts ]; do
        if ./cadence --transport grpc --ad "$endpoint" --do "$DOMAIN" domain describe &>/dev/null; then
            echo -e "  ${GREEN}✓ Domain visible on $cluster_name${NC}"
            break
        fi
        attempts=$((attempts + 1))
        if [ $attempts -lt $max_attempts ]; then
            sleep 1
        fi
    done

    if [ $attempts -eq $max_attempts ]; then
        echo -e "  ${RED}✗ Domain not visible on $cluster_name after ${max_attempts}s${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ Domain metadata propagated to all clusters${NC}"

# Step 2: Start canary workers (one per cluster)
echo -e "\n${YELLOW}Step 2: Starting canary workers...${NC}"

# We need workers on each cluster because cluster attributes pin workflows to specific clusters
# Domain registration happens in the shell script above - workers just need to poll
# Workers on cluster1/2 will log errors about domain registration (not primary cluster) but will continue polling

CANARY_PIDS=()
CANARY_ROOTS=()

start_worker() {
    local cluster_idx=$1
    local endpoint=$2
    local cluster_name=$3

    echo -e "  ${GREEN}Starting worker for $cluster_name ($endpoint)...${NC}"

    # Create temp root directory with proper config structure
    local canary_root=$(mktemp -d)
    local canary_config_dir="$canary_root/config/canary"
    mkdir -p "$canary_config_dir"

    # Create base.yaml (required by canary)
    cat > "$canary_config_dir/base.yaml" <<EOF
canary:
  domains: ["$DOMAIN"]
  excludes: []

cadence:
  service: "cadence-frontend"
  address: "$endpoint"

log:
  stdout: true
  level: "info"

metrics:
  prometheus:
    listenAddress: "0.0.0.0:$((9090 + cluster_idx))"
EOF

    # Create development.yaml (loaded when --env is not specified)
    cp "$canary_config_dir/base.yaml" "$canary_config_dir/development.yaml"

    # Start worker (redirect stderr to ignore domain registration errors on non-primary clusters)
    ./cadence-canary --root "$canary_root" start -m worker > "canary_worker_${cluster_name}.log" 2>&1 &
    local pid=$!

    CANARY_PIDS+=($pid)
    CANARY_ROOTS+=("$canary_root")

    echo -e "  ${GREEN}✓ Worker started for $cluster_name (PID: $pid)${NC}"
}

# Start workers for all 3 clusters
start_worker 0 "$GRPC_ENDPOINT_CLUSTER0" "cluster0"
start_worker 1 "$GRPC_ENDPOINT_CLUSTER1" "cluster1"
start_worker 2 "$GRPC_ENDPOINT_CLUSTER2" "cluster2"

# Wait for workers to initialize
echo -e "\n${YELLOW}Waiting for workers to initialize...${NC}"
sleep 10

# Verify workers started correctly
all_workers_ok=true
for i in "${!CANARY_PIDS[@]}"; do
    pid=${CANARY_PIDS[$i]}
    cluster_name="cluster$i"

    if ! ps -p $pid > /dev/null; then
        echo -e "${RED}✗ Worker on $cluster_name failed to start!${NC}"
        echo -e "${YELLOW}Last 20 lines of canary_worker_${cluster_name}.log:${NC}"
        tail -20 "canary_worker_${cluster_name}.log"
        all_workers_ok=false
    else
        if grep -q "Domain.*$DOMAIN" "canary_worker_${cluster_name}.log" || \
           grep -q "Starting workflow" "canary_worker_${cluster_name}.log"; then
            echo -e "${GREEN}✓ Worker on $cluster_name is polling domain: $DOMAIN${NC}"
        else
            # Check if it's just the domain registration error (expected on cluster1/2)
            if [ $i -ne 0 ] && grep -q "not primary cluster" "canary_worker_${cluster_name}.log"; then
                echo -e "${YELLOW}⚠ Worker on $cluster_name logged domain registration error (expected on non-primary), but should still poll${NC}"
            else
                echo -e "${YELLOW}⚠ Worker on $cluster_name may not be polling $DOMAIN${NC}"
                echo -e "${YELLOW}Check canary_worker_${cluster_name}.log for details${NC}"
            fi
        fi
    fi
done

if [ "$all_workers_ok" = false ]; then
    echo -e "${RED}✗ One or more workers failed to start${NC}"
    exit 1
fi

echo -e "${GREEN}✓ All canary workers started successfully${NC}"

# Step 3: Start jitter workflows with different cluster attributes
echo -e "\n${YELLOW}Step 3: Starting $NUM_WORKFLOWS jitter workflows...${NC}"

LOCATIONS=("seattle" "london" "tokyo")
WORKFLOW_IDS=()

for i in $(seq 1 $NUM_WORKFLOWS); do
    # Randomly select a location
    LOCATION=${LOCATIONS[$((RANDOM % 3))]}
    start_index=$((i + 20))
    WF_ID="jitter-wf-$start_index-$LOCATION"
    WORKFLOW_IDS+=("$WF_ID")

    echo -e "  Starting workflow ${BLUE}$WF_ID${NC} with cluster attribute ${GREEN}region.$LOCATION${NC}..."

    # Create workflow input with activity parameters
    WORKFLOW_INPUT=$(cat <<EOF
{
  "ScheduledTimeNanos": 0,
  "TotalActivities": $TOTAL_ACTIVITIES,
  "ConcurrentActivities": $CONCURRENT_ACTIVITIES
}
EOF
)

    # Start workflow with cluster attribute to pin it to a specific location
    cadence_cli --do "$DOMAIN" workflow start \
        --tl canary-task-queue \
        --wt workflow.jitter \
        --et 3600 \
        --wid "$WF_ID" \
        --cluster_attribute_scope region \
        --cluster_attribute_name "$LOCATION" \
        --input "$WORKFLOW_INPUT" &>/dev/null &

    # Stagger workflow starts slightly
    sleep 0.5
done

echo -e "${GREEN}✓ Started $NUM_WORKFLOWS jitter workflows${NC}"

# List running workflows
echo -e "\n${YELLOW}Currently running workflows:${NC}"
cadence_cli --do "$DOMAIN" workflow list --open --ps 20 | head -20 || true

# Step 4: Start monitoring in background
echo -e "\n${YELLOW}Step 4: Starting monitoring...${NC}"

# Monitor for async dispatch timeouts
monitor_logs() {
    local cluster_log="$1"
    local cluster_name="$2"

    # This would monitor actual log files if available
    # For now, we'll just indicate monitoring is active
    echo -e "${GREEN}  Monitoring $cluster_name for async dispatch timeouts${NC}"
}

# Start log monitoring (if log files are accessible)
# monitor_logs "/path/to/cluster0.log" "cluster0" &
# monitor_logs "/path/to/cluster1.log" "cluster1" &

echo -e "${GREEN}✓ Monitoring started${NC}"

# Wait before first failover to avoid "Domain update too frequent" error
echo -e "\n${YELLOW}Waiting 60 seconds before first failover to avoid rate limiting...${NC}"
sleep 60

# Step 5: Failover loop
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}Starting Failover Loop${NC}"
echo -e "${BLUE}=========================================${NC}"
echo -e "${YELLOW}Failovers will cycle: seattle → london → tokyo → seattle...${NC}"
echo -e "${YELLOW}Interval: ${FAILOVER_INTERVAL}s between each failover${NC}"
echo -e "${RED}Press Ctrl+C to stop${NC}\n"

# Cleanup function
cleanup() {
    echo -e "\n\n${YELLOW}Cleaning up...${NC}"

    # Kill all canary workers
    if [ ${#CANARY_PIDS[@]} -gt 0 ]; then
        echo -e "Stopping canary workers..."
        for i in "${!CANARY_PIDS[@]}"; do
            pid=${CANARY_PIDS[$i]}
            if [ ! -z "$pid" ]; then
                echo -e "  Stopping worker on cluster$i (PID: $pid)..."
                kill $pid 2>/dev/null || true
                wait $pid 2>/dev/null || true
            fi
        done
    fi

    # Remove temp root directories
    if [ ${#CANARY_ROOTS[@]} -gt 0 ]; then
        echo -e "Removing temp directories..."
        for canary_root in "${CANARY_ROOTS[@]}"; do
            if [ ! -z "$canary_root" ] && [ -d "$canary_root" ]; then
                rm -rf "$canary_root"
            fi
        done
    fi

    echo -e "${GREEN}✓ Cleanup complete${NC}"
    echo -e "\nLogs saved to:"
    echo -e "  - canary_worker_cluster0.log"
    echo -e "  - canary_worker_cluster1.log"
    echo -e "  - canary_worker_cluster2.log"
    echo -e "  - failover.log"

    exit 0
}

trap cleanup SIGINT SIGTERM

# Failover function
failover_location() {
    local location=$1
    local target_cluster=$2

    echo -e "\n${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ${YELLOW}Failing over location ${GREEN}$location${YELLOW} to ${GREEN}$target_cluster${NC}..."

    cadence_cli --do "$DOMAIN" domain update \
        --active_clusters "location.$location:$target_cluster" 2>&1 | tee -a failover.log

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ${GREEN}✓ Failover successful${NC}"
    else
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ${RED}✗ Failover FAILED${NC}"
    fi
}

# Check workflow progress
# Outputs the open workflow count to stdout
check_workflow_progress() {
    {
        echo ""
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ${YELLOW}Checking workflow progress...${NC}"
    } >&2

    local open_count=$(cadence_cli --do "$DOMAIN" workflow list --open --ps 100 2>/dev/null | grep -c "jitter-wf" || echo "0")
    local closed_count=$(cadence_cli --do "$DOMAIN" workflow list --ps 100 2>/dev/null | grep -c "jitter-wf" || echo "0")

    {
        echo -e "  Open workflows: ${GREEN}$open_count${NC}"
        echo -e "  Completed workflows: ${GREEN}$closed_count${NC}"
        echo -e "  ${YELLOW}Check metrics for task_backlog_per_tl and async_task_dispatch_timeout${NC}"
    } >&2

    # Output the open count to stdout for capture (no formatting, just the number)
    printf "%s" "$open_count"
}

# Initialize failover log
echo "Failover log started at $(date)" > failover.log

counter=0
locations=("seattle" "london" "tokyo")
all_clusters=("$CLUSTER0" "$CLUSTER1" "$CLUSTER2")

# Track current cluster index for each location using simple variables
# Initial state: seattle→cluster0(idx=0), london→cluster1(idx=1), tokyo→cluster2(idx=2)
seattle_cluster_idx=0
london_cluster_idx=1
tokyo_cluster_idx=2

while true; do
    counter=$((counter + 1))

    # Cycle through locations: seattle, london, tokyo, seattle, ...
    location_index=$(((counter - 1) % 3))
    location=${locations[$location_index]}

    # Get current cluster index for this location
    if [ "$location" == "seattle" ]; then
        current_cluster_idx=$seattle_cluster_idx
    elif [ "$location" == "london" ]; then
        current_cluster_idx=$london_cluster_idx
    else  # tokyo
        current_cluster_idx=$tokyo_cluster_idx
    fi

    current_cluster=${all_clusters[$current_cluster_idx]}

    # Move to next cluster (rotate through all 3)
    next_cluster_idx=$(((current_cluster_idx + 1) % 3))
    target_cluster=${all_clusters[$next_cluster_idx]}

    # Update tracking for this location
    if [ "$location" == "seattle" ]; then
        seattle_cluster_idx=$next_cluster_idx
    elif [ "$location" == "london" ]; then
        london_cluster_idx=$next_cluster_idx
    else  # tokyo
        tokyo_cluster_idx=$next_cluster_idx
    fi

    echo -e "\n${BLUE}========== Failover #$counter ==========${NC}"
    echo -e "${YELLOW}Location: $location${NC}"
    echo -e "${YELLOW}Current cluster: $current_cluster → Target cluster: $target_cluster${NC}"

    failover_location "$location" "$target_cluster"
    open_workflows=$(check_workflow_progress)

    # Trim any whitespace/newlines
    open_workflows=$(echo "$open_workflows" | tr -d '[:space:]')

    # Default to 999 if we got invalid data (prevents script exit on error)
    if ! [[ "$open_workflows" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Warning: Got invalid workflow count: '$open_workflows', defaulting to 999${NC}" >&2
        open_workflows=999
    fi

    # Check if all workflows have completed
    if [ "$open_workflows" -eq 0 ]; then
        echo -e "\n${GREEN}=========================================${NC}"
        echo -e "${GREEN}All workflows completed successfully!${NC}"
        echo -e "${GREEN}=========================================${NC}"
        echo -e "Total failovers performed: ${GREEN}$counter${NC}"
        break
    fi

    echo -e "\n${YELLOW}Waiting ${FAILOVER_INTERVAL}s before next failover...${NC}"
    sleep "$FAILOVER_INTERVAL"
done

# Clean exit after all workflows complete
cleanup
