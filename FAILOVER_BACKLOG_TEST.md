# Failover + Backlog Test Guide

This guide helps you reproduce the issue where **failover causes tasks to get stuck in backlog**, evidenced by:
- Increasing `task_backlog_per_tl` metric
- "Async task dispatch timed out" logs
- Workflows stalling during/after failover

## Overview

The test:
1. Creates an active-active domain with 3 cluster attributes: **seattle**, **london**, **tokyo**
2. Starts 20 "jitter" workflows (canary workflows that execute 500 concurrent activities)
3. Performs rolling failovers every 60 seconds: seattle → london → tokyo → repeat
4. Monitors for backlog issues

## Prerequisites

You need a **running 3-cluster Cadence deployment**:

### Option 1: XDC Development Clusters (Recommended)

Start 3 local clusters using the XDC configs:

```bash
# Terminal 1 - Cluster 0 (port 7833)
./cadence-server --config config --zone xdc --env development_xdc_cluster0 start

# Terminal 2 - Cluster 1 (port 8833)
./cadence-server --config config --zone xdc --env development_xdc_cluster1 start

# Terminal 3 - Cluster 2 (port 9833)
./cadence-server --config config --zone xdc --env development_xdc_cluster2 start
```

### Option 2: Your Custom Clusters

Update the script's endpoint variables:
```bash
GRPC_ENDPOINT_CLUSTER0="your-cluster0-host:7833"
GRPC_ENDPOINT_CLUSTER1="your-cluster1-host:8833"
GRPC_ENDPOINT_CLUSTER2="your-cluster2-host:9833"
```

## Quick Start

### 1. Build Everything

```bash
make cadence-canary
make cadence
```

### 2. Run the Test

**Basic usage** (uses defaults):
```bash
./test_failover_backlog.sh test-failover-stress
```

**Custom configuration**:
```bash
# Increase load for more aggressive testing
./test_failover_backlog.sh --num-workflows 20 --total-activities 200 test-failover-stress

# Reduce load to avoid Cassandra OOM
./test_failover_backlog.sh --num-workflows 5 --total-activities 50

# Full customization
./test_failover_backlog.sh \
  --num-workflows 15 \
  --total-activities 150 \
  --concurrent-activities 30 \
  --failover-interval 45 \
  my-domain-name
```

**Available options**:
- `--num-workflows N` - Number of jitter workflows to run (default: 10)
- `--total-activities N` - Total activities per workflow (default: 100)
- `--concurrent-activities N` - Concurrent activities per workflow (default: 20)
- `--failover-interval N` - Seconds between failovers (default: 60)
- `--help` - Show usage information

**What the test does**:
- ✅ Register domain with cluster attributes
- ✅ Start 3 canary workers (one per cluster)
- ✅ Launch N jitter workflows with random cluster attributes
- ✅ Begin rolling failovers every N seconds
- ✅ Monitor workflow progress
- ✅ Automatically stop when all workflows complete (or press Ctrl+C to stop early)

**Default load**: 10 workflows × 100 activities = 1,000 tasks. This is sized to avoid Cassandra OOM issues in local testing while still reproducing async match backlog problems during failovers.

### 3. Watch for Issues

**Terminal 1** (test is running):
```
[2025-01-15 10:30:00] Failing over location seattle to cluster1...
✓ Failover successful
  Open workflows: 18
  Completed workflows: 2
  Check metrics for task_backlog_per_tl and async_task_dispatch_timeout

Waiting 60s before next failover...
```

The test will automatically stop when all workflows complete, or you can press Ctrl+C to stop early.

**Terminal 2** (watch workflows):
```bash
watch -n 5 './cadence --transport grpc --ad localhost:7833 --do test-failover-stress workflow list --open | head -20'
```

**Terminal 3** (monitor logs for timeouts):
```bash
# If you have access to cluster logs
tail -f /path/to/matching.log | grep -i "async task dispatch timeout"
```

**Terminal 4** (check metrics):
```bash
# If you have prometheus/metrics endpoint
watch -n 5 'curl -s http://localhost:9091/metrics | grep task_backlog_per_tl'
```

## What to Look For

### ❌ Signs of the Bug

1. **Increasing Backlog**
   ```
   task_backlog_per_tl{domain="test-failover-stress"} 150  # Growing over time
   ```

2. **Timeout Logs**
   ```
   "Async task dispatch timed out" error="context deadline exceeded"
   domain="test-failover-stress" taskList="canary-task-queue"
   ```

3. **Stuck Workflows**
   ```bash
   # Number of open workflows stays constant even though they should be completing
   Open workflows: 20  # Same number for 10+ minutes
   ```

4. **Decision Task Accumulation**
   - Workflows show "decision tasks pending" but no progress
   - Activities not being scheduled

### ✅ Expected Behavior (After Fix)

- Workflows continue making progress during failovers
- Backlog remains low (< 10 tasks typically)
- No async dispatch timeout errors
- Workflows complete within expected time (~5-10 minutes)
- Test automatically stops with success message when all workflows finish:
  ```
  =========================================
  All workflows completed successfully!
  =========================================
  Total failovers performed: 5
  ```

## Understanding the Test

### Cluster Attributes

The domain has 3 logical locations mapped to 3 physical clusters:

```
Initial State:
  location.seattle → cluster0
  location.london  → cluster1
  location.tokyo   → cluster2

Failover Cycle (each location rotates through all 3 clusters):

Minute 0 - Failover seattle:
  location.seattle → cluster1  ← Moved (cluster0 → cluster1)
  location.london  → cluster1
  location.tokyo   → cluster2

Minute 1 - Failover london:
  location.seattle → cluster1
  location.london  → cluster2  ← Moved (cluster1 → cluster2)
  location.tokyo   → cluster2

Minute 2 - Failover tokyo:
  location.seattle → cluster1
  location.london  → cluster2
  location.tokyo   → cluster0  ← Moved (cluster2 → cluster0)

Minute 3 - Failover seattle:
  location.seattle → cluster2  ← Moved (cluster1 → cluster2)
  location.london  → cluster2
  location.tokyo   → cluster0

...continues cycling...
```

### Jitter Workflow

The jitter workflow (`canary/jitter.go`):
- Executes 100 activities total (reduced from 500 to avoid Cassandra OOM)
- Runs 20 activities concurrently at any time (reduced from 50)
- Each workflow takes ~2-5 minutes to complete
- Sensitive to matching service issues
- All activities go through async match (sync match disabled), putting more load on persistence

### Why This Reproduces the Bug

1. **Active-Active Domain**: Required for the bug (standby domains don't show the issue)
2. **Cluster Attributes**: Locations pin workflows to specific clusters
3. **Multi-Cluster Workers**: One worker per cluster to handle tasks on each cluster
4. **Failover During Execution**: Moving active location while workflows run
5. **High Task Volume**: 100 activities × 10 workflows = 1,000 tasks (all persisted via async match)
6. **Concurrent Execution**: Stresses the matching service
7. **Forced Async Match**: All tasks go through persistence layer instead of in-memory sync match

### Worker Setup

The test starts **3 canary workers**, one connected to each cluster:
- Worker 1 → cluster0 (localhost:7833) - Handles seattle workflows (initially)
- Worker 2 → cluster1 (localhost:8833) - Handles london workflows (initially)
- Worker 3 → cluster2 (localhost:9833) - Handles tokyo workflows (initially)

Each worker polls the same domain (`test-failover-stress`) but receives tasks based on which cluster is active for each location's cluster attribute. This is necessary because cluster attributes pin workflows to specific physical clusters.

## Customizing the Test

### Run More/Fewer Workflows

Use command-line parameters:
```bash
# Light load - good for initial testing
./test_failover_backlog.sh --num-workflows 5 --total-activities 50

# Heavy load - more aggressive stress testing (may cause Cassandra OOM)
./test_failover_backlog.sh --num-workflows 20 --total-activities 200
```

### Change Failover Frequency

```bash
# Failover every 30 seconds (more aggressive)
./test_failover_backlog.sh --failover-interval 30

# Failover every 2 minutes (less aggressive)
./test_failover_backlog.sh --failover-interval 120
```

### Adjust Concurrent Activities

```bash
# More concurrency - higher load on matching service
./test_failover_backlog.sh --concurrent-activities 40

# Less concurrency - lower load but longer workflow duration
./test_failover_backlog.sh --concurrent-activities 10
```

### Use Different Locations

Edit the script to use different location names:
```bash
LOCATIONS=("us-west" "us-east" "eu-west")
```

Then update the cluster_data when registering the domain.

### Test Specific Scenarios

#### Scenario 1: Kill Matching Service During Failover

```bash
# In another terminal, kill matching periodically
while true; do
    sleep 90  # Offset from failover timing
    docker exec cadence-cluster0 pkill -f matching
    sleep 120
done
```

#### Scenario 2: Network Partition Simulation

```bash
# Disconnect cluster1 during failover
docker network disconnect cadence-net cadence-cluster1
sleep 30
docker network connect cadence-net cadence-cluster1
```

## Debugging

### Check Domain Configuration

```bash
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress domain describe
```

Look for:
- `ActiveClusterName`: Which cluster is currently active for each location
- `ClusterReplicationConfig`: The cluster attribute mappings

### Check Task List

```bash
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  tasklist describe --tl canary-task-queue --tlt activity
```

### Observe Specific Workflow

```bash
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  workflow observe -w jitter-wf-1-seattle
```

### Check History for Stuck Workflow

```bash
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  workflow show -w jitter-wf-1-seattle | grep -i "activity\|decision"
```

### Manual Failover

If you want to manually trigger a failover instead of using the loop:

```bash
# Failover seattle to cluster1
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  domain update --active_clusters 'location.seattle:cluster1'

# Failover london to cluster0
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  domain update --active_clusters 'location.london:cluster0'
```

## Testing Your Fix

After making changes to force async matching:

1. **Rebuild**:
   ```bash
   make clean
   make cadence-canary
   ```

2. **Run test**:
   ```bash
   ./test_failover_backlog.sh test-after-fix
   ```

3. **Compare results**:
   - Before fix: Backlog increases, timeouts occur, workflows stuck
   - After fix: Backlog stable, no timeouts, workflows complete

## Cleanup

The test will automatically clean up when all workflows complete, or you can stop it early with `Ctrl+C`.

The script will automatically:
- Stop all 3 canary workers
- Clean up temporary files
- Save logs to:
  - `canary_worker_cluster0.log`
  - `canary_worker_cluster1.log`
  - `canary_worker_cluster2.log`
  - `failover.log`

To clean up workflows:

```bash
# List and terminate stuck workflows if needed
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress workflow list --open

./cadence --transport grpc --ad localhost:7833 --do test-failover-stress \
  workflow terminate -w jitter-wf-1-seattle --reason "test cleanup"
```

## Files Created

- `canary/jitter.go` - Jitter workflow implementation
- `canary/const.go` - Updated with jitter constants
- `test_failover_backlog.sh` - Main test script (starts 3 workers, handles failovers)
- `FAILOVER_BACKLOG_TEST.md` - This guide

## Troubleshooting

### "Domain not found"

The script auto-registers the domain. If it fails:
```bash
./cadence --transport grpc --ad localhost:7833 --do test-failover-stress domain register \
  --active_cluster cluster0 \
  --clusters cluster0,cluster1,cluster2 \
  --active_clusters "location.seattle:cluster0" "location.london:cluster1" "location.tokyo:cluster2" \
  --is_global_domain true
```

### "Task list not found"

Wait a few seconds for canary worker to start and register task lists.

### Workflows not starting

Check canary worker logs:
```bash
tail -f canary_worker_cluster0.log
tail -f canary_worker_cluster1.log
tail -f canary_worker_cluster2.log
```

Verify all workers are running:
```bash
ps aux | grep cadence-canary
```

You should see 3 worker processes running.

### No failovers happening

Check that all 3 clusters are reachable:
```bash
./cadence --transport grpc --ad localhost:7833 cluster health
./cadence --transport grpc --ad localhost:8833 cluster health
./cadence --transport grpc --ad localhost:9833 cluster health
```

### Cassandra Out of Memory

If you see Cassandra OOM errors:
```
ERROR: java.lang.OutOfMemoryError: Java heap space
```

**Why this happens**: Forcing async match persists every task to Cassandra. With local Docker, this can exhaust memory.

**Solutions**:
1. **Reduce test scale using command-line parameters** (easiest):
   ```bash
   # Minimal load
   ./test_failover_backlog.sh --num-workflows 5 --total-activities 50

   # Very light load for constrained environments
   ./test_failover_backlog.sh --num-workflows 3 --total-activities 30
   ```

2. **Increase Cassandra memory**:
   ```bash
   # Edit your docker-compose or container settings
   environment:
     - MAX_HEAP_SIZE=2G
     - HEAP_NEWSIZE=512M
   ```

3. **Restart Cassandra**:
   ```bash
   docker restart cassandra
   # Wait for it to come back up
   sleep 30
   ```

**Note**: The default configuration (10 workflows × 100 activities) should work on most systems, but you can easily reduce it further if needed.

## Advanced: Metrics Collection

If you want to collect metrics during the test:

```bash
# Export backlog metrics
while true; do
    echo "$(date +%s),$(curl -s http://localhost:9091/metrics | grep 'task_backlog_per_tl{' | grep -o 'task_backlog_per_tl{[^}]*} [0-9]*')" >> backlog_metrics.csv
    sleep 5
done
```

Then analyze with:
```bash
# Plot in gnuplot or import to spreadsheet
cat backlog_metrics.csv
```
