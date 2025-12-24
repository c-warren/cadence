# Failover + Node Death Testing Guide

This guide explains how to reproduce and test the issue where **failover + node death results in tasks not getting processed**, evidenced by:
- Increasing task backlog (task_backlog_per_tl metric)
- "Async task dispatch timed out" logs
- Tasks stuck and not being dispatched to workers

## Quick Start

### 1. Start the Simulation

```bash
# Build cadence if not already built
make cadence-server

# Run the failover stress test scenario
./simulation/replication/run.sh --scenario activeactive_failover_stress
```

This will:
- Start a 2-cluster setup (cluster0 and cluster1)
- Create an active-active domain with cluster attributes
- Start 10 long-running workflows (5 in each cluster region)
- Each workflow executes 100 activities in a loop

### 2. Perform Manual Failovers

In a **separate terminal**, run the failover loop script:

```bash
# Failover every 30 seconds (default)
./simulation/replication/failover_loop.sh test-domain-failover-stress 30

# Or customize the interval (e.g., every 15 seconds for more aggressive testing)
./simulation/replication/failover_loop.sh test-domain-failover-stress 15
```

The script will alternate the active cluster between cluster0 and cluster1 every 30 seconds.

### 3. Monitor for Issues

#### Watch Logs for Async Dispatch Timeouts

In another terminal:

```bash
# Watch cluster0 logs
docker logs -f cadence-cluster0 2>&1 | grep -i "async task dispatch"

# Watch cluster1 logs
docker logs -f cadence-cluster1 2>&1 | grep -i "async task dispatch"
```

#### Monitor Backlog Metrics

If you have Prometheus/metrics configured:

```bash
# Query task backlog gauge
curl -s 'http://localhost:9090/api/v1/query?query=task_backlog_per_tl' | jq

# Or watch it continuously
watch -n 5 'curl -s "http://localhost:9090/api/v1/query?query=task_backlog_per_tl" | jq ".data.result[] | {domain: .metric.domain, taskList: .metric.taskList, backlog: .value[1]}"'
```

#### Check Workflow Progress

```bash
# List running workflows
docker exec cadence-cluster0 cadence --do test-domain-failover-stress workflow list

# Observe specific workflow
docker exec cadence-cluster0 cadence --do test-domain-failover-stress workflow observe -w stress-wf-1
```

## Advanced Testing: Simulating Node Death

To simulate a more realistic scenario where a node dies during failover:

### Kill Matching Service During Failover

```bash
# In a separate terminal, run this loop to periodically kill matching service
while true; do
    sleep 45  # Offset from failover timing
    echo "[$(date)] Killing matching service in cluster0..."
    docker exec cadence-cluster0 pkill -f matching
    sleep 90
done
```

### Kill Entire Node

```bash
# During failover, stop the container entirely
docker stop cadence-cluster0

# Wait a few seconds
sleep 10

# Restart it
docker start cadence-cluster0
```

## What to Look For

### Signs of the Bug

1. **Increasing Backlog**
   - `task_backlog_per_tl` metric increasing over time
   - Backlog not decreasing even when workers are available

2. **Timeout Logs**
   ```
   "Async task dispatch timed out" error="context deadline exceeded"
   ```

3. **Stuck Workflows**
   - Workflows appear running but no progress
   - Activities not being dispatched
   - Decision tasks accumulating

4. **Matching Service Issues**
   - Task lists showing tasks but no pollers receiving them
   - Forward errors in logs
   - Isolation group issues

### Expected Behavior (After Fix)

- Backlog should remain low and stable
- No "async task dispatch timed out" errors
- Workflows complete successfully despite failovers
- Tasks get reassigned promptly after failover

## Customizing the Test

### Increase Stress Level

Edit `simulation/replication/testdata/replication_simulation_activeactive_failover_stress.yaml`:

```yaml
# Add more workflows
- op: start_workflow
  at: 0s
  workflowID: stress-wf-11
  workflowType: activity-loop-workflow
  cluster: cluster0
  domain: test-domain-failover-stress
  workflowExecutionStartToCloseTimeout: 600s
  activityCount: 200  # More activities = more stress
  activeClusterSelectionPolicy:
    clusterAttribute:
      scope: region
      name: region0
```

### Change Failover Frequency

```bash
# Aggressive failover every 10 seconds
./simulation/replication/failover_loop.sh test-domain-failover-stress 10

# Slower failover every 60 seconds
./simulation/replication/failover_loop.sh test-domain-failover-stress 60
```

### Test with Your Async Match Changes

After making your sync→async match changes:

1. Rebuild: `make cadence-server`
2. Rebuild docker images: `docker/buildkite/docker-compose-local-build.sh`
3. Run the test again
4. Compare backlog behavior before/after your changes

## Debugging Tips

### Access Container Shells

```bash
# Cluster0
docker exec -it cadence-cluster0 /bin/bash

# Cluster1
docker exec -it cadence-cluster1 /bin/bash
```

### Check Matching Service Metrics

```bash
# From inside container
curl -s http://localhost:9091/metrics | grep -i "task_backlog\|async_match\|sync_match"
```

### Check Task List State

```bash
docker exec cadence-cluster0 cadence --do test-domain-failover-stress \
  tasklist describe --tl canary-task-queue --tlt activity
```

### Enable Verbose Logging

Add to `config/dynamicconfig/replication_simulation_activeactive_failover_stress.yml`:

```yaml
matching.longPollExpirationInterval:
  - value: "10s"
matching.asyncTaskDispatchTimeout:
  - value: "30s"  # Increase to reduce timeout errors
system.enableDebugMode:
  - value: true
```

## Clean Up

```bash
# Stop the simulation
# Press Ctrl+C in the terminal running the simulation

# Stop the failover loop
# Press Ctrl+C in the terminal running failover_loop.sh

# Clean up docker containers
docker compose -f docker/buildkite/docker-compose-local.yml down
```

## Troubleshooting

### "Domain not found" error

The simulation framework should auto-create the domain. If not:

```bash
docker exec cadence-cluster0 cadence --do test-domain-failover-stress domain register \
  --active_cluster cluster0 \
  --clusters cluster0 cluster1 \
  --global_domain true \
  --is_global_domain true \
  --cluster_data '{"region0": "cluster0", "region1": "cluster1"}'
```

### No workflows starting

Check worker logs:

```bash
docker logs cadence-cluster0-worker
docker logs cadence-cluster1-worker
```

### Container networking issues

Verify containers can reach each other:

```bash
docker exec cadence-cluster0 ping cadence-cluster1
```

## Related Files

- Test scenario: `simulation/replication/testdata/replication_simulation_activeactive_failover_stress.yaml`
- Failover script: `simulation/replication/failover_loop.sh`
- Workflow definitions: `simulation/replication/workflows/activityloop/workflow.go`
