# Integration Test Setup - Complete Plan

## 1. Database Setup (Cassandra)

```bash
# Start Cassandra
docker compose -f docker/dev/cassandra.yml up

# Install XDC schema
until make install-schema-xdc; do sleep 1; done
```

## 2. Dynamic Config Setup

Create **`config/dynamicconfig/development.yaml`** (or update if exists):

```yaml
# Enable TransferQueueV2 (REQUIRED for DLQ)
history.enableTransferQueueV2:
  - value: true
    constraints: {}

# SHORT discard delay for testing (default is 15 minutes)
history.StandbyTaskMissingEventsDiscardDelay:
  - value: "10s"
    constraints: {}

# Reduce resend delay for faster testing
history.StandbyTaskMissingEventsResendDelay:
  - value: "2s"
    constraints: {}
```

Make sure your cluster configs reference this dynamicconfig file. Check **`config/development_xdc_cluster0.yaml`** (and cluster1, cluster2):

```yaml
dynamicconfig:
  client: file
  filepath: config/dynamicconfig/development.yaml  # Ensure this path is correct
```

## 3. Start Multi-Cluster Instance

```bash
# Start all three clusters
./cadence-server --config config --zone xdc --env development_xdc_cluster0 start
./cadence-server --config config --zone xdc --env development_xdc_cluster1 start
./cadence-server --config config --zone xdc --env development_xdc_cluster2 start
```

## 4. Setup Active-Active Domain

```bash
# Register active-active domain
./cadence --transport grpc --ad localhost:7833 --domain dlq-test domain register \
  --gd true \
  --clusters cluster0,cluster1,cluster2

# Wait for domain to propagate
sleep 30

# Set cluster attribute (location.london active in cluster0)
./cadence --transport grpc --ad localhost:7833 --domain dlq-test domain update \
  --active_clusters 'location.london:cluster0'
```

## 5. Test DLQ Flow

### Step A: Start Worker (in cluster0)

In a new terminal:
```bash
# You'll need a test workflow/worker running
# Example: Use cadence-bench or a simple test worker
# The worker should be registered on cluster0:7833 for tasklist "test-tl"
```

### Step B: Create Workflow (triggers transfer task)

```bash
# Start a workflow in cluster0 (active for location.london)
./cadence --transport grpc --ad localhost:7833 --domain dlq-test workflow start \
  --tasklist test-tl \
  --workflow_type MyWorkflow \
  --workflow_id test-dlq-wf-1 \
  --execution_timeout 300 \
  --input '{}'
```

### Step C: Monitor Standby Cluster (cluster1)

**Watch cluster1 logs** for DLQ enqueue after ~10 seconds:

```bash
# Expected log output (cluster1 - standby):
# After 10s of retrying every 2s:

Enqueuing standby task to DLQ due to task being pending for too long
{"wf-id": "test-dlq-wf-1", "wf-domain-id": "...", "queue-task-id": X, ...}
```

### Step D: Trigger Failover

```bash
# Failover location.london from cluster0 → cluster1
./cadence --transport grpc --ad localhost:7833 --domain dlq-test domain update \
  --active_clusters 'location.london:cluster1'
```

### Step E: Verify DLQ Processing

**Watch cluster1 logs** immediately after failover:

```bash
# Expected log output (cluster1 - now active):

Processing DLQ tasks for failover
{"shard-id": X, "wf-domain-id": "...", "value": "location.london"}

DLQ task deserialized successfully, would execute
{"queue-task-id": X, "wf-domain-id": "..."}

Completed processing DLQ tasks for failover
{"shard-id": X, "wf-domain-id": "..."}
```

## 6. Key Log Patterns to Verify

### ✅ Task retrying on standby (cluster1):
```
Standby task missing events, retrying
```

### ✅ Task enqueued to DLQ (cluster1 after 10s):
```
Enqueuing standby task to DLQ due to task being pending for too long
```

### ✅ DLQ processing triggered by failover (cluster1):
```
Processing DLQ tasks for failover
DLQ task deserialized successfully, would execute
Completed processing DLQ tasks for failover
```

### ✅ No duplicate processing:
After failover completes, the same task ID should not appear in logs again.

## 7. Verification Checklist

- [ ] Dynamic config loaded (check startup logs for `enableTransferQueueV2: true`)
- [ ] All 3 clusters started successfully
- [ ] Domain registered with global flag
- [ ] Cluster attribute set (`location.london:cluster0`)
- [ ] Workflow started (creates transfer task)
- [ ] Standby cluster (cluster1) retries task every 2s for 10s
- [ ] Task enqueued to DLQ after 10s
- [ ] Failover command succeeds
- [ ] DLQ processor triggered on cluster1
- [ ] Task deserialized and "executed" (logged)
- [ ] Task removed from DLQ (no more logs for same task ID)

## 8. Troubleshooting

### If DLQ not triggered:
```bash
# Check dynamic config is loaded
grep "enableTransferQueueV2" <cluster-log-file>

# Should see: "enableTransferQueueV2: true"
# If not, config file path is wrong or not loaded
```

### If tasks not discarded:
```bash
# Check discard delay is applied
grep "StandbyTaskMissingEventsDiscardDelay" <cluster-log-file>

# Should see: 10s (not default 15m)
```

### If DLQ not processing on failover:
```bash
# Check failover notification
grep "FailoverDomainWithClusterAttribute" <cluster-log-file>

# If missing, failover callback may not be wired up
```

## 9. Quick Test Summary

**Timeline:**
- T+0s: Start workflow on cluster0 (active)
- T+0s: cluster1 (standby) starts receiving replication events
- T+0-10s: cluster1 retries task every 2s (missing events)
- T+10s: cluster1 enqueues task to DLQ
- T+15s: Trigger failover (location.london → cluster1)
- T+15s: cluster1 processes DLQ, deserializes and "executes" task
- T+16s: Task removed from DLQ, workflow can proceed

**Expected Result:** Task is NOT lost even though it was discarded on standby before failover.

---

## Notes

- **DLQ is in-memory** - survives only for server lifetime
- **Only works with QueueV2** - must set `enableTransferQueueV2: true`
- **POC doesn't actually execute** - just deserializes and logs (production will execute)
- **Cassandra is fine** - DLQ doesn't touch database in this POC
