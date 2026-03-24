# DLQ Cluster Attribute Fix

## Problem

When running domain failover with cluster attributes:
```bash
./cadence domain failover --active_clusters 'cluster.cluster0:cluster1,cluster.cluster1:cluster1,cluster.cluster2:cluster1'
```

The DLQ processor was being called with **empty cluster attributes** (`scope=""`, `name=""`), causing it to find 0 tasks:

```json
{"msg":"[DLQ] Executing query","value":"scope=, name=, task_type=0, ackLevel=-1"}
{"msg":"[DLQ] Query complete","value":"rowsScanned=0, tasksReturned=0"}
```

This meant that DLQ tasks with cluster attributes like `cluster.cluster0` and `cluster.cluster2` were never processed during failover.

## Root Cause

In `/service/history/engine/engineimpl/register_domain_failover_callback.go`, when active-active domains fail over, the code was calling:

```go
if len(failoverActiveActiveDomainIDs) > 0 {
    // Process DLQ and reschedule tasks for active-active domains
    for _, processor := range e.queueProcessors {
        processor.FailoverDomain(failoverActiveActiveDomainIDs)  // ❌ No cluster attributes!
    }
}
```

The `FailoverDomain` method only receives domain IDs, not the specific cluster attributes that are failing over. While the domain cache entry (`*cache.DomainCacheEntry`) contains the full `ActiveClusters` configuration with all cluster attributes, this information wasn't being extracted and passed to the DLQ processor.

## Solution

### Approach: Extract Cluster Attributes from Domain Cache

The domain failover callback already has access to `nextDomains []*cache.DomainCacheEntry`, which contains the full replication configuration including `ActiveClusters.AttributeScopes`. We extract the cluster attributes that are **active on the current cluster** and call `FailoverDomainWithClusterAttribute` for each one.

### Implementation

**File Modified:** `/service/history/engine/engineimpl/register_domain_failover_callback.go`

1. **Updated failover callback** to call new helper method:
```go
if len(failoverActiveActiveDomainIDs) > 0 {
    e.logger.Info("Active-Active Domain updated", tag.WorkflowDomainIDs(failoverActiveActiveDomainIDs))

    // For active-active domains, extract cluster attributes from domain config
    e.processActiveActiveDomainFailover(nextDomains, failoverActiveActiveDomainIDs)
}
```

2. **Added helper method** `processActiveActiveDomainFailover()`:
   - Iterates through all domains that are failing over
   - Extracts cluster attributes from `replicationConfig.ActiveClusters.AttributeScopes`
   - Filters to only cluster attributes where `ActiveClusterName == currentCluster`
   - Groups domains by cluster attribute
   - Calls `FailoverDomainWithClusterAttribute` for each cluster attribute

**Key Code:**
```go
func (e *historyEngineImpl) processActiveActiveDomainFailover(
    nextDomains []*cache.DomainCacheEntry,
    failoverActiveActiveDomainIDs map[string]struct{},
) {
    clusterAttrToDomains := make(map[string]map[string]struct{})

    for _, domain := range nextDomains {
        if _, ok := failoverActiveActiveDomainIDs[domain.GetInfo().ID]; !ok {
            continue
        }

        replicationConfig := domain.GetReplicationConfig()
        for scopeName, scope := range replicationConfig.ActiveClusters.AttributeScopes {
            for attrName, clusterInfo := range scope.ClusterAttributes {
                // Only process cluster attributes active on current cluster
                if clusterInfo.ActiveClusterName == e.currentClusterName {
                    key := scopeName + "." + attrName
                    if _, exists := clusterAttrToDomains[key]; !exists {
                        clusterAttrToDomains[key] = make(map[string]struct{})
                    }
                    clusterAttrToDomains[key][domain.GetInfo().ID] = struct{}{}
                }
            }
        }
    }

    // Call FailoverDomainWithClusterAttribute for each cluster attribute
    for clusterAttrKey, domainIDs := range clusterAttrToDomains {
        parts := strings.SplitN(clusterAttrKey, ".", 2)
        scope, name := parts[0], parts[1]

        for _, processor := range e.queueProcessors {
            if qaProcessor, ok := processor.(interface {
                FailoverDomainWithClusterAttribute(domainIDs map[string]struct{}, clusterAttribute interface{})
            }); ok {
                clusterAttr := struct {
                    Scope string
                    Name  string
                }{Scope: scope, Name: name}
                qaProcessor.FailoverDomainWithClusterAttribute(domainIDs, &clusterAttr)
            }
        }
    }
}
```

**File Modified:** `/service/history/queuev2/queue_base.go`

Reverted the hardcoded approach - `FailoverDomain()` now only handles traditional active-passive domains:
```go
func (q *queueBase) FailoverDomain(domainIDs map[string]struct{}) {
    // For traditional active-passive domains, process DLQ without cluster attributes
    // Active-active domains should call FailoverDomainWithClusterAttribute instead
    q.processDLQForFailover(context.Background(), domainIDs, nil)

    // Reschedule tasks
    q.rescheduler.RescheduleDomains(domainIDs)
}
```

## Expected Behavior After Fix

For the command:
```bash
./cadence domain failover --active_clusters 'cluster.cluster0:cluster1,cluster.cluster1:cluster1,cluster.cluster2:cluster1'
```

You should see logs like:
```json
{"msg":"Active-active domain cluster attribute active on current cluster","wf-domain-id":"...","scope":"cluster","name":"cluster0","active_cluster":"cluster1"}
{"msg":"Active-active domain cluster attribute active on current cluster","wf-domain-id":"...","scope":"cluster","name":"cluster1","active_cluster":"cluster1"}
{"msg":"Active-active domain cluster attribute active on current cluster","wf-domain-id":"...","scope":"cluster","name":"cluster2","active_cluster":"cluster1"}
{"msg":"Processing DLQ for active-active cluster attribute","scope":"cluster","name":"cluster0","domain_count":1}
{"msg":"Processing DLQ tasks for failover","shard-id":0,"wf-domain-id":"...","value":"cluster","value":"cluster0"}
{"msg":"[DLQ] Executing query","value":"scope=cluster, name=cluster0, task_type=0, ackLevel=-1"}
{"msg":"[DLQ] Scanned row","task-id":1048835}
{"msg":"DLQ task executed successfully","task-id":1048835}
```

This means:
- ✅ Cluster attributes are extracted from domain configuration
- ✅ DLQ is queried with the correct cluster attributes (cluster0, cluster1, cluster2)
- ✅ Tasks are processed and executed during failover

## Verification

```bash
# 1. Rebuild and restart
make
./start_cadence.sh

# 2. Run failover command
./cadence domain failover --active_clusters 'cluster.cluster0:cluster1,cluster.cluster2:cluster1'

# 3. Check logs - should see:
#    - "Active-active domain cluster attribute active on current cluster"
#    - "Processing DLQ for active-active cluster attribute" with scope and name
#    - "DLQ task executed successfully" for each processed task

# 4. Query DLQ to verify tasks were deleted
./query_dlq.sh --verbose
# Should show fewer tasks (or none) for cluster0 and cluster2
```

## Why This Approach is Correct

1. **Uses Existing Data**: The domain cache entry already contains all cluster attribute information via `ActiveClusters.AttributeScopes`
2. **No Hardcoding**: Dynamically extracts cluster attributes from the domain configuration
3. **Accurate**: Only processes cluster attributes that are active on the current cluster
4. **Efficient**: Groups domains by cluster attribute to minimize DLQ queries
5. **Backward Compatible**: Active-passive domains continue using `FailoverDomain()`

## Files Modified

1. `/service/history/engine/engineimpl/register_domain_failover_callback.go`
   - Updated `domainChangeCB()` to call `processActiveActiveDomainFailover()`
   - Added `processActiveActiveDomainFailover()` to extract and process cluster attributes
   - Added imports for `fmt`, `strings`, and `queuev2`
   - **CRITICAL FIX**: Use `queuev2.ClusterAttributeKey` type and `queuev2.Queue` interface for type assertion
     - Before: Anonymous struct + anonymous interface (FAILED)
     - After: Proper types (WORKS)

2. `/service/history/queuev2/queue_base.go`
   - Simplified `FailoverDomain()` to only handle active-passive domains
   - Removed hardcoded cluster attribute approach

## Common Issues and Troubleshooting

### "Processor does not support cluster attribute failover"
This error occurs if:
- Type assertion uses wrong type (must be `queuev2.Queue`)
- Processor is a legacy v1 processor (doesn't implement `queuev2.Queue`)

**Solution:** Make sure to use the correct type assertion:
```go
if qv2Processor, ok := processor.(queuev2.Queue); ok {
    qv2Processor.FailoverDomainWithClusterAttribute(domainIDs, clusterAttr)
}
```

### Empty cluster attributes ("scope=, name=")
This occurs if:
- Type assertion fails and falls back to `FailoverDomain()`
- Cluster attributes are not extracted from domain config

**Solution:** Check that:
1. Domain has `ActiveClusters.AttributeScopes` configured
2. Type assertion succeeds (see above)
3. Logs show "Active-active domain cluster attribute active on current cluster"
