# DLQ Ack Level Row Proliferation - Implementation Summary

## Problem Fixed

The standby task DLQ was creating ~40 ack level rows instead of 1 row per partition. The root cause was using `task_id` both as part of the clustering key AND to store the ack level value, causing each update to create a new row instead of updating the existing one.

## Solution Implemented

### Phase 1: Schema ✅
**Status:** Already present in schema

The `history_task_dlq_range` table already includes the `ack_level_value` column:
- File: `schema/cassandra/cadence/versioned/v0.45/dlq_tables.cql`
- Line 42: `ack_level_value bigint`

### Phase 2: Update Code to Use ack_level_value Column ✅
**Status:** IMPLEMENTED

**Files Modified:**

1. **`common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq_cql.go`**
   - Updated `templateGetAckLevelRange` to SELECT from `ack_level_value` column
   - Updated `templateUpdateAckLevelRange` to INSERT into `history_task_dlq_range` with proper columns
   - Uses sentinel values: `row_type = 1`, `visibility_timestamp = toTimestamp(toDate(0))`, `task_id = -1`

2. **`common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq.go`**
   - `GetAckLevel()`: Now reads from `ack_level_value` column
   - `UpdateAckLevel()`: Now writes to `ack_level_value` column with correct parameters
   - `insertAckLevel()`: Reuses `UpdateAckLevel()` (safe with sentinel `task_id=-1`)

**Key Changes:**
- Ack level rows use: `task_id=-1`, `row_type=1`, `visibility_timestamp=0`
- Actual ack level value stored in `ack_level_value` column (not in `task_id` or `version`)
- This ensures only ONE ack level row per `(shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, task_type)` partition

### Phase 3: Fix DeleteStandbyTask Loop ✅
**Status:** Already correct in current code

The `DeleteStandbyTask()` implementation already only updates the ack level for the specific task type from the request. No loop through all task types exists.

### Phase 5: Populate Cluster Attributes from Workflow Metadata ✅
**Status:** IMPLEMENTED

**Files Modified:**

1. **`service/history/task/standby_task_util.go`**
   - Added constant: `ClusterAttributeScopeDefault = "cluster"` for active-standby domains
   - Added structs: `transferPostActionInfo`, `postActionInfoWithCluster` to carry cluster attributes
   - Updated `standbyTaskPostActionEnqueueToDLQ()` to extract cluster attributes from postActionInfo
   - Added `extractClusterAttributes()` helper to extract from wrapped postActionInfo
   - Added `unwrapPostActionInfo()` helper to extract original info from wrapper
   - Updated `standbyTaskPostActionNoOp()` to unwrap postActionInfo

2. **`service/history/task/transfer_standby_task_executor.go`**
   - Updated `getDiscardTaskFn()` to remove hardcoded cluster attribute parameters
   - Added `getClusterAttributesFromMutableState()` to extract cluster attributes from execution info
   - Updated `processTransfer()` to:
     - Extract cluster attributes from mutable state BEFORE releasing it
     - Wrap actionFn result with cluster attributes in `postActionInfoWithCluster`
     - Pass wrapped info to postActionFn
   - Updated `pushActivity()`, `pushDecision()`, `fetchHistoryFromRemote()` to unwrap postActionInfo

**Cluster Attribute Extraction Logic:**
1. Check if workflow has `ActiveClusterSelectionPolicy.ClusterAttribute`
2. If yes: use `attr.GetScope()` and `attr.GetName()`
3. If no (traditional active-standby): use `ClusterAttributeScopeDefault` ("cluster") and current cluster name

**Error Handling:**
- If cluster attributes cannot be extracted, ERROR is returned (not silent fallback)
- This ensures DLQ entries are always correctly partitioned

## Verification Steps

### Step 1: Clean Existing Test Data
```bash
# Delete all ack level rows from test database
docker exec dev-cassandra-1 cqlsh -k cadence_cluster1 -e \
  "DELETE FROM history_task_dlq_range WHERE row_type = 1;"
```

### Step 2: Test with New Code
```bash
# Enqueue and process some tasks
# Delete a few tasks from DLQ to trigger ack level updates

# Verify only ONE ack level row per partition
./query_dlq.sh --count --ack-levels-only
```

**Expected Result:**
- For 1 domain, 1 cluster attr, 1 shard, 1 task type: exactly 1 ack level row
- Ack level row should have: `task_id = -1`, `row_type = 1`, `visibility_timestamp = 1970-01-01`
- Processing new tasks should UPDATE existing ack level row, not create new ones

### Step 3: Verify Cluster Attributes
```bash
# Check that cluster attributes are populated
./query_dlq.sh --verbose

# For active-active domains:
# - cluster_attribute_scope: from workflow's ActiveClusterSelectionPolicy
# - cluster_attribute_name: from workflow's ActiveClusterSelectionPolicy

# For active-standby domains:
# - cluster_attribute_scope: "cluster"
# - cluster_attribute_name: current cluster name (e.g., "cluster1")

# Verify NO rows have empty cluster attributes:
docker exec dev-cassandra-1 cqlsh -k cadence_cluster1 -e \
  "SELECT count(*) FROM history_task_dlq_range WHERE cluster_attribute_scope = '' ALLOW FILTERING;"
# Expected: 0 rows
```

### Step 4: Integration Tests
```bash
# Run existing DLQ tests to ensure functionality still works
go test ./common/persistence/nosql/nosqlplugin/cassandra -run TestDLQ
```

## Success Criteria

✅ **Fix Complete When:**
1. Only 1 ack level row exists per `(shard, domain, cluster_attr, task_type)` partition
2. All ack level rows have `task_id = -1`, `row_type = 1`, `visibility_timestamp = 0`
3. Processing new tasks updates existing ack level row instead of creating new ones
4. All DLQ tasks have valid cluster attributes (no empty strings)
5. Cluster attributes correctly reflect:
   - Active-active: from workflow's `ActiveClusterSelectionPolicy.ClusterAttribute`
   - Active-standby: scope="cluster", name=current cluster name
6. All existing tests pass

## Files Modified

1. `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq_cql.go`
2. `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq.go`
3. `service/history/task/standby_task_util.go`
4. `service/history/task/transfer_standby_task_executor.go`

## What Was NOT Changed

- Schema definition (already correct)
- `DeleteStandbyTask()` implementation (already correct - no loop)
- Persistence interfaces (already had necessary fields)
- Timer task executor (would need similar changes, but not in scope for this POC)
