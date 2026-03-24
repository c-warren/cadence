# Ack Level Table Mismatch Fix

## Problem

Ack levels were being saved to the **wrong table**:
- Tasks: saved to `history_task_dlq` ✓
- Ack levels: saved to `history_task_dlq_range` ✗ (WRONG!)

**Root Cause:** When I updated the CQL templates, I accidentally used `history_task_dlq_range` instead of `history_task_dlq`.

## Symptoms

- Logs showed "Successfully updated ack level"
- But querying `history_task_dlq` showed no ack level rows
- Ack levels were going to a different table that doesn't exist in the current setup

## Fix

### Step 1: Add Missing Column to Table

The `history_task_dlq` table is missing the `ack_level_value` column. Add it:

```bash
cqlsh -k cadence_cluster0 -e "ALTER TABLE history_task_dlq ADD ack_level_value bigint;"
cqlsh -k cadence_cluster1 -e "ALTER TABLE history_task_dlq ADD ack_level_value bigint;"
cqlsh -k cadence_cluster2 -e "ALTER TABLE history_task_dlq ADD ack_level_value bigint;"
```

### Step 2: Fix CQL Templates

**File:** `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq_cql.go`

**Changed:**
```go
// Before (WRONG - using history_task_dlq_range):
templateGetAckLevelRange = `SELECT ack_level_value
    FROM history_task_dlq_range
    WHERE ...`

templateUpdateAckLevelRange = `INSERT INTO history_task_dlq_range (...)
    VALUES (...)`

// After (CORRECT - using history_task_dlq):
templateGetAckLevelRange = `SELECT ack_level_value
    FROM history_task_dlq
    WHERE ...`

templateUpdateAckLevelRange = `INSERT INTO history_task_dlq (
    shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
    task_type, task_id, visibility_timestamp, workflow_id, run_id,
    task_payload, encoding_type, version, ack_level_value, created_at
) VALUES (?, ?, ?, ?, ?, -1, toTimestamp(toDate(0)), '', '', null, '', ?, ?, ?)`
```

### Step 3: Fix Parameter Order

**File:** `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq.go`

Updated `UpdateAckLevel()` to pass parameters in correct order:

```go
query := d.session.Query(templateUpdateAckLevelRange,
    shardID,
    domainID,
    scope,
    name,
    taskType,
    0,           // version (not used for ack level rows)
    newAckLevel, // ack_level_value
    now,         // created_at
)
```

## How Ack Levels Work

**Ack level rows** are stored in the same table as tasks but with:
- `task_id = -1` (sentinel value)
- `visibility_timestamp = 1970-01-01 00:00:00` (epoch 0)
- `workflow_id = ''` (empty)
- `run_id = ''` (empty)
- `task_payload = null`
- `version = 0`
- `ack_level_value = <max task ID processed>` (the actual ack level)

**Reading ack levels:**
```sql
SELECT ack_level_value
FROM history_task_dlq
WHERE shard_id = ? AND domain_id = ? AND cluster_attribute_scope = ?
  AND cluster_attribute_name = ? AND task_type = ? AND task_id = -1
```

**Updating ack levels:**
```sql
INSERT INTO history_task_dlq (...)
VALUES (?, ?, ?, ?, ?, -1, epoch_0, '', '', null, '', 0, <new_ack_level>, now)
```

Since `task_id = -1` is unique per partition + task_type, each INSERT will create or replace the ack level row.

## Verification

After applying the fixes:

**1. Add the column to all clusters:**
```bash
for cluster in cluster0 cluster1 cluster2; do
    cqlsh -k cadence_$cluster -e "ALTER TABLE history_task_dlq ADD ack_level_value bigint;"
done
```

**2. Rebuild and restart:**
```bash
make
./restart_cadence.sh
```

**3. Run failover:**
```bash
./cadence domain failover --active_clusters 'cluster.cluster0:cluster1,cluster.cluster2:cluster1'
```

**4. Check ack levels:**
```bash
cqlsh -k cadence_cluster1 -e "SELECT shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, task_id, ack_level_value FROM history_task_dlq WHERE task_id = -1 ALLOW FILTERING;"
```

**Expected output:**
```
 shard_id | domain_id | cluster_attribute_scope | cluster_attribute_name | task_type | task_id | ack_level_value
----------+-----------+-------------------------+------------------------+-----------+---------+-----------------
        0 | <uuid>    | cluster                 | cluster0               |         0 |      -1 |         1048598
        1 | <uuid>    | cluster                 | cluster0               |         0 |      -1 |         1048582
        2 | <uuid>    | cluster                 | cluster2               |         0 |      -1 |         1048607
```

You should see:
- ✅ One row per `(shard_id, domain_id, cluster_attr, task_type)` partition
- ✅ `task_id = -1` for all ack level rows
- ✅ `ack_level_value` matches the max task ID that was processed

## Files Modified

1. `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq_cql.go`
   - Changed `templateGetAckLevelRange` to query `history_task_dlq` instead of `history_task_dlq_range`
   - Changed `templateUpdateAckLevelRange` to insert into `history_task_dlq` instead of `history_task_dlq_range`

2. `common/persistence/nosql/nosqlplugin/cassandra/history_task_dlq.go`
   - Updated `UpdateAckLevel()` parameter order: added `version = 0` before `ack_level_value`
