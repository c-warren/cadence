-- DLQ Simplified Schema Migration v0.46
-- This migration creates a simplified DLQ table that enables efficient range deletes.
--
-- Key improvements over v0.45:
-- 1. Removes row_type from clustering key (use task_id = -1 for ack level rows instead)
-- 2. Moves visibility_timestamp to data column (avoids epoch=0 ordering issues)
-- 3. Clustering key is (task_type, task_id) - enables true range deletes
-- 4. Range deletes create only ONE tombstone per delete operation
-- 5. More efficient queries using real domain/cluster values

CREATE TABLE history_task_dlq (
    shard_id                  int,
    domain_id                 text,
    cluster_attribute_scope   text,
    cluster_attribute_name    text,
    task_type                 int,              -- Transfer, Timer, etc.
    task_id                   bigint,           -- Task ID; -1 for ack level rows
    visibility_timestamp      timestamp,        -- When task should be visible (data column, not clustering)
    workflow_id               text,
    run_id                    text,
    task_payload              blob,             -- Serialized task data
    encoding_type             text,             -- "json", "thrift", "proto", "thriftrw"
    version                   bigint,           -- Task version
    created_at                timestamp,        -- When added to DLQ
    PRIMARY KEY ((shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name), task_type, task_id)
) WITH CLUSTERING ORDER BY (task_type ASC, task_id ASC)
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };