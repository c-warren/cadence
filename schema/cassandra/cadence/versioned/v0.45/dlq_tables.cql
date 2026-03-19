-- DLQ Comparison Schema Migration v1.0
-- This migration creates two separate DLQ tables for performance comparison:
-- 1. history_task_dlq_point: Uses point-delete pattern (one DELETE per task)
-- 2. history_task_dlq_range: Uses range-delete pattern (ack-level advancement)

-- Point-Delete Table: Each task is individually deleted when processed
CREATE TABLE history_task_dlq_point (
    shard_id                  int,
    domain_id                 text,
    cluster_attribute_scope   text,
    cluster_attribute_name    text,
    task_type                 int,          -- Transfer, Timer, etc.
    visibility_timestamp      timestamp,    -- For PRIMARY KEY ordering
    task_id                   bigint,
    workflow_id               text,
    run_id                    text,
    task_payload              blob,         -- Serialized task data
    encoding_type             text,         -- "json", "thrift", "proto", "thriftrw"
    version                   bigint,       -- Task version
    created_at                timestamp,    -- When added to DLQ
    updated_at                timestamp,    -- Last update time
    PRIMARY KEY ((shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name), task_type, visibility_timestamp, task_id)
) WITH CLUSTERING ORDER BY (task_type ASC, visibility_timestamp ASC, task_id ASC)
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE TABLE history_task_dlq_range (
    shard_id                  int,
    domain_id                 text,
    cluster_attribute_scope   text,
    cluster_attribute_name    text,
    task_type                 int,          -- Transfer, Timer, etc.
    row_type                  int,          -- 0=task, 1=ack_level (MUST be in clustering key for filtering)
    visibility_timestamp      timestamp,    -- For PRIMARY KEY ordering
    task_id                   bigint,
    workflow_id               text,
    run_id                    text,
    task_payload              blob,         -- Serialized task data
    encoding_type             text,         -- "json", "thrift", "proto", "thriftrw"
    version                   bigint,       -- Task version
    ack_level_value           bigint,       -- Ack level value for ack_level rows (task_id=-1)
    created_at                timestamp,    -- When added to DLQ
    updated_at                timestamp,    -- Last update time
    PRIMARY KEY ((shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name), task_type, row_type, visibility_timestamp, task_id)
) WITH CLUSTERING ORDER BY (task_type ASC, row_type ASC, visibility_timestamp ASC, task_id ASC)
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
