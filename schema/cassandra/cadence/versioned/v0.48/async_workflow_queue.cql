-- Cassandra/history-backed async workflow queue.
-- async_workflow_queue holds async workflow requests (start / signal-with-start) awaiting execution,
-- grouped into logical queues (queue_name) and sharded by history shard id. message_id is a monotonic
-- per-shard sequence written by the single shard owner, giving FIFO ordering without conditional writes.
CREATE TABLE async_workflow_queue (
  queue_name      text,
  shard_id        int,
  message_id      bigint,
  message_payload blob,
  encoding        text,
  partition_key   text,
  created_time    timestamp,
  PRIMARY KEY ((queue_name, shard_id), message_id)
) WITH CLUSTERING ORDER BY (message_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

-- Per-(queue_name, shard_id) consumption cursor (ack_level) with a version for CAS updates.
CREATE TABLE async_workflow_queue_metadata (
  queue_name        text,
  shard_id          int,
  ack_level         bigint,
  version           bigint,
  created_time      timestamp,
  last_updated_time timestamp,
  PRIMARY KEY ((queue_name, shard_id))
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

-- Dead-letter queue for poison / corrupt async workflow requests. Same shape as async_workflow_queue.
CREATE TABLE async_workflow_queue_dlq (
  queue_name      text,
  shard_id        int,
  message_id      bigint,
  message_payload blob,
  encoding        text,
  partition_key   text,
  created_time    timestamp,
  PRIMARY KEY ((queue_name, shard_id), message_id)
) WITH CLUSTERING ORDER BY (message_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };
