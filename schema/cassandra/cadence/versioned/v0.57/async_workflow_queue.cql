-- Per-history-shard FIFO queue of async workflow requests (StartWorkflowExecutionAsync and
-- SignalWithStartWorkflowExecutionAsync), replacing the Kafka hop.
--
-- One partition per shard. Inside it, type = 0 rows are messages, clustered by the cluster that
-- minted them and then by a monotonic message_id (the shard's task id sequence). type = 1 rows are
-- control rows, one per source_cluster, carrying that cluster's ack_level: every message of that
-- source with message_id <= ack_level is done and may be range-deleted.
--
-- Control rows use the negative sentinel message_id = -12345 (as the tasks table does) so a range
-- delete of acked messages can never sweep a control row; every read and delete also pins type.
CREATE TABLE async_workflow_queue (
  shard_id                 int,
  type                     int,       -- 0 = message row, 1 = control row
  source_cluster           text,      -- cluster that minted message_id
  message_id               bigint,    -- message rows: monotonic per (shard, source_cluster); control rows: -12345
  -- message rows (type = 0)
  message_payload          blob,      -- encoded request envelope
  message_payload_encoding text,      -- encoding of message_payload
  domain_name              text,
  workflow_id              text,
  request_id               text,
  request_type             int,       -- 0 = start workflow, 1 = signal with start
  -- control rows (type = 1)
  ack_level                bigint,
  created_time             timestamp,
  last_updated_time        timestamp,
  PRIMARY KEY ((shard_id), type, source_cluster, message_id)
) WITH CLUSTERING ORDER BY (type ASC, source_cluster ASC, message_id ASC)
  AND compaction = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };
