-- Dead-letter queue for poison / corrupt async workflow requests.
-- (source_cluster, message_id) is the source message's key, not a fresh sequence: it is already
-- unique within the shard, so a retried DLQ write is an overwrite rather than a duplicate, and the
-- table needs no sequence read, no control row and no fence of its own.
CREATE TABLE async_workflow_queue_dlq (
  shard_id                 int,
  source_cluster           text,      -- cluster that minted message_id
  message_id               bigint,    -- the source message's id
  message_payload          blob,      -- ThriftRW-encoded sqlblobs.AsyncRequestMessage (the whole envelope)
  message_payload_encoding text,      -- encoding of the envelope, not of its inner payload
  domain_name              text,
  workflow_id              text,
  request_id               text,
  request_type             int,       -- 0 = start workflow, 1 = signal with start
  reason                   text,      -- why it was dead-lettered (decode error, validation error)
  created_time             timestamp, -- when the row entered the DLQ
  PRIMARY KEY ((shard_id), source_cluster, message_id)
) WITH CLUSTERING ORDER BY (source_cluster ASC, message_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };
