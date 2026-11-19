package cassandra

const (
	templateInsertAsyncWorkflowDLQMessageQuery = `INSERT INTO async_workflow_queue_dlq ` +
		`(shard_id, source_cluster, message_id, message_payload, message_payload_encoding, domain_name, workflow_id, request_id, request_type, reason, created_time) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	templateSelectAsyncWorkflowDLQMessagesQuery = `SELECT message_id, message_payload, message_payload_encoding, domain_name, workflow_id, request_id, request_type, reason, created_time ` +
		`FROM async_workflow_queue_dlq ` +
		`WHERE shard_id = ? AND source_cluster = ? AND message_id > ?`
	templateRangeDeleteAsyncWorkflowDLQMessagesQuery = `DELETE FROM async_workflow_queue_dlq ` +
		`WHERE shard_id = ? AND source_cluster = ? AND message_id <= ?`
)
