package cassandra

const (
	templateEnqueueMessageQuery             = `INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQuery           = `SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery                = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? LIMIT ?`
	templateGetMessagesFromDLQQuery         = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateRangeDeleteMessagesBeforeQuery  = `DELETE FROM queue WHERE queue_type = ? and message_id < ?`
	templateRangeDeleteMessagesBetweenQuery = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery              = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`
	templateGetQueueMetadataQuery           = `SELECT cluster_ack_level, version FROM queue_metadata WHERE queue_type = ?`
	templateInsertQueueMetadataQuery        = `INSERT INTO queue_metadata (queue_type, cluster_ack_level, version, created_time) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueMetadataQuery        = `UPDATE queue_metadata SET cluster_ack_level = ?, version = ?, last_updated_time = ? WHERE queue_type = ? IF version = ?`
	templateGetQueueSizeQuery               = `SELECT COUNT(1) AS count FROM queue WHERE queue_type=?`
)
