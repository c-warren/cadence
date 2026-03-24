// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

const (
	// Table names for DLQ comparison
	tableHistoryTaskDLQPoint = "history_task_dlq_point"
	tableHistoryTaskDLQ      = "history_task_dlq"
)

const (
	// Point-Delete queries
	templateEnqueueStandbyTaskPoint = `INSERT INTO history_task_dlq_point (
		shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
		task_type, visibility_timestamp, task_id, workflow_id, run_id,
		task_payload, encoding_type, version, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateReadStandbyTasksPoint = `SELECT
		shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
		task_type, visibility_timestamp, task_id, workflow_id, run_id,
		task_payload, encoding_type, version, created_at
		FROM history_task_dlq_point
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND visibility_timestamp >= ?
		LIMIT ?`

	templateDeleteStandbyTaskPoint = `DELETE FROM history_task_dlq_point
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND visibility_timestamp = ?
		AND task_id = ?`

	templateGetStandbyTaskDLQSizePoint = `SELECT COUNT(*) as count
		FROM history_task_dlq_point
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?`

	// Range-Delete queries
	templateEnqueueStandbyTaskRange = `INSERT INTO history_task_dlq (
		shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
		task_type, task_id, visibility_timestamp, workflow_id, run_id,
		task_payload, encoding_type, version, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateReadStandbyTasksRange = `SELECT
		shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
		task_type, task_id, visibility_timestamp, workflow_id, run_id,
		task_payload, encoding_type, version, created_at
		FROM history_task_dlq
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND task_id > ?
		LIMIT ?`

	templateGetAckLevelRange = `SELECT ack_level_value
		FROM history_task_dlq
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND task_id = -1`

	templateUpdateAckLevelRange = `INSERT INTO history_task_dlq (
		shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name,
		task_type, task_id, visibility_timestamp, workflow_id, run_id,
		task_payload, encoding_type, version, ack_level_value, created_at
	) VALUES (?, ?, ?, ?, ?, -1, toTimestamp(toDate(0)), '', '', null, '', ?, ?, ?)`

	templateGetStandbyTaskDLQSizeRange = `SELECT COUNT(*) as count
		FROM history_task_dlq
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND task_id > -1`

	templateRangeDeleteTasksRange = `DELETE FROM history_task_dlq
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND task_id >= 0
		AND task_id <= ?`

	templateDeleteStandbyTaskRange = `DELETE FROM history_task_dlq
		WHERE shard_id = ?
		AND domain_id = ?
		AND cluster_attribute_scope = ?
		AND cluster_attribute_name = ?
		AND task_type = ?
		AND task_id = ?`
)
