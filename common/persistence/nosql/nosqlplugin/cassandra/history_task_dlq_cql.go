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
	templateInsertHistoryDLQTask = `INSERT INTO history_task_dlq (` +
		`shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, ` +
		`task_type, visibility_ts, task_id, workflow_id, run_id, ` +
		`task_payload, encoding_type, version, created_at` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	// Tuple comparison (visibility_ts, task_id) > (?, ?) AND (visibility_ts, task_id) <= (?, ?)
	// provides correct ordering across the composite key for both timer and immediate tasks.
	templateSelectHistoryDLQTasksOrderByKey = `SELECT ` +
		`task_type, visibility_ts, task_id, domain_id, workflow_id, run_id, ` +
		`task_payload, encoding_type, version, created_at ` +
		`FROM history_task_dlq ` +
		`WHERE shard_id = ? AND domain_id = ? ` +
		`AND cluster_attribute_scope = ? AND cluster_attribute_name = ? ` +
		`AND task_type = ? ` +
		`AND (visibility_ts, task_id) > (?, ?) AND (visibility_ts, task_id) <= (?, ?) ` +
		`LIMIT ?`

	// Single range tombstone: deletes all rows with visibility_ts strictly before the boundary.
	templateRangeDeleteHistoryDLQTasksBefore = `DELETE FROM history_task_dlq ` +
		`WHERE shard_id = ? AND domain_id = ? ` +
		`AND cluster_attribute_scope = ? AND cluster_attribute_name = ? ` +
		`AND task_type = ? AND visibility_ts < ?`

	// Single range tombstone: deletes rows at the boundary timestamp up to the ack task_id.
	// Called after templateRangeDeleteHistoryDLQTasksBefore to cleanly delete up to a
	// (visibility_ts, task_id) boundary using exactly two range tombstones.
	templateRangeDeleteHistoryDLQTasksAtTS = `DELETE FROM history_task_dlq ` +
		`WHERE shard_id = ? AND domain_id = ? ` +
		`AND cluster_attribute_scope = ? AND cluster_attribute_name = ? ` +
		`AND task_type = ? AND visibility_ts = ? AND task_id <= ?`

	templateSelectHistoryDLQAckLevels = `SELECT ` +
		`domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, ` +
		`ack_level_visibility_ts, ack_level_task_id, last_updated_at ` +
		`FROM history_task_dlq_ack_level ` +
		`WHERE shard_id = ?`

	templateSelectHistoryDLQAckLevelsByDomain = `SELECT ` +
		`domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, ` +
		`ack_level_visibility_ts, ack_level_task_id, last_updated_at ` +
		`FROM history_task_dlq_ack_level ` +
		`WHERE shard_id = ? AND domain_id = ?`

	templateSelectHistoryDLQAckLevelsByDomainAndClusterAttribute = `SELECT ` +
		`domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, ` +
		`ack_level_visibility_ts, ack_level_task_id, last_updated_at ` +
		`FROM history_task_dlq_ack_level ` +
		`WHERE shard_id = ? AND domain_id = ? AND cluster_attribute_scope = ? AND cluster_attribute_name = ?`

	templateUpsertHistoryDLQAckLevel = `INSERT INTO history_task_dlq_ack_level (` +
		`shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, ` +
		`task_type, ack_level_visibility_ts, ack_level_task_id, last_updated_at` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)
