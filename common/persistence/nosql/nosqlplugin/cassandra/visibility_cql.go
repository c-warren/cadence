package cassandra

const (
	// /////////////// Open Executions /////////////////
	openExecutionsColumnsForSelect = " workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time "

	openExecutionsColumnsForInsert = "(domain_id, domain_partition, " + openExecutionsColumnsForSelect + ")"

	templateCreateWorkflowExecutionStartedWithTTL = `INSERT INTO open_executions ` +
		openExecutionsColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionStarted = `INSERT INTO open_executions` +
		openExecutionsColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateDeleteWorkflowExecutionStarted = `DELETE FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time = ? ` +
		`AND run_id = ?`

	templateGetOpenWorkflowExecutions = `SELECT ` + openExecutionsColumnsForSelect +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetOpenWorkflowExecutionsByType = `SELECT ` + openExecutionsColumnsForSelect +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetOpenWorkflowExecutionsByID = `SELECT ` + openExecutionsColumnsForSelect +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	// Intended for admin operations only
	templateGetOpenWorkflowExecutionsByRunID = `SELECT ` + openExecutionsColumnsForSelect +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND run_id = ? ` +
		`ALLOW FILTERING`

	// Intended for admin operations only
	templateDeleteVisibilityRecord = `DELETE FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`and domain_partition = ? ` +
		`and start_time = ? ` +
		`and run_id = ? `

	// /////////////// Closed Executions /////////////////
	closedExecutionColumnsForSelect = " workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time "

	closedExecutionColumnsForInsert = "(domain_id, domain_partition, " + closedExecutionColumnsForSelect + ")"

	templateCreateWorkflowExecutionClosedWithTTL = `INSERT INTO closed_executions ` +
		closedExecutionColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions ` +
		closedExecutionColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateWorkflowExecutionClosedWithTTLV2 = `INSERT INTO closed_executions_v2 ` +
		closedExecutionColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateCreateWorkflowExecutionClosedV2 = `INSERT INTO closed_executions_v2 ` +
		closedExecutionColumnsForInsert +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetClosedWorkflowExecutions = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? `

	templateGetClosedWorkflowExecutionsByType = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByID = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatus = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND start_time >= ? ` +
		`AND start_time <= ? ` +
		`AND status = ? `

	templateGetClosedWorkflowExecution = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? ALLOW FILTERING `

	templateGetClosedWorkflowExecutionsSortByCloseTime = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? `

	templateGetClosedWorkflowExecutionsByTypeSortByCloseTime = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_type_name = ? `

	templateGetClosedWorkflowExecutionsByIDSortByCloseTime = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND workflow_id = ? `

	templateGetClosedWorkflowExecutionsByStatusSortByClosedTime = `SELECT ` + closedExecutionColumnsForSelect +
		`FROM closed_executions_v2 ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND close_time >= ? ` +
		`AND close_time <= ? ` +
		`AND status = ? `
)
