package cassandra

const (
	templateTaskListType = `{` +
		`domain_id: ?, ` +
		`name: ?, ` +
		`type: ?, ` +
		`ack_level: ?, ` +
		`kind: ?, ` +
		`last_updated: ?, ` +
		`adaptive_partition_config: ? ` +
		`}`

	templateTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`schedule_id: ?,` +
		`created_time: ?, ` +
		`partition_config: ? ` +
		`}`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task, created_time) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `, ?)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task, created_time) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `, ?) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task, TTL(task) AS ttl ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetTasksCountQuery = `SELECT count(1) as count ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? `

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE domain_id = ? ` +
		`AND task_list_name = ? ` +
		`AND task_list_type = ? ` +
		`AND type = ? ` +
		`AND task_id > ? ` +
		`AND task_id <= ? `

	templateGetTaskList = `SELECT ` +
		`range_id, ` +
		`task_list ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskListQuery = `INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list, ` +
		`created_time ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ` + templateTaskListType + `, ?) IF NOT EXISTS`

	templateUpdateTaskListQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_list = ` + templateTaskListType + " , last_updated_time = ? " +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListQueryWithTTLPart1 = ` INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`created_time ` +
		`) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?`

	templateUpdateTaskListQueryWithTTLPart2 = `UPDATE tasks USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_list = ` + templateTaskListType + " , last_updated_time = ? " +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListRangeIDQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`last_updated_time = ? ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateDeleteTaskListQuery = `DELETE FROM tasks ` +
		`WHERE domain_id = ? ` +
		`AND task_list_name = ? ` +
		`AND task_list_type = ? ` +
		`AND type = ? ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
)
