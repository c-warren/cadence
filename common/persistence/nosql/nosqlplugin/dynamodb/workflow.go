package dynamodb

import (
	"context"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

var _ nosqlplugin.WorkflowCRUD = (*ddb)(nil)

func (db *ddb) InsertWorkflowExecutionWithTasks(
	ctx context.Context,
	requests *nosqlplugin.WorkflowRequestsWriteRequest,
	currentWorkflowRequest *nosqlplugin.CurrentWorkflowWriteRequest,
	execution *nosqlplugin.WorkflowExecutionRequest,
	tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask,
	activeClusterSelectionPolicyRow *nosqlplugin.ActiveClusterSelectionPolicyRow,
	shardCondition *nosqlplugin.ShardCondition,
) error {
	panic("TODO")
}

func (db *ddb) UpdateWorkflowExecutionWithTasks(
	ctx context.Context,
	requests *nosqlplugin.WorkflowRequestsWriteRequest,
	currentWorkflowRequest *nosqlplugin.CurrentWorkflowWriteRequest,
	mutatedExecution *nosqlplugin.WorkflowExecutionRequest,
	insertedExecution *nosqlplugin.WorkflowExecutionRequest,
	activeClusterSelectionPolicyRow *nosqlplugin.ActiveClusterSelectionPolicyRow,
	resetExecution *nosqlplugin.WorkflowExecutionRequest,
	tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask,
	shardCondition *nosqlplugin.ShardCondition,
) error {
	panic("TODO")
}

func (db *ddb) SelectCurrentWorkflow(ctx context.Context, shardID int, domainID, workflowID string) (*nosqlplugin.CurrentWorkflowRow, error) {
	panic("TODO")
}

func (db *ddb) SelectWorkflowExecution(ctx context.Context, shardID int, domainID, workflowID, runID string) (*nosqlplugin.WorkflowExecution, error) {
	panic("TODO")
}

func (db *ddb) DeleteCurrentWorkflow(ctx context.Context, shardID int, domainID, workflowID, currentRunIDCondition string) error {
	panic("TODO")
}

func (db *ddb) DeleteWorkflowExecution(ctx context.Context, shardID int, domainID, workflowID, runID string) error {
	panic("TODO")
}

func (db *ddb) SelectWorkflowTimerTasks(ctx context.Context, shardID int, domainID, workflowID, runID string) ([]persistence.HistoryTaskKey, error) {
	panic("TODO")
}

func (db *ddb) SelectAllCurrentWorkflows(ctx context.Context, shardID int, pageToken []byte, pageSize int) ([]*persistence.CurrentWorkflowExecution, []byte, error) {
	panic("TODO")
}

func (db *ddb) SelectAllWorkflowExecutions(ctx context.Context, shardID int, pageToken []byte, pageSize int) ([]*persistence.InternalListConcreteExecutionsEntity, []byte, error) {
	panic("TODO")
}

func (db *ddb) IsWorkflowExecutionExists(ctx context.Context, shardID int, domainID, workflowID, runID string) (bool, error) {
	panic("TODO")
}

func (db *ddb) SelectTransferTasksOrderByTaskID(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	panic("TODO")
}

func (db *ddb) DeleteTransferTask(ctx context.Context, shardID int, keys []persistence.HistoryTaskKey) error {
	panic("TODO")
}

func (db *ddb) RangeDeleteTransferTasks(ctx context.Context, shardID int, inclusiveBeginTaskID, exclusiveEndTaskID int64) error {
	panic("TODO")
}

func (db *ddb) SelectTimerTasksOrderByVisibilityTime(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTime, exclusiveMaxTime time.Time) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	panic("TODO")
}

func (db *ddb) DeleteTimerTask(ctx context.Context, shardID int, keys []persistence.HistoryTaskKey) error {
	panic("TODO")
}

func (db *ddb) RangeDeleteTimerTasks(ctx context.Context, shardID int, inclusiveMinTime, exclusiveMaxTime time.Time) error {
	panic("TODO")
}

func (db *ddb) SelectReplicationTasksOrderByTaskID(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	panic("TODO")
}

func (db *ddb) DeleteReplicationTask(ctx context.Context, shardID int, keys []persistence.HistoryTaskKey) error {
	panic("TODO")
}

func (db *ddb) RangeDeleteReplicationTasks(ctx context.Context, shardID int, exclusiveEndTaskID int64) error {
	panic("TODO")
}

func (db *ddb) InsertReplicationTask(ctx context.Context, tasks []*nosqlplugin.HistoryMigrationTask, condition nosqlplugin.ShardCondition) error {
	panic("TODO")
}

func (db *ddb) InsertHistoryTasks(ctx context.Context, tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask, currentTimeStamp time.Time, condition nosqlplugin.ShardCondition) error {
	panic("TODO")
}

func (db *ddb) DeleteCrossClusterTask(ctx context.Context, shardID int, targetCluster string, taskID int64) error {
	panic("TODO")
}

func (db *ddb) InsertReplicationDLQTask(ctx context.Context, shardID int, sourceCluster string, task *nosqlplugin.HistoryMigrationTask) error {
	panic("TODO")
}

func (db *ddb) SelectReplicationDLQTasksOrderByTaskID(ctx context.Context, shardID int, sourceCluster string, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	panic("TODO")
}

func (db *ddb) SelectReplicationDLQTasksCount(ctx context.Context, shardID int, sourceCluster string) (int64, error) {
	panic("TODO")
}

func (db *ddb) DeleteReplicationDLQTask(ctx context.Context, shardID int, sourceCluster string, taskID int64) error {
	panic("TODO")
}

func (db *ddb) RangeDeleteReplicationDLQTasks(ctx context.Context, shardID int, sourceCluster string, inclusiveBeginTaskID, exclusiveEndTaskID int64) error {
	panic("TODO")
}

func (db *ddb) SelectActiveClusterSelectionPolicy(ctx context.Context, shardID int, domainID, wfID, rID string) (*nosqlplugin.ActiveClusterSelectionPolicyRow, error) {
	panic("TODO")
}

func (db *ddb) DeleteActiveClusterSelectionPolicy(ctx context.Context, shardID int, domainID, wfID, rID string) error {
	panic("TODO")
}
