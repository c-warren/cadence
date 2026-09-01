package replication

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

// TaskReader will read replication tasks from database
type TaskReader struct {
	shardID          int
	executionManager persistence.ExecutionManager
}

// NewTaskReader creates new TaskReader
func NewTaskReader(shardID int, executionManager persistence.ExecutionManager) *TaskReader {
	return &TaskReader{
		shardID:          shardID,
		executionManager: executionManager,
	}
}

// Read reads and returns replications tasks from readLevel to maxReadLevel
func (r *TaskReader) Read(ctx context.Context, readLevel int64, maxReadLevel int64, batchSize int) ([]persistence.Task, bool, error) {
	// Check if it is even possible to return any results.
	// If not return early with empty response. Do not hit persistence.
	if readLevel >= maxReadLevel {
		return nil, false, nil
	}

	response, err := r.executionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		TaskCategory:        persistence.HistoryTaskCategoryReplication,
		InclusiveMinTaskKey: persistence.NewImmediateTaskKey(readLevel + 1),
		ExclusiveMaxTaskKey: persistence.NewImmediateTaskKey(maxReadLevel + 1),
		PageSize:            batchSize,
		ShardID:             common.Ptr(r.shardID),
	})
	if err != nil {
		return nil, false, err
	}

	hasMore := response.NextPageToken != nil
	return response.Tasks, hasMore, nil
}
