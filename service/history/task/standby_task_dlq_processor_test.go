package task

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

func TestStandbyTaskDLQProcessor_ProcessOnFailover(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue some tasks to DLQ with proper serialization
	for i := 0; i < 3; i++ {
		taskInfo := &persistence.TransferTaskInfo{
			DomainID:            "test-domain",
			WorkflowID:          fmt.Sprintf("wf%d", i),
			RunID:               "run1",
			TaskID:              int64(100 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Unix(0, int64(1000000000+i)),
		}

		taskPayload, err := json.Marshal(taskInfo)
		require.NoError(t, err)

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "test-domain",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:           fmt.Sprintf("wf%d", i),
			RunID:                "run1",
			TaskID:               int64(100 + i),
			VisibilityTimestamp:  int64(1000000000 + i),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          taskPayload,
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Create processor
	processor := NewStandbyTaskDLQProcessor(
		dlqManager,
		nil, // executor not needed for POC
		logger,
	)

	// Process failover
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
	})
	require.NoError(t, err)

	// Verify tasks were removed from DLQ after processing
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0, "All tasks should be removed from DLQ after processing")
}
