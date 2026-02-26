package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func TestStandbyTaskPostActionEnqueueToDLQ(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
	defer dlqManager.Close()

	ctx := context.Background()

	// Create a transfer task info
	taskInfo := &persistence.TransferTaskInfo{
		DomainID:            "test-domain",
		WorkflowID:          "test-workflow",
		RunID:               "test-run",
		TaskID:              123,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		Version:             5,
		VisibilityTimestamp: time.Now(),
	}

	// Convert to Task interface
	task, err := taskInfo.ToTask()
	require.NoError(t, err)

	postActionInfo := &pushActivityToMatchingInfo{
		activityScheduleToStartTimeout: 60,
		tasklist:                       types.TaskList{Name: "test-tasklist"},
	}

	// Create the discard function with DLQ
	discardFn := standbyTaskPostActionEnqueueToDLQ(
		dlqManager,
		1, // shardID
		"scope1",
		"attr1",
	)

	// Execute
	err = discardFn(ctx, task, postActionInfo, logger)
	require.Error(t, err)
	require.Equal(t, ErrTaskDiscarded, err)

	// Verify task was enqueued to DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(123), resp.Tasks[0].TaskID)
	require.Equal(t, taskInfo.VisibilityTimestamp.UnixNano(), resp.Tasks[0].VisibilityTimestamp)
}
