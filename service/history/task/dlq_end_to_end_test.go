package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

// TestDLQ_EndToEnd demonstrates the full DLQ flow:
// 1. Standby task is enqueued to DLQ when it exceeds discard delay
// 2. On failover, DLQ processor reads and processes the task
// 3. Task is removed from DLQ after successful processing
func TestDLQ_EndToEnd(t *testing.T) {
	logger := testlogger.New(t)
	ctx := context.Background()

	// Create DLQ manager
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	// Step 1: Simulate standby cluster receiving a task that can't be processed
	// (missing replication events from active cluster)
	taskInfo := &persistence.TransferTaskInfo{
		DomainID:            "test-domain",
		WorkflowID:          "test-workflow",
		RunID:               "test-run",
		TaskID:              999,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		Version:             10,
		VisibilityTimestamp: time.Now().Add(-30 * time.Minute), // Task is old
	}

	// Convert to persistence.Task
	persistenceTask, err := taskInfo.ToTask()
	require.NoError(t, err)

	// Simulate the discard flow - task has been pending for too long
	postActionInfo := &pushActivityToMatchingInfo{
		activityScheduleToStartTimeout: 60,
	}

	discardFn := standbyTaskPostActionEnqueueToDLQ(
		dlqManager,
		1, // shardID
		"data-residency",
		"us-west",
	)

	err = discardFn(ctx, persistenceTask, postActionInfo, logger)
	require.Equal(t, ErrTaskDiscarded, err, "Task should be marked as discarded")

	// Step 2: Verify task is in DLQ
	dlqTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, dlqTasks.Tasks, 1, "Task should be in DLQ")
	require.Equal(t, int64(999), dlqTasks.Tasks[0].TaskID)

	// Step 3: Simulate cluster attribute failover from active to standby
	// The standby cluster becomes active for this cluster attribute
	processor := NewStandbyTaskDLQProcessor(dlqManager, nil, logger)

	err = processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
	})
	require.NoError(t, err)

	// Step 4: Verify task was removed from DLQ
	dlqTasksAfter, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, dlqTasksAfter.Tasks, 0, "Task should be removed from DLQ after processing")

	// Step 5: Verify DLQ size is correct
	sizeResp, err := dlqManager.GetStandbyTaskDLQSize(ctx, &persistence.GetStandbyTaskDLQSizeRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), sizeResp.Size, "DLQ should be empty")
}

// TestDLQ_MultipleClusterAttributes demonstrates that DLQ properly scopes tasks
// by cluster attribute, so only the failed-over attribute's tasks are processed
func TestDLQ_MultipleClusterAttributes(t *testing.T) {
	logger := testlogger.New(t)
	ctx := context.Background()

	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	// Enqueue tasks for different cluster attributes
	attributes := []struct {
		scope string
		name  string
	}{
		{"data-residency", "us-west"},
		{"data-residency", "eu-central"},
		{"compliance", "pci"},
	}

	for i, attr := range attributes {
		taskInfo := &persistence.TransferTaskInfo{
			DomainID:            "multi-domain",
			WorkflowID:          "workflow-" + attr.name,
			RunID:               "run1",
			TaskID:              int64(100 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Now(),
		}

		taskPayload, err := json.Marshal(taskInfo)
		require.NoError(t, err)

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "multi-domain",
			ClusterAttributeScope: attr.scope,
			ClusterAttributeName:  attr.name,
			WorkflowID:           "workflow-" + attr.name,
			RunID:                "run1",
			TaskID:               int64(100 + i),
			VisibilityTimestamp:  time.Now().UnixNano(),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          taskPayload,
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Process failover for only us-west
	processor := NewStandbyTaskDLQProcessor(dlqManager, nil, logger)
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
	})
	require.NoError(t, err)

	// Verify only us-west task was processed
	usWestTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, usWestTasks.Tasks, 0, "us-west task should be processed")

	// Verify eu-central task is still in DLQ
	euTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "eu-central",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, euTasks.Tasks, 1, "eu-central task should still be in DLQ")

	// Verify pci task is still in DLQ
	pciTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "multi-domain",
		ClusterAttributeScope: "compliance",
		ClusterAttributeName:  "pci",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, pciTasks.Tasks, 1, "pci task should still be in DLQ")
}
