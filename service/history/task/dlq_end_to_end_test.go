package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/clock"
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
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
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
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, dlqTasks.Tasks, 1, "Task should be in DLQ")
	require.Equal(t, int64(999), dlqTasks.Tasks[0].TaskID)

	// Step 3: Simulate cluster attribute failover from active to standby
	// The standby cluster becomes active for this cluster attribute
	// Mock task initializer - in real usage this comes from queue
	mockInitializer := func(t persistence.Task) Task {
		return nil // For this test, we don't actually execute tasks
	}
	processor := NewStandbyTaskDLQProcessor(dlqManager, nil, mockInitializer, clock.NewMockedTimeSource(), func() bool { return true }, logger)

	err = processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
	})
	require.NoError(t, err)

	// Step 4: Verify task was removed from DLQ
	dlqTasksAfter, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, dlqTasksAfter.Tasks, 0, "Task should be removed from DLQ after processing")

	// Step 5: Verify DLQ size is correct
	sizeResp, err := dlqManager.GetStandbyTaskDLQSize(ctx, &persistence.GetStandbyTaskDLQSizeRequest{
		ShardID:               1,
		DomainID:              "test-domain",
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

	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
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
			ShardID:               1,
			DomainID:              "multi-domain",
			ClusterAttributeScope: attr.scope,
			ClusterAttributeName:  attr.name,
			WorkflowID:            "workflow-" + attr.name,
			RunID:                 "run1",
			TaskID:                int64(100 + i),
			VisibilityTimestamp:   time.Now().UnixNano(),
			TaskType:              persistence.TransferTaskTypeActivityTask,
			TaskPayload:           taskPayload,
			Version:               5,
		})
		require.NoError(t, err)
	}

	// Process failover for only us-west
	mockInitializer := func(t persistence.Task) Task {
		return nil // For this test, we don't actually execute tasks
	}
	processor := NewStandbyTaskDLQProcessor(dlqManager, nil, mockInitializer, clock.NewMockedTimeSource(), func() bool { return true }, logger)
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:               1,
		DomainID:              "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
	})
	require.NoError(t, err)

	// Verify only us-west task was processed
	usWestTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "us-west",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, usWestTasks.Tasks, 0, "us-west task should be processed")

	// Verify eu-central task is still in DLQ
	euTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "multi-domain",
		ClusterAttributeScope: "data-residency",
		ClusterAttributeName:  "eu-central",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, euTasks.Tasks, 1, "eu-central task should still be in DLQ")

	// Verify pci task is still in DLQ
	pciTasks, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "multi-domain",
		ClusterAttributeScope: "compliance",
		ClusterAttributeName:  "pci",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, pciTasks.Tasks, 1, "pci task should still be in DLQ")
}

// TestDLQ_ComparisonPointVsRange demonstrates comparison between point-delete and range-delete
// DLQ implementations using the in-memory implementation as a baseline
// NOTE: This test uses in-memory DLQ. For true Cassandra comparison testing,
// use the integration test suite with real Cassandra instances
func TestDLQ_ComparisonPointVsRange(t *testing.T) {
	logger := testlogger.New(t)
	ctx := context.Background()

	// Use in-memory DLQ for baseline comparison
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
	defer dlqManager.Close()

	const (
		numTasks      = 100
		shardID       = 1
		domainID      = "comparison-test-domain"
		scope         = "data-residency"
		attributeName = "us-west"
	)

	// Step 1: Enqueue multiple tasks
	enqueuedTasks := make([]*persistence.EnqueueStandbyTaskRequest, numTasks)
	startTime := time.Now()

	for i := 0; i < numTasks; i++ {
		taskInfo := &persistence.TransferTaskInfo{
			DomainID:            domainID,
			WorkflowID:          "workflow-" + string(rune('A'+i%26)),
			RunID:               "run-1",
			TaskID:              int64(1000 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             int64(i),
			VisibilityTimestamp: time.Now().Add(time.Duration(i) * time.Second),
		}

		taskPayload, err := json.Marshal(taskInfo)
		require.NoError(t, err)

		req := &persistence.EnqueueStandbyTaskRequest{
			ShardID:               shardID,
			DomainID:              domainID,
			ClusterAttributeScope: scope,
			ClusterAttributeName:  attributeName,
			WorkflowID:            taskInfo.WorkflowID,
			RunID:                 taskInfo.RunID,
			TaskID:                taskInfo.TaskID,
			VisibilityTimestamp:   taskInfo.VisibilityTimestamp.UnixNano(),
			TaskType:              taskInfo.TaskType,
			TaskPayload:           taskPayload,
			Version:               taskInfo.Version,
		}

		err = dlqManager.EnqueueStandbyTask(ctx, req)
		require.NoError(t, err)
		enqueuedTasks[i] = req
	}

	enqueueTime := time.Since(startTime)
	t.Logf("Enqueued %d tasks in %v (avg: %v per task)", numTasks, enqueueTime, enqueueTime/numTasks)

	// Step 2: Read all tasks in pages
	readStartTime := time.Now()
	var allReadTasks []*persistence.StandbyTaskDLQEntry
	var pageToken []byte
	pageSize := 25

	for {
		resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
			ShardID:               shardID,
			DomainID:              domainID,
			ClusterAttributeScope: scope,
			ClusterAttributeName:  attributeName,
			PageSize:              pageSize,
			NextPageToken:         pageToken,
		})
		require.NoError(t, err)

		allReadTasks = append(allReadTasks, resp.Tasks...)
		if len(resp.NextPageToken) == 0 {
			break
		}
		pageToken = resp.NextPageToken
	}

	readTime := time.Since(readStartTime)
	require.Len(t, allReadTasks, numTasks, "Should read all enqueued tasks")
	t.Logf("Read %d tasks in %v (avg: %v per task)", numTasks, readTime, readTime/numTasks)

	// Step 3: Get DLQ size
	sizeStartTime := time.Now()
	sizeResp, err := dlqManager.GetStandbyTaskDLQSize(ctx, &persistence.GetStandbyTaskDLQSizeRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: scope,
		ClusterAttributeName:  attributeName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(numTasks), sizeResp.Size)
	t.Logf("GetStandbyTaskDLQSize completed in %v", time.Since(sizeStartTime))

	// Step 4: Delete half the tasks (simulate partial processing)
	deleteStartTime := time.Now()
	tasksToDelete := numTasks / 2

	for i := 0; i < tasksToDelete; i++ {
		err = dlqManager.DeleteStandbyTask(ctx, &persistence.DeleteStandbyTaskRequest{
			ShardID:               shardID,
			DomainID:              domainID,
			ClusterAttributeScope: scope,
			ClusterAttributeName:  attributeName,
			TaskID:                allReadTasks[i].TaskID,
			TaskType:              allReadTasks[i].TaskType,
			VisibilityTimestamp:   allReadTasks[i].VisibilityTimestamp,
		})
		require.NoError(t, err)
	}

	deleteTime := time.Since(deleteStartTime)
	t.Logf("Deleted %d tasks in %v (avg: %v per task)", tasksToDelete, deleteTime, deleteTime/time.Duration(tasksToDelete))

	// Step 5: Verify remaining tasks
	remainingResp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: scope,
		ClusterAttributeName:  attributeName,
		PageSize:              numTasks,
	})
	require.NoError(t, err)
	require.Len(t, remainingResp.Tasks, numTasks-tasksToDelete, "Should have remaining tasks after partial delete")

	// Step 6: Verify size after delete
	sizeAfterDelete, err := dlqManager.GetStandbyTaskDLQSize(ctx, &persistence.GetStandbyTaskDLQSizeRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: scope,
		ClusterAttributeName:  attributeName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(numTasks-tasksToDelete), sizeAfterDelete.Size)

	// Summary
	t.Logf("\n=== DLQ Performance Summary ===")
	t.Logf("Tasks: %d", numTasks)
	t.Logf("Enqueue: %v total, %v per task", enqueueTime, enqueueTime/time.Duration(numTasks))
	t.Logf("Read:    %v total, %v per task", readTime, readTime/time.Duration(numTasks))
	t.Logf("Delete:  %v total, %v per task", deleteTime, deleteTime/time.Duration(tasksToDelete))
	t.Logf("Note: This test uses in-memory DLQ. For Cassandra comparison testing,")
	t.Logf("      run integration tests with real Cassandra instances to compare")
	t.Logf("      point-delete vs range-delete performance and tombstone accumulation")
}
