package persistence

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/log/testlogger"
)

func TestStandbyTaskDLQManager_EnqueueAndRead(t *testing.T) {
	// This will fail until we implement the in-memory manager
	manager := NewInMemoryStandbyTaskDLQManager(testlogger.New(t))
	require.NotNil(t, manager)

	ctx := context.Background()

	// Test enqueue
	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:            "wf1",
		RunID:                 "run1",
		TaskID:                100,
		VisibilityTimestamp:   1234567890000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("test-payload"),
		Version:               5,
	})
	require.NoError(t, err)

	// Test read
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)
	require.Equal(t, int64(1234567890000000000), resp.Tasks[0].VisibilityTimestamp)
}

func TestInMemoryStandbyTaskDLQManager(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T, manager StandbyTaskDLQManager)
	}{
		{
			name: "EnqueueAndRead",
			test: testEnqueueAndRead,
		},
		{
			name: "DuplicateEnqueue",
			test: testDuplicateEnqueue,
		},
		{
			name: "Delete",
			test: testDelete,
		},
		{
			name: "GetSize",
			test: testGetSize,
		},
		{
			name: "Pagination",
			test: testPagination,
		},
		{
			name: "ClusterAttributeScoping",
			test: testClusterAttributeScoping,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewInMemoryStandbyTaskDLQManager(testlogger.New(t))
			defer manager.Close()
			tt.test(t, manager)
		})
	}
}

func testEnqueueAndRead(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:            "wf1",
		RunID:                 "run1",
		TaskID:                100,
		VisibilityTimestamp:   1000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("payload1"),
		Version:               5,
	})
	require.NoError(t, err)

	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)
	require.Equal(t, int64(1000000000), resp.Tasks[0].VisibilityTimestamp)
	require.Equal(t, "wf1", resp.Tasks[0].WorkflowID)
}

func testDuplicateEnqueue(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	req := &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:            "wf1",
		RunID:                 "run1",
		TaskID:                100,
		VisibilityTimestamp:   2000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("payload1"),
		Version:               5,
	}

	err := manager.EnqueueStandbyTask(ctx, req)
	require.NoError(t, err)

	// Duplicate should fail
	err = manager.EnqueueStandbyTask(ctx, req)
	require.Error(t, err)
	_, ok := err.(*ConditionFailedError)
	require.True(t, ok, "expected ConditionFailedError")
}

func testDelete(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:            "wf1",
		RunID:                 "run1",
		TaskID:                100,
		VisibilityTimestamp:   3000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("payload1"),
		Version:               5,
	})
	require.NoError(t, err)

	err = manager.DeleteStandbyTask(ctx, &DeleteStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		TaskID:                100,
		TaskType:              TransferTaskTypeActivityTask,
		VisibilityTimestamp:   3000000000,
	})
	require.NoError(t, err)

	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0)
}

func testGetSize(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
			ShardID:               1,
			DomainID:              "domain1",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:            fmt.Sprintf("wf%d", i),
			RunID:                 "run1",
			TaskID:                int64(100 + i),
			VisibilityTimestamp:   int64(4000000000 + i),
			TaskType:              TransferTaskTypeActivityTask,
			TaskPayload:           []byte("payload"),
			Version:               5,
		})
		require.NoError(t, err)
	}

	resp, err := manager.GetStandbyTaskDLQSize(ctx, &GetStandbyTaskDLQSizeRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), resp.Size)
}

func testPagination(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	// Enqueue 10 tasks
	for i := 0; i < 10; i++ {
		err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
			ShardID:               1,
			DomainID:              "domain1",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:            fmt.Sprintf("wf%d", i),
			RunID:                 "run1",
			TaskID:                int64(100 + i),
			VisibilityTimestamp:   int64(5000000000 + i),
			TaskType:              TransferTaskTypeActivityTask,
			TaskPayload:           []byte("payload"),
			Version:               5,
		})
		require.NoError(t, err)
	}

	// Read first page
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              3,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)
	require.NotNil(t, resp.NextPageToken)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)

	// Read second page
	resp, err = manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              3,
		NextPageToken:         resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)
	require.Equal(t, int64(103), resp.Tasks[0].TaskID)
}

func testClusterAttributeScoping(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	// Enqueue to scope1:attr1
	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:            "wf1",
		RunID:                 "run1",
		TaskID:                100,
		VisibilityTimestamp:   6000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("payload1"),
		Version:               5,
	})
	require.NoError(t, err)

	// Enqueue to scope1:attr2
	err = manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr2",
		WorkflowID:            "wf2",
		RunID:                 "run2",
		TaskID:                200,
		VisibilityTimestamp:   7000000000,
		TaskType:              TransferTaskTypeActivityTask,
		TaskPayload:           []byte("payload2"),
		Version:               5,
	})
	require.NoError(t, err)

	// Read scope1:attr1 - should only get first task
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)

	// Read scope1:attr2 - should only get second task
	resp, err = manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr2",
		PageSize:              10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(200), resp.Tasks[0].TaskID)
}
