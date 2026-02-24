# Standby Task DLQ In-Memory POC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement an in-memory Dead Letter Queue (DLQ) for standby transfer tasks to prevent task loss during active-active domain failovers with replication delays.

**Architecture:** Replace the current task discard behavior with DLQ enqueue. Store tasks in memory with shard:domain:cluster-attribute scoping. On failover, query and reprocess DLQ tasks for the failed-over domain/cluster-attribute combination.

**Tech Stack:** Go, in-memory storage (maps + mutexes), Cadence persistence interfaces, queueV2 integration

**Scope:** Core functionality - DLQ persistence + task executor integration + cluster attribute failover. Table tests + integration tests + simulation tests. Skip periodic processing and CLI commands for this POC.

---

## Context

Cadence's active-active domains allow workflows to have different active clusters per cluster attribute. When a standby cluster cannot process a task (due to missing replicated history events), it retries the task. After `StandbyTaskMissingEventsDiscardDelay` (default 15 minutes), the task is discarded to prevent infinite retries.

If a failover occurs after the discard delay but before the active cluster processes the task, the task is permanently lost, causing workflows to become stuck. This POC implements a DLQ to store discarded tasks so they can be reprocessed after failover.

**Key Design Decision:** Use in-memory storage instead of Cassandra to validate the design without schema changes. This allows rapid iteration and testing before committing to a database schema.

---

## Task 1: Define Standby Task DLQ Request/Response Types

**Files:**
- Modify: `common/persistence/data_manager_interfaces.go` (append to end of file)

**Step 1: Write failing test for StandbyTaskDLQManager interface**

Create: `common/persistence/standby_task_dlq_test.go`

```go
package persistence

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandbyTaskDLQManager_EnqueueAndRead(t *testing.T) {
	// This will fail until we implement the in-memory manager
	manager := NewInMemoryStandbyTaskDLQManager()
	require.NotNil(t, manager)

	ctx := context.Background()

	// Test enqueue
	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("test-payload"),
		Version:              5,
	})
	require.NoError(t, err)

	// Test read
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./common/persistence -run TestStandbyTaskDLQManager_EnqueueAndRead -v`
Expected: FAIL with "undefined: EnqueueStandbyTaskRequest"

**Step 3: Define request/response types in data_manager_interfaces.go**

Add to end of file (after line ~1800):

```go
// StandbyTaskDLQManager manages the dead letter queue for standby tasks
type StandbyTaskDLQManager interface {
	Closeable
	EnqueueStandbyTask(ctx context.Context, request *EnqueueStandbyTaskRequest) error
	ReadStandbyTasks(ctx context.Context, request *ReadStandbyTasksRequest) (*ReadStandbyTasksResponse, error)
	DeleteStandbyTask(ctx context.Context, request *DeleteStandbyTaskRequest) error
	GetStandbyTaskDLQSize(ctx context.Context, request *GetStandbyTaskDLQSizeRequest) (*GetStandbyTaskDLQSizeResponse, error)
}

// EnqueueStandbyTaskRequest is the request to enqueue a standby task to DLQ
type EnqueueStandbyTaskRequest struct {
	ShardID              int
	DomainID             string
	ClusterAttributeScope string
	ClusterAttributeName  string
	WorkflowID           string
	RunID                string
	TaskID               int64
	TaskType             int
	TaskPayload          []byte
	Version              int64
}

// ReadStandbyTasksRequest is the request to read standby tasks from DLQ
type ReadStandbyTasksRequest struct {
	ShardID              int
	DomainID             string
	ClusterAttributeScope string
	ClusterAttributeName  string
	PageSize             int
	NextPageToken        []byte
}

// ReadStandbyTasksResponse contains standby tasks from DLQ
type ReadStandbyTasksResponse struct {
	Tasks         []*StandbyTaskDLQEntry
	NextPageToken []byte
}

// StandbyTaskDLQEntry represents a task in the DLQ
type StandbyTaskDLQEntry struct {
	ShardID              int
	DomainID             string
	ClusterAttributeScope string
	ClusterAttributeName  string
	WorkflowID           string
	RunID                string
	TaskID               int64
	TaskType             int
	TaskPayload          []byte
	Version              int64
	EnqueuedAt           int64 // Unix timestamp
}

// DeleteStandbyTaskRequest is the request to delete a standby task from DLQ
type DeleteStandbyTaskRequest struct {
	ShardID              int
	DomainID             string
	ClusterAttributeScope string
	ClusterAttributeName  string
	TaskID               int64
}

// GetStandbyTaskDLQSizeRequest is the request to get DLQ size
type GetStandbyTaskDLQSizeRequest struct {
	ShardID              int
	DomainID             string
	ClusterAttributeScope string
	ClusterAttributeName  string
}

// GetStandbyTaskDLQSizeResponse contains the DLQ size
type GetStandbyTaskDLQSizeResponse struct {
	Size int64
}
```

**Step 4: Run test to verify it still fails (but for the right reason)**

Run: `go test ./common/persistence -run TestStandbyTaskDLQManager_EnqueueAndRead -v`
Expected: FAIL with "undefined: NewInMemoryStandbyTaskDLQManager"

**Step 5: Commit the types**

```bash
git add common/persistence/data_manager_interfaces.go common/persistence/standby_task_dlq_test.go
git commit -m "feat: add StandbyTaskDLQManager interface and request/response types"
```

---

## Task 2: Implement In-Memory Standby Task DLQ Store

**Files:**
- Create: `common/persistence/inmemory_standby_task_dlq.go`
- Modify: `common/persistence/standby_task_dlq_test.go`

**Step 1: Implement in-memory DLQ manager**

Create: `common/persistence/inmemory_standby_task_dlq.go`

```go
package persistence

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type (
	inMemoryStandbyTaskDLQManager struct {
		mu    sync.RWMutex
		tasks map[string][]*StandbyTaskDLQEntry // key: shard:domain:scope:name
	}
)

// NewInMemoryStandbyTaskDLQManager creates a new in-memory DLQ manager
func NewInMemoryStandbyTaskDLQManager() StandbyTaskDLQManager {
	return &inMemoryStandbyTaskDLQManager{
		tasks: make(map[string][]*StandbyTaskDLQEntry),
	}
}

func (m *inMemoryStandbyTaskDLQManager) EnqueueStandbyTask(
	ctx context.Context,
	request *EnqueueStandbyTaskRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getKey(
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
	)

	// Check for duplicates
	tasks := m.tasks[key]
	for _, task := range tasks {
		if task.TaskID == request.TaskID {
			return &ConditionFailedError{
				Msg: fmt.Sprintf("task already exists: taskID=%d", request.TaskID),
			}
		}
	}

	entry := &StandbyTaskDLQEntry{
		ShardID:              request.ShardID,
		DomainID:             request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
		WorkflowID:           request.WorkflowID,
		RunID:                request.RunID,
		TaskID:               request.TaskID,
		TaskType:             request.TaskType,
		TaskPayload:          request.TaskPayload,
		Version:              request.Version,
		EnqueuedAt:           time.Now().Unix(),
	}

	m.tasks[key] = append(m.tasks[key], entry)

	// Sort by TaskID for consistent ordering
	sort.Slice(m.tasks[key], func(i, j int) bool {
		return m.tasks[key][i].TaskID < m.tasks[key][j].TaskID
	})

	return nil
}

func (m *inMemoryStandbyTaskDLQManager) ReadStandbyTasks(
	ctx context.Context,
	request *ReadStandbyTasksRequest,
) (*ReadStandbyTasksResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.getKey(
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
	)

	allTasks := m.tasks[key]
	if len(allTasks) == 0 {
		return &ReadStandbyTasksResponse{
			Tasks:         []*StandbyTaskDLQEntry{},
			NextPageToken: nil,
		}, nil
	}

	// Simple pagination: use page token as offset
	offset := 0
	if len(request.NextPageToken) > 0 {
		_, err := fmt.Sscanf(string(request.NextPageToken), "%d", &offset)
		if err != nil {
			return nil, err
		}
	}

	if offset >= len(allTasks) {
		return &ReadStandbyTasksResponse{
			Tasks:         []*StandbyTaskDLQEntry{},
			NextPageToken: nil,
		}, nil
	}

	end := offset + request.PageSize
	if end > len(allTasks) {
		end = len(allTasks)
	}

	tasks := make([]*StandbyTaskDLQEntry, end-offset)
	copy(tasks, allTasks[offset:end])

	var nextPageToken []byte
	if end < len(allTasks) {
		nextPageToken = []byte(fmt.Sprintf("%d", end))
	}

	return &ReadStandbyTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *inMemoryStandbyTaskDLQManager) DeleteStandbyTask(
	ctx context.Context,
	request *DeleteStandbyTaskRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getKey(
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
	)

	tasks := m.tasks[key]
	for i, task := range tasks {
		if task.TaskID == request.TaskID {
			// Remove task from slice
			m.tasks[key] = append(tasks[:i], tasks[i+1:]...)
			return nil
		}
	}

	// Not an error if task doesn't exist (idempotent delete)
	return nil
}

func (m *inMemoryStandbyTaskDLQManager) GetStandbyTaskDLQSize(
	ctx context.Context,
	request *GetStandbyTaskDLQSizeRequest,
) (*GetStandbyTaskDLQSizeResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.getKey(
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
	)

	return &GetStandbyTaskDLQSizeResponse{
		Size: int64(len(m.tasks[key])),
	}, nil
}

func (m *inMemoryStandbyTaskDLQManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = make(map[string][]*StandbyTaskDLQEntry)
}

func (m *inMemoryStandbyTaskDLQManager) getKey(
	shardID int,
	domainID string,
	clusterAttributeScope string,
	clusterAttributeName string,
) string {
	return fmt.Sprintf("%d:%s:%s:%s", shardID, domainID, clusterAttributeScope, clusterAttributeName)
}
```

**Step 2: Run test to verify it passes**

Run: `go test ./common/persistence -run TestStandbyTaskDLQManager_EnqueueAndRead -v`
Expected: PASS

**Step 3: Add table tests for all operations**

Add to `common/persistence/standby_task_dlq_test.go`:

```go
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
			manager := NewInMemoryStandbyTaskDLQManager()
			defer manager.Close()
			tt.test(t, manager)
		})
	}
}

func testEnqueueAndRead(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload1"),
		Version:              5,
	})
	require.NoError(t, err)

	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)
	require.Equal(t, "wf1", resp.Tasks[0].WorkflowID)
}

func testDuplicateEnqueue(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	req := &EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload1"),
		Version:              5,
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
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload1"),
		Version:              5,
	})
	require.NoError(t, err)

	err = manager.DeleteStandbyTask(ctx, &DeleteStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		TaskID:               100,
	})
	require.NoError(t, err)

	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0)
}

func testGetSize(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "domain1",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:           fmt.Sprintf("wf%d", i),
			RunID:                "run1",
			TaskID:               int64(100 + i),
			TaskType:             TransferTaskTypeActivityTask,
			TaskPayload:          []byte("payload"),
			Version:              5,
		})
		require.NoError(t, err)
	}

	resp, err := manager.GetStandbyTaskDLQSize(ctx, &GetStandbyTaskDLQSizeRequest{
		ShardID:              1,
		DomainID:             "domain1",
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
			ShardID:              1,
			DomainID:             "domain1",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:           fmt.Sprintf("wf%d", i),
			RunID:                "run1",
			TaskID:               int64(100 + i),
			TaskType:             TransferTaskTypeActivityTask,
			TaskPayload:          []byte("payload"),
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Read first page
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             3,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)
	require.NotNil(t, resp.NextPageToken)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)

	// Read second page
	resp, err = manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             3,
		NextPageToken:        resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)
	require.Equal(t, int64(103), resp.Tasks[0].TaskID)
}

func testClusterAttributeScoping(t *testing.T, manager StandbyTaskDLQManager) {
	ctx := context.Background()

	// Enqueue to scope1:attr1
	err := manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload1"),
		Version:              5,
	})
	require.NoError(t, err)

	// Enqueue to scope1:attr2
	err = manager.EnqueueStandbyTask(ctx, &EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr2",
		WorkflowID:           "wf2",
		RunID:                "run2",
		TaskID:               200,
		TaskType:             TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload2"),
		Version:              5,
	})
	require.NoError(t, err)

	// Read scope1:attr1 - should only get first task
	resp, err := manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(100), resp.Tasks[0].TaskID)

	// Read scope1:attr2 - should only get second task
	resp, err = manager.ReadStandbyTasks(ctx, &ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "domain1",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr2",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(200), resp.Tasks[0].TaskID)
}
```

**Step 4: Run table tests**

Run: `go test ./common/persistence -run TestInMemoryStandbyTaskDLQManager -v`
Expected: PASS (all 6 sub-tests)

**Step 5: Commit the implementation**

```bash
git add common/persistence/inmemory_standby_task_dlq.go common/persistence/standby_task_dlq_test.go
git commit -m "feat: implement in-memory standby task DLQ manager with pagination and scoping"
```

---

## Task 3: Integrate DLQ with Standby Task Executor

**Files:**
- Modify: `service/history/task/standby_task_util.go`
- Modify: `service/history/task/transfer_standby_task_executor.go`

**Step 1: Write test for DLQ enqueue on task discard**

Create: `service/history/task/standby_task_dlq_integration_test.go`

```go
package task

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

func TestStandbyTaskPostActionEnqueueToDLQ(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Create a transfer task
	task := &persistence.TransferTaskInfo{
		DomainID:            "test-domain",
		WorkflowID:          "test-workflow",
		RunID:               "test-run",
		TaskID:              123,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		Version:             5,
		VisibilityTimestamp: testClock.Now(),
	}

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
	err := discardFn(ctx, task, postActionInfo, logger)
	require.NoError(t, err)

	// Verify task was enqueued to DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Equal(t, int64(123), resp.Tasks[0].TaskID)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./service/history/task -run TestStandbyTaskPostActionEnqueueToDLQ -v`
Expected: FAIL with "undefined: standbyTaskPostActionEnqueueToDLQ"

**Step 3: Implement standbyTaskPostActionEnqueueToDLQ function**

Add to `service/history/task/standby_task_util.go` after line 92:

```go
// standbyTaskPostActionEnqueueToDLQ enqueues the task to DLQ instead of discarding
func standbyTaskPostActionEnqueueToDLQ(
	dlqManager persistence.StandbyTaskDLQManager,
	shardID int,
	clusterAttributeScope string,
	clusterAttributeName string,
) standbyPostActionFn {
	return func(
		ctx context.Context,
		task persistence.Task,
		postActionInfo interface{},
		logger log.Logger,
	) error {
		if postActionInfo == nil {
			return nil
		}

		logger.Warn("Enqueuing standby task to DLQ due to task being pending for too long",
			tag.WorkflowID(task.GetWorkflowID()),
			tag.WorkflowRunID(task.GetRunID()),
			tag.WorkflowDomainID(task.GetDomainID()),
			tag.TaskID(task.GetTaskID()),
			tag.TaskType(task.GetTaskType()),
			tag.FailoverVersion(task.GetVersion()),
			tag.Timestamp(task.GetVisibilityTimestamp()))

		// Serialize the task
		taskPayload, err := serializeTask(task)
		if err != nil {
			logger.Error("Failed to serialize task for DLQ", tag.Error(err))
			return ErrTaskDiscarded
		}

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              shardID,
			DomainID:             task.GetDomainID(),
			ClusterAttributeScope: clusterAttributeScope,
			ClusterAttributeName:  clusterAttributeName,
			WorkflowID:           task.GetWorkflowID(),
			RunID:                task.GetRunID(),
			TaskID:               task.GetTaskID(),
			TaskType:             task.GetTaskType(),
			TaskPayload:          taskPayload,
			Version:              task.GetVersion(),
		})

		if err != nil {
			// If duplicate, treat as success
			if _, ok := err.(*persistence.ConditionFailedError); ok {
				logger.Info("Task already exists in DLQ, treating as success",
					tag.TaskID(task.GetTaskID()))
				return ErrTaskDiscarded
			}
			logger.Error("Failed to enqueue task to DLQ", tag.Error(err))
			return err
		}

		// Return ErrTaskDiscarded to remove from execution queue
		return ErrTaskDiscarded
	}
}

// serializeTask converts a task to bytes for storage
func serializeTask(task persistence.Task) ([]byte, error) {
	// For POC, we'll store the task info as JSON
	// In production, this would use the same serialization as the persistence layer
	switch t := task.(type) {
	case *persistence.TransferTaskInfo:
		return json.Marshal(t)
	case *persistence.TimerTaskInfo:
		return json.Marshal(t)
	default:
		return nil, fmt.Errorf("unsupported task type: %T", task)
	}
}
```

Add import:
```go
import (
	"encoding/json"
	// ... existing imports
)
```

**Step 4: Run test to verify it passes**

Run: `go test ./service/history/task -run TestStandbyTaskPostActionEnqueueToDLQ -v`
Expected: PASS

**Step 5: Wire DLQ manager into transfer standby task executor**

Modify `service/history/task/transfer_standby_task_executor.go` constructor (around line 80):

```go
type transferStandbyTaskExecutor struct {
	*transferTaskExecutorBase
	clusterName            string
	dlqManager             persistence.StandbyTaskDLQManager  // ADD THIS
	shardID                int                                 // ADD THIS
	clusterAttributeScope  string                             // ADD THIS
	clusterAttributeName   string                             // ADD THIS
}

func newTransferStandbyTaskExecutor(
	shard shard.Context,
	historyEngine engine.Engine,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	historyClient history.Client,
	dlqManager persistence.StandbyTaskDLQManager,  // ADD THIS PARAMETER
) Executor {
	return &transferStandbyTaskExecutor{
		transferTaskExecutorBase: newTransferTaskExecutorBase(
			shard,
			historyEngine,
			logger,
			metricsClient,
			historyClient,
		),
		clusterName:           clusterName,
		dlqManager:            dlqManager,           // ADD THIS
		shardID:               shard.GetShardID(),   // ADD THIS
		clusterAttributeScope: "",                   // ADD THIS - will be set per-task
		clusterAttributeName:  "",                   // ADD THIS - will be set per-task
	}
}
```

**Step 6: Update getStandbyPostActionFn calls to use DLQ function**

Find all calls to `getStandbyPostActionFn` in `transfer_standby_task_executor.go` and update the discard function parameter.

Example (around line 168 in processActivityTask):

```go
// OLD:
return t.processTransfer(
	ctx,
	false,
	task,
	task.ScheduleID,
	actionFn,
	getStandbyPostActionFn(
		t.logger,
		task,
		t.getCurrentTime,
		t.config.StandbyTaskMissingEventsResendDelay(),
		t.config.StandbyTaskMissingEventsDiscardDelay(),
		t.pushActivity,
		standbyTaskPostActionTaskDiscarded,  // OLD
	),
)

// NEW:
return t.processTransfer(
	ctx,
	false,
	task,
	task.ScheduleID,
	actionFn,
	getStandbyPostActionFn(
		t.logger,
		task,
		t.getCurrentTime,
		t.config.StandbyTaskMissingEventsResendDelay(),
		t.config.StandbyTaskMissingEventsDiscardDelay(),
		t.pushActivity,
		standbyTaskPostActionEnqueueToDLQ(  // NEW
			t.dlqManager,
			t.shardID,
			t.clusterAttributeScope,
			t.clusterAttributeName,
		),
	),
)
```

Repeat for all task types: decision tasks, cancel execution, signal execution, start child execution, etc.

**Step 7: Run tests**

Run: `go test ./service/history/task -v`
Expected: Tests compile and existing tests still pass

**Step 8: Commit the integration**

```bash
git add service/history/task/standby_task_util.go service/history/task/transfer_standby_task_executor.go service/history/task/standby_task_dlq_integration_test.go
git commit -m "feat: integrate DLQ with standby task executor to enqueue discarded tasks"
```

---

## Task 4: Update Failover Domain Interface for Cluster Attributes

**Files:**
- Modify: `service/history/queuev2/interface.go`
- Modify: `service/history/engine/engineimpl/register_domain_failover_callback.go`

**Step 1: Write test for cluster attribute failover notification**

Create: `service/history/queuev2/failover_cluster_attribute_test.go`

```go
package queuev2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailoverDomainWithClusterAttributes(t *testing.T) {
	// Create a mock queue that tracks failover calls
	type failoverCall struct {
		domainIDs        map[string]struct{}
		clusterAttribute *ClusterAttributeKey
	}

	var calls []failoverCall

	mockQueue := &mockQueue{
		failoverFn: func(domainIDs map[string]struct{}, clusterAttr *ClusterAttributeKey) {
			calls = append(calls, failoverCall{
				domainIDs:        domainIDs,
				clusterAttribute: clusterAttr,
			})
		},
	}

	// Call with cluster attribute
	clusterAttr := &ClusterAttributeKey{
		Scope: "scope1",
		Name:  "attr1",
	}
	mockQueue.FailoverDomainWithClusterAttribute(
		map[string]struct{}{"domain1": {}},
		clusterAttr,
	)

	require.Len(t, calls, 1)
	require.Contains(t, calls[0].domainIDs, "domain1")
	require.Equal(t, "scope1", calls[0].clusterAttribute.Scope)
	require.Equal(t, "attr1", calls[0].clusterAttribute.Name)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./service/history/queuev2 -run TestFailoverDomainWithClusterAttributes -v`
Expected: FAIL with "undefined: ClusterAttributeKey"

**Step 3: Update queuev2 interface**

Modify `service/history/queuev2/interface.go`:

```go
type (
	// ClusterAttributeKey identifies a cluster attribute
	ClusterAttributeKey struct {
		Scope string
		Name  string
	}

	Queue interface {
		common.Daemon
		Category() persistence.HistoryTaskCategory
		NotifyNewTask(string, *hcommon.NotifyTaskInfo)

		// FailoverDomain triggers failover for specified domains
		// For backwards compatibility with active-passive domains
		FailoverDomain(map[string]struct{})

		// FailoverDomainWithClusterAttribute triggers failover for specified domains
		// with a specific cluster attribute (for active-active domains)
		FailoverDomainWithClusterAttribute(map[string]struct{}, *ClusterAttributeKey)

		HandleAction(context.Context, string, *queue.Action) (*queue.ActionResult, error)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}
)
```

**Step 4: Update queue implementations to support new method**

Modify `service/history/queuev2/queue_base.go` to add the new method:

```go
func (q *queueBase) FailoverDomainWithClusterAttribute(
	domainIDs map[string]struct{},
	clusterAttribute *ClusterAttributeKey,
) {
	// For POC, delegate to existing FailoverDomain
	// In full implementation, this would pass cluster attribute to rescheduler
	q.FailoverDomain(domainIDs)

	// TODO: Pass cluster attribute to rescheduler for scoped DLQ processing
	// q.rescheduler.RescheduleDomainsWithClusterAttribute(domainIDs, clusterAttribute)
}
```

**Step 5: Update domain failover callback to extract and pass cluster attributes**

Modify `service/history/engine/engineimpl/register_domain_failover_callback.go` (around line 100-150):

```go
// In the domainChangeCB function, after determining failoverActiveActiveDomainIDs:

// For active-active domains, extract cluster attributes that changed
type domainClusterAttrKey struct {
	domainID string
	scope    string
	name     string
}

failoverClusterAttributes := make(map[domainClusterAttrKey]struct{})

for domainID := range failoverActiveActiveDomainIDs {
	prevDomainCacheEntry := prevDomains[domainID]
	nextDomainCacheEntry := nextDomains[domainID]

	if prevDomainCacheEntry == nil {
		continue
	}

	// Compare cluster attributes to find changes
	prevAttrs := prevDomainCacheEntry.GetReplicationConfig().ActiveClusters
	nextAttrs := nextDomainCacheEntry.GetReplicationConfig().ActiveClusters

	// Iterate through all scopes and attributes to find failovers
	for scope, scopeAttrs := range nextAttrs.AttributeScopes {
		for name, attrInfo := range scopeAttrs.ClusterAttributes {
			// Check if this attribute failed over to current cluster
			prevAttrInfo := getClusterAttribute(prevAttrs, scope, name)
			if prevAttrInfo != nil &&
			   prevAttrInfo.ActiveClusterName != e.currentClusterName &&
			   attrInfo.ActiveClusterName == e.currentClusterName {
				failoverClusterAttributes[domainClusterAttrKey{
					domainID: domainID,
					scope:    scope,
					name:     name,
				}] = struct{}{}
			}
		}
	}
}

// Notify queues with cluster attribute information
for key := range failoverClusterAttributes {
	clusterAttr := &queuev2.ClusterAttributeKey{
		Scope: key.scope,
		Name:  key.name,
	}
	for _, processor := range e.queueProcessors {
		if qv2, ok := processor.(interface {
			FailoverDomainWithClusterAttribute(map[string]struct{}, *queuev2.ClusterAttributeKey)
		}); ok {
			qv2.FailoverDomainWithClusterAttribute(
				map[string]struct{}{key.domainID: {}},
				clusterAttr,
			)
		}
	}
}

// Helper function
func getClusterAttribute(
	activeClusters *types.ActiveClusters,
	scope string,
	name string,
) *types.ActiveClusterInfo {
	if activeClusters == nil || activeClusters.AttributeScopes == nil {
		return nil
	}
	scopeAttrs, ok := activeClusters.AttributeScopes[scope]
	if !ok || scopeAttrs.ClusterAttributes == nil {
		return nil
	}
	attrInfo, ok := scopeAttrs.ClusterAttributes[name]
	if !ok {
		return nil
	}
	return &attrInfo
}
```

**Step 6: Run tests**

Run: `go test ./service/history/queuev2 -v`
Expected: Tests compile and pass

**Step 7: Commit the failover updates**

```bash
git add service/history/queuev2/interface.go service/history/queuev2/queue_base.go service/history/queuev2/failover_cluster_attribute_test.go service/history/engine/engineimpl/register_domain_failover_callback.go
git commit -m "feat: extend failover interface to support cluster attribute scoping"
```

---

## Task 5: Implement DLQ Failover Processing Logic

**Files:**
- Create: `service/history/task/standby_task_dlq_processor.go`
- Modify: `service/history/task/transfer_standby_task_executor.go`

**Step 1: Write test for DLQ processing on failover**

Create: `service/history/task/standby_task_dlq_processor_test.go`

```go
package task

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

func TestStandbyTaskDLQProcessor_ProcessOnFailover(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue some tasks to DLQ
	for i := 0; i < 3; i++ {
		err := dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "test-domain",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  "attr1",
			WorkflowID:           fmt.Sprintf("wf%d", i),
			RunID:                "run1",
			TaskID:               int64(100 + i),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          []byte(fmt.Sprintf("payload%d", i)),
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Create processor
	processor := NewStandbyTaskDLQProcessor(
		dlqManager,
		nil, // executor - will be mocked
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
	// Tasks should be removed after successful processing
	require.Len(t, resp.Tasks, 0)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./service/history/task -run TestStandbyTaskDLQProcessor_ProcessOnFailover -v`
Expected: FAIL with "undefined: NewStandbyTaskDLQProcessor"

**Step 3: Implement DLQ processor**

Create: `service/history/task/standby_task_dlq_processor.go`

```go
package task

import (
	"context"
	"encoding/json"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	// StandbyTaskDLQProcessor processes tasks from the DLQ after failover
	StandbyTaskDLQProcessor struct {
		dlqManager persistence.StandbyTaskDLQManager
		executor   Executor
		logger     log.Logger
	}

	// ProcessFailoverRequest contains parameters for processing DLQ on failover
	ProcessFailoverRequest struct {
		ShardID              int
		DomainID             string
		ClusterAttributeScope string
		ClusterAttributeName  string
	}
)

// NewStandbyTaskDLQProcessor creates a new DLQ processor
func NewStandbyTaskDLQProcessor(
	dlqManager persistence.StandbyTaskDLQManager,
	executor Executor,
	logger log.Logger,
) *StandbyTaskDLQProcessor {
	return &StandbyTaskDLQProcessor{
		dlqManager: dlqManager,
		executor:   executor,
		logger:     logger,
	}
}

// ProcessFailover processes all DLQ tasks for a specific domain/cluster attribute combination
func (p *StandbyTaskDLQProcessor) ProcessFailover(
	ctx context.Context,
	request *ProcessFailoverRequest,
) error {
	p.logger.Info("Processing DLQ tasks for failover",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope),
		tag.Value(request.ClusterAttributeName))

	pageSize := 100
	var nextPageToken []byte

	for {
		// Read tasks from DLQ
		resp, err := p.dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
			ShardID:              request.ShardID,
			DomainID:             request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			PageSize:             pageSize,
			NextPageToken:        nextPageToken,
		})
		if err != nil {
			p.logger.Error("Failed to read DLQ tasks", tag.Error(err))
			return err
		}

		// Process each task
		for _, dlqTask := range resp.Tasks {
			if err := p.processTask(ctx, dlqTask); err != nil {
				p.logger.Error("Failed to process DLQ task",
					tag.TaskID(dlqTask.TaskID),
					tag.Error(err))
				// Continue processing other tasks
				continue
			}

			// Delete task from DLQ after successful processing
			if err := p.dlqManager.DeleteStandbyTask(ctx, &persistence.DeleteStandbyTaskRequest{
				ShardID:              dlqTask.ShardID,
				DomainID:             dlqTask.DomainID,
				ClusterAttributeScope: dlqTask.ClusterAttributeScope,
				ClusterAttributeName:  dlqTask.ClusterAttributeName,
				TaskID:               dlqTask.TaskID,
			}); err != nil {
				p.logger.Error("Failed to delete DLQ task after processing",
					tag.TaskID(dlqTask.TaskID),
					tag.Error(err))
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		nextPageToken = resp.NextPageToken
	}

	p.logger.Info("Completed processing DLQ tasks for failover",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID))

	return nil
}

// processTask deserializes and executes a single DLQ task
func (p *StandbyTaskDLQProcessor) processTask(
	ctx context.Context,
	dlqTask *persistence.StandbyTaskDLQEntry,
) error {
	// Deserialize task
	task, err := p.deserializeTask(dlqTask)
	if err != nil {
		return err
	}

	// Execute task through executor
	// The executor will handle active/standby routing and task processing
	_, err = p.executor.Execute(task)
	if err != nil {
		return err
	}

	return nil
}

// deserializeTask converts DLQ entry back to a Task
func (p *StandbyTaskDLQProcessor) deserializeTask(
	dlqTask *persistence.StandbyTaskDLQEntry,
) (Task, error) {
	// Deserialize based on task type
	switch dlqTask.TaskType {
	case persistence.TransferTaskTypeActivityTask,
		persistence.TransferTaskTypeDecisionTask,
		persistence.TransferTaskTypeCancelExecution,
		persistence.TransferTaskTypeSignalExecution,
		persistence.TransferTaskTypeStartChildExecution:
		var taskInfo persistence.TransferTaskInfo
		if err := json.Unmarshal(dlqTask.TaskPayload, &taskInfo); err != nil {
			return nil, err
		}
		task, err := taskInfo.ToTask()
		if err != nil {
			return nil, err
		}
		return newTransferTask(
			p.logger,
			task,
			&taskInfo,
			false, // forceNewWorkflow
			nil,   // filter
		), nil

	default:
		return nil, fmt.Errorf("unsupported task type: %d", dlqTask.TaskType)
	}
}
```

**Step 4: Wire DLQ processor into queue on failover**

Modify `service/history/queuev2/queue_base.go` to process DLQ on failover:

```go
func (q *queueBase) FailoverDomainWithClusterAttribute(
	domainIDs map[string]struct{},
	clusterAttribute *ClusterAttributeKey,
) {
	// Delegate to existing FailoverDomain for backwards compatibility
	q.FailoverDomain(domainIDs)

	// Process DLQ for each domain/cluster attribute
	for domainID := range domainIDs {
		if q.dlqProcessor != nil {
			ctx := context.Background()
			err := q.dlqProcessor.ProcessFailover(ctx, &task.ProcessFailoverRequest{
				ShardID:              q.shard.GetShardID(),
				DomainID:             domainID,
				ClusterAttributeScope: clusterAttribute.Scope,
				ClusterAttributeName:  clusterAttribute.Name,
			})
			if err != nil {
				q.logger.Error("Failed to process DLQ on failover",
					tag.WorkflowDomainID(domainID),
					tag.Error(err))
			}
		}
	}
}
```

Add dlqProcessor field to queueBase and wire it in the constructor.

**Step 5: Run tests**

Run: `go test ./service/history/task -run TestStandbyTaskDLQProcessor -v`
Expected: PASS

**Step 6: Commit DLQ processor**

```bash
git add service/history/task/standby_task_dlq_processor.go service/history/task/standby_task_dlq_processor_test.go service/history/queuev2/queue_base.go
git commit -m "feat: implement DLQ processor for failover reprocessing"
```

---

## Task 6: Add Dynamic Configuration for Simulation Testing

**Files:**
- Modify: `common/dynamicconfig/constants.go`
- Create: `service/history/task/chaos_config.go`

**Step 1: Add dynamic config keys**

Modify `common/dynamicconfig/constants.go`:

```go
// Add to the list of config keys:

// StandbyTaskMissingEventsDiscardDelay is the time before discarding standby tasks
// For simulation testing, this can be set to very low values (e.g., 1s)
StandbyTaskMissingEventsDiscardDelay = "history.standbyTaskMissingEventsDiscardDelay"

// StandbyTaskErrorInjectionRate is the error injection rate for standby task processing (0.0-1.0)
// Used for simulation testing to introduce chaos
StandbyTaskErrorInjectionRate = "history.standbyTaskErrorInjectionRate"
```

**Step 2: Create chaos injection wrapper**

Create: `service/history/task/chaos_config.go`

```go
package task

import (
	"math/rand"

	"github.com/uber/cadence/common/dynamicconfig"
)

type (
	// ChaosConfig provides error injection for testing
	ChaosConfig struct {
		errorRate dynamicconfig.FloatPropertyFn
	}
)

// NewChaosConfig creates a new chaos configuration
func NewChaosConfig(errorRate dynamicconfig.FloatPropertyFn) *ChaosConfig {
	return &ChaosConfig{
		errorRate: errorRate,
	}
}

// ShouldInjectError returns true if an error should be injected
func (c *ChaosConfig) ShouldInjectError() bool {
	rate := c.errorRate()
	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}
```

**Step 3: Integrate chaos config into task executor**

Modify `service/history/task/transfer_standby_task_executor.go`:

```go
type transferStandbyTaskExecutor struct {
	*transferTaskExecutorBase
	clusterName            string
	dlqManager             persistence.StandbyTaskDLQManager
	shardID                int
	clusterAttributeScope  string
	clusterAttributeName   string
	chaosConfig            *ChaosConfig  // ADD THIS
}

// In Execute method, inject errors:
func (t *transferStandbyTaskExecutor) Execute(task Task) (ExecuteResponse, error) {
	// Chaos injection for testing
	if t.chaosConfig != nil && t.chaosConfig.ShouldInjectError() {
		return ExecuteResponse{}, fmt.Errorf("chaos: injected error for testing")
	}

	// ... existing execute logic
}
```

**Step 4: Write test for chaos injection**

Create: `service/history/task/chaos_config_test.go`

```go
package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/dynamicconfig"
)

func TestChaosConfig(t *testing.T) {
	tests := []struct {
		name          string
		errorRate     float64
		expectedError bool
	}{
		{
			name:          "NoErrors",
			errorRate:     0.0,
			expectedError: false,
		},
		{
			name:          "AlwaysError",
			errorRate:     1.0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewChaosConfig(func() float64 { return tt.errorRate })

			if tt.errorRate == 0.0 {
				require.False(t, config.ShouldInjectError())
			} else if tt.errorRate == 1.0 {
				require.True(t, config.ShouldInjectError())
			}
		})
	}
}
```

**Step 5: Run test**

Run: `go test ./service/history/task -run TestChaosConfig -v`
Expected: PASS

**Step 6: Commit chaos configuration**

```bash
git add common/dynamicconfig/constants.go service/history/task/chaos_config.go service/history/task/chaos_config_test.go service/history/task/transfer_standby_task_executor.go
git commit -m "feat: add dynamic config for simulation testing with chaos injection"
```

---

## Task 7: Write Integration Tests

**Files:**
- Create: `service/history/task/dlq_integration_test.go`

**Step 1: Write end-to-end integration test**

Create: `service/history/task/dlq_integration_test.go`

```go
package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

func TestDLQ_EndToEnd_StandbyToActive(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Step 1: Enqueue standby task to DLQ (simulating discard)
	task := &persistence.TransferTaskInfo{
		DomainID:            "test-domain",
		WorkflowID:          "test-workflow",
		RunID:               "test-run",
		TaskID:              123,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		Version:             5,
		VisibilityTimestamp: time.Now(),
	}

	taskPayload, err := json.Marshal(task)
	require.NoError(t, err)

	err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "test-workflow",
		RunID:                "test-run",
		TaskID:               123,
		TaskType:             persistence.TransferTaskTypeActivityTask,
		TaskPayload:          taskPayload,
		Version:              5,
	})
	require.NoError(t, err)

	// Step 2: Verify task is in DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)

	// Step 3: Simulate failover - create mock executor
	executedTasks := []int64{}
	mockExecutor := &mockExecutor{
		executeFn: func(task Task) (ExecuteResponse, error) {
			executedTasks = append(executedTasks, task.GetTaskInfo().GetTaskID())
			return ExecuteResponse{IsActiveTask: true}, nil
		},
	}

	// Step 4: Process DLQ on failover
	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err = processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
	})
	require.NoError(t, err)

	// Step 5: Verify task was executed
	require.Len(t, executedTasks, 1)
	require.Equal(t, int64(123), executedTasks[0])

	// Step 6: Verify task was removed from DLQ
	resp, err = dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0)
}

func TestDLQ_DuplicateHandling(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue same task twice
	req := &persistence.EnqueueStandbyTaskRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		WorkflowID:           "wf1",
		RunID:                "run1",
		TaskID:               100,
		TaskType:             persistence.TransferTaskTypeActivityTask,
		TaskPayload:          []byte("payload"),
		Version:              5,
	}

	err := dlqManager.EnqueueStandbyTask(ctx, req)
	require.NoError(t, err)

	// Second enqueue should fail with ConditionFailedError
	err = dlqManager.EnqueueStandbyTask(ctx, req)
	require.Error(t, err)
	_, ok := err.(*persistence.ConditionFailedError)
	require.True(t, ok)

	// Verify only one task in DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr1",
		PageSize:             10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
}

func TestDLQ_ClusterAttributeScoping(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue tasks to different cluster attributes
	for _, attr := range []string{"attr1", "attr2", "attr3"} {
		err := dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "test-domain",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  attr,
			WorkflowID:           "wf-" + attr,
			RunID:                "run1",
			TaskID:               int64(100),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          []byte("payload"),
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Process failover for only attr2
	executedTasks := []string{}
	mockExecutor := &mockExecutor{
		executeFn: func(task Task) (ExecuteResponse, error) {
			executedTasks = append(executedTasks, task.GetTaskInfo().GetWorkflowID())
			return ExecuteResponse{IsActiveTask: true}, nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "test-domain",
		ClusterAttributeScope: "scope1",
		ClusterAttributeName:  "attr2",
	})
	require.NoError(t, err)

	// Only attr2 task should be executed
	require.Len(t, executedTasks, 1)
	require.Equal(t, "wf-attr2", executedTasks[0])

	// Verify attr1 and attr3 still in DLQ
	for _, attr := range []string{"attr1", "attr3"} {
		resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
			ShardID:              1,
			DomainID:             "test-domain",
			ClusterAttributeScope: "scope1",
			ClusterAttributeName:  attr,
			PageSize:             10,
		})
		require.NoError(t, err)
		require.Len(t, resp.Tasks, 1, "attr=%s should still have task", attr)
	}
}

// Mock executor for testing
type mockExecutor struct {
	executeFn func(Task) (ExecuteResponse, error)
}

func (m *mockExecutor) Execute(task Task) (ExecuteResponse, error) {
	return m.executeFn(task)
}

func (m *mockExecutor) Stop() {}
```

**Step 2: Run integration tests**

Run: `go test ./service/history/task -run TestDLQ_ -v`
Expected: PASS (all 3 integration tests)

**Step 3: Commit integration tests**

```bash
git add service/history/task/dlq_integration_test.go
git commit -m "test: add end-to-end integration tests for DLQ failover flow"
```

---

## Task 8: Write Simulation Tests

**Files:**
- Create: `service/history/task/dlq_simulation_test.go`

**Step 1: Write simulation test with short discard delay**

Create: `service/history/task/dlq_simulation_test.go`

```go
package task

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

// TestDLQ_Simulation_ShortDiscardDelay simulates rapid task discard with short delay
func TestDLQ_Simulation_ShortDiscardDelay(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Configure short discard delay (1 second for simulation)
	discardDelay := 1 * time.Second

	// Simulate multiple tasks being discarded
	numTasks := 10
	for i := 0; i < numTasks; i++ {
		task := &persistence.TransferTaskInfo{
			DomainID:            "sim-domain",
			WorkflowID:          fmt.Sprintf("wf-%d", i),
			RunID:               "run1",
			TaskID:              int64(100 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Now().Add(-discardDelay - time.Second),
		}

		taskPayload, err := json.Marshal(task)
		require.NoError(t, err)

		// Simulate standby task being discarded after delay
		time.Sleep(10 * time.Millisecond) // Small delay between tasks

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "sim-domain",
			ClusterAttributeScope: "sim-scope",
			ClusterAttributeName:  "sim-attr",
			WorkflowID:           fmt.Sprintf("wf-%d", i),
			RunID:                "run1",
			TaskID:               int64(100 + i),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          taskPayload,
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Verify all tasks in DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
		PageSize:             100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, numTasks)

	// Simulate failover
	executedCount := 0
	mockExecutor := &mockExecutor{
		executeFn: func(task Task) (ExecuteResponse, error) {
			executedCount++
			return ExecuteResponse{IsActiveTask: true}, nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err = processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
	})
	require.NoError(t, err)
	require.Equal(t, numTasks, executedCount)

	// Verify DLQ is empty
	resp, err = dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
		PageSize:             100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0)
}

// TestDLQ_Simulation_ChaosInjection tests DLQ with error injection
func TestDLQ_Simulation_ChaosInjection(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue tasks
	numTasks := 20
	for i := 0; i < numTasks; i++ {
		task := &persistence.TransferTaskInfo{
			DomainID:            "chaos-domain",
			WorkflowID:          fmt.Sprintf("wf-%d", i),
			RunID:               "run1",
			TaskID:              int64(200 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Now(),
		}

		taskPayload, err := json.Marshal(task)
		require.NoError(t, err)

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              1,
			DomainID:             "chaos-domain",
			ClusterAttributeScope: "chaos-scope",
			ClusterAttributeName:  "chaos-attr",
			WorkflowID:           fmt.Sprintf("wf-%d", i),
			RunID:                "run1",
			TaskID:               int64(200 + i),
			TaskType:             persistence.TransferTaskTypeActivityTask,
			TaskPayload:          taskPayload,
			Version:              5,
		})
		require.NoError(t, err)
	}

	// Executor with 30% error rate
	successCount := 0
	errorCount := 0
	errorRate := 0.3

	mockExecutor := &mockExecutor{
		executeFn: func(task Task) (ExecuteResponse, error) {
			// Inject errors randomly
			if rand.Float64() < errorRate {
				errorCount++
				return ExecuteResponse{}, fmt.Errorf("chaos: injected error")
			}
			successCount++
			return ExecuteResponse{IsActiveTask: true}, nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:              1,
		DomainID:             "chaos-domain",
		ClusterAttributeScope: "chaos-scope",
		ClusterAttributeName:  "chaos-attr",
	})
	require.NoError(t, err)

	// Verify some succeeded and some failed
	require.Greater(t, successCount, 0)
	require.Greater(t, errorCount, 0)
	require.Equal(t, numTasks, successCount+errorCount)

	// Failed tasks should remain in DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:              1,
		DomainID:             "chaos-domain",
		ClusterAttributeScope: "chaos-scope",
		ClusterAttributeName:  "chaos-attr",
		PageSize:             100,
	})
	require.NoError(t, err)
	require.Equal(t, errorCount, len(resp.Tasks))
}

// TestDLQ_Simulation_ConcurrentFailovers tests concurrent failover processing
func TestDLQ_Simulation_ConcurrentFailovers(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager()
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue tasks for multiple domains and cluster attributes
	numDomains := 5
	numAttrs := 3
	tasksPerCombo := 10

	for d := 0; d < numDomains; d++ {
		for a := 0; a < numAttrs; a++ {
			for t := 0; t < tasksPerCombo; t++ {
				task := &persistence.TransferTaskInfo{
					DomainID:            fmt.Sprintf("domain-%d", d),
					WorkflowID:          fmt.Sprintf("wf-%d-%d-%d", d, a, t),
					RunID:               "run1",
					TaskID:              int64(d*1000 + a*100 + t),
					TaskType:            persistence.TransferTaskTypeActivityTask,
					Version:             5,
					VisibilityTimestamp: time.Now(),
				}

				taskPayload, err := json.Marshal(task)
				require.NoError(t, err)

				err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
					ShardID:              1,
					DomainID:             fmt.Sprintf("domain-%d", d),
					ClusterAttributeScope: "scope1",
					ClusterAttributeName:  fmt.Sprintf("attr-%d", a),
					WorkflowID:           fmt.Sprintf("wf-%d-%d-%d", d, a, t),
					RunID:                "run1",
					TaskID:               int64(d*1000 + a*100 + t),
					TaskType:             persistence.TransferTaskTypeActivityTask,
					TaskPayload:          taskPayload,
					Version:              5,
				})
				require.NoError(t, err)
			}
		}
	}

	// Process failovers concurrently
	var wg sync.WaitGroup
	executedCount := sync.Map{}

	mockExecutor := &mockExecutor{
		executeFn: func(task Task) (ExecuteResponse, error) {
			domainID := task.GetTaskInfo().GetDomainID()
			count, _ := executedCount.LoadOrStore(domainID, &atomic.Int32{})
			count.(*atomic.Int32).Add(1)
			return ExecuteResponse{IsActiveTask: true}, nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)

	for d := 0; d < numDomains; d++ {
		for a := 0; a < numAttrs; a++ {
			wg.Add(1)
			go func(domainIdx, attrIdx int) {
				defer wg.Done()
				err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
					ShardID:              1,
					DomainID:             fmt.Sprintf("domain-%d", domainIdx),
					ClusterAttributeScope: "scope1",
					ClusterAttributeName:  fmt.Sprintf("attr-%d", attrIdx),
				})
				require.NoError(t, err)
			}(d, a)
		}
	}

	wg.Wait()

	// Verify all tasks were processed
	totalExpected := numDomains * numAttrs * tasksPerCombo
	totalExecuted := 0
	executedCount.Range(func(key, value interface{}) bool {
		totalExecuted += int(value.(*atomic.Int32).Load())
		return true
	})
	require.Equal(t, totalExpected, totalExecuted)
}
```

**Step 2: Run simulation tests**

Run: `go test ./service/history/task -run TestDLQ_Simulation -v -timeout 30s`
Expected: PASS (all 3 simulation tests)

**Step 3: Commit simulation tests**

```bash
git add service/history/task/dlq_simulation_test.go
git commit -m "test: add simulation tests for DLQ with short delays and chaos injection"
```

---

## Verification

After implementing all tasks, verify the complete system works:

**Run all tests:**
```bash
make test
```

**Run specific DLQ tests:**
```bash
go test ./common/persistence -run Standby -v
go test ./service/history/task -run DLQ -v
```

**Manual verification:**
1. Start Cadence server with in-memory DLQ enabled
2. Create active-active domain with cluster attributes
3. Create workflow in active cluster
4. Create transfer task (activity/decision)
5. Wait for task to exceed discard delay on standby
6. Verify task is in DLQ (check logs)
7. Trigger failover for cluster attribute
8. Verify task is processed and removed from DLQ

---

## Future Work (Out of Scope for POC)

- Periodic DLQ processing (scan and retry)
- CLI commands for DLQ management (read, merge, purge)
- Cassandra/SQL persistence implementation
- Timer task DLQ support
- Metrics and alerting
- Production configuration and rollout strategy
