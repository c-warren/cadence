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

	// Check for duplicates (unique by TaskID + VisibilityTimestamp)
	tasks := m.tasks[key]
	for _, task := range tasks {
		if task.TaskID == request.TaskID && task.VisibilityTimestamp == request.VisibilityTimestamp {
			return &ConditionFailedError{
				Msg: fmt.Sprintf("task already exists: taskID=%d, visibilityTimestamp=%d", request.TaskID, request.VisibilityTimestamp),
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
		VisibilityTimestamp:  request.VisibilityTimestamp,
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
		if task.TaskID == request.TaskID && task.VisibilityTimestamp == request.VisibilityTimestamp {
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
