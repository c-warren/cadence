// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -package $GOPACKAGE -destination history_task_dlq_manager_mock.go github.com/uber/cadence/common/persistence HistoryTaskSerializer

package persistence

import (
	"context"
	"fmt"
	"sync"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

// HistoryTaskSerializer serializes and deserializes history tasks. It is a subset of
// serialization.TaskSerializer, declared here to avoid an import cycle between the
// persistence and persistence/serialization packages.
type HistoryTaskSerializer interface {
	SerializeTask(HistoryTaskCategory, Task) (DataBlob, error)
	DeserializeTask(HistoryTaskCategory, *DataBlob) (Task, error)
}

// dlqPartitionKey identifies a single DLQ partition + task category. It is used as
// the key for the in-process "ack-level row already exists" cache.
type dlqPartitionKey struct {
	shardID               int
	domainID              string
	clusterAttributeScope string
	clusterAttributeName  string
	taskCategory          int
}

type historyTaskDLQManagerImpl struct {
	persistence    HistoryDLQTaskStore
	taskSerializer HistoryTaskSerializer
	logger         log.Logger
	timeSrc        clock.TimeSource

	// seeded memoizes partitions whose ack-level row is known to exist, so steady-state
	// DLQ writes skip the existence read/seed entirely. It is a dlqPartitionKey -> struct{}
	// membership set; see nosqlExecutionStore.missingShardIDLogs for the same once-per-key
	// sync.Map idiom. Safe to cache forever: a history shard has a single owner, ack-level
	// rows are only ever created (never deleted), and the only writer of those rows is the
	// seed path below.
	seeded sync.Map
}

// NewHistoryTaskDLQManager creates a new HistoryTaskDLQManager.
func NewHistoryTaskDLQManager(
	persistence HistoryDLQTaskStore,
	taskSerializer HistoryTaskSerializer,
	logger log.Logger,
) HistoryTaskDLQManager {
	return &historyTaskDLQManagerImpl{
		persistence:    persistence,
		taskSerializer: taskSerializer,
		logger:         logger,
		timeSrc:        clock.NewRealTimeSource(),
	}
}

// CreateHistoryDLQTask serializes the task and writes it to the DLQ store.
func (m *historyTaskDLQManagerImpl) CreateHistoryDLQTask(
	ctx context.Context,
	request CreateHistoryDLQTaskRequest,
) error {
	blob, err := m.taskSerializer.SerializeTask(request.Task.GetTaskCategory(), request.Task)
	if err != nil {
		return fmt.Errorf("failed to serialize history DLQ task: %w", err)
	}
	// Ensure an ack-level row exists for this partition/task-category.
	if err := m.ensureAckLevel(ctx, request); err != nil {
		return err
	}
	// Use the task's key to store the visibility_ts/task_id in the DLQ.
	taskKey := request.Task.GetTaskKey()
	return m.persistence.CreateHistoryDLQTask(ctx, InternalCreateHistoryDLQTaskRequest{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
		TaskType:              request.Task.GetTaskCategory().ID(),
		TaskID:                taskKey.GetTaskID(),
		WorkflowID:            request.Task.GetWorkflowID(),
		RunID:                 request.Task.GetRunID(),
		Version:               request.Task.GetVersion(),
		VisibilityTimestamp:   taskKey.GetScheduledTime(),
		CreatedAt:             m.timeSrc.Now().UTC(),
		TaskBlob:              &DataBlob{Data: blob.Data, Encoding: blob.Encoding},
	})
}

// ensureAckLevel guarantees an ack-level row exists for the partition/task-category
// Results are cached per host to minimize reads and prevent duplicate writes.
func (m *historyTaskDLQManagerImpl) ensureAckLevel(
	ctx context.Context,
	request CreateHistoryDLQTaskRequest,
) error {
	key := dlqPartitionKey{
		shardID:               request.ShardID,
		domainID:              request.DomainID,
		clusterAttributeScope: request.ClusterAttributeScope,
		clusterAttributeName:  request.ClusterAttributeName,
		taskCategory:          request.Task.GetTaskCategory().ID(),
	}

	if _, ok := m.seeded.Load(key); ok {
		return nil
	}

	// A single read returns every category for this partition (the ack-level table is
	// partitioned by shard/domain/scope/name with task_type as the clustering key), so
	// cache them all to avoid re-reading when other categories are written.
	resp, err := m.persistence.GetHistoryDLQAckLevels(ctx, HistoryDLQGetAckLevelsRequest{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
	})
	if err != nil {
		return fmt.Errorf("failed to read DLQ ack levels: %w", err)
	}

	for _, row := range resp.AckLevels {
		m.seeded.Store(dlqPartitionKey{
			shardID:               request.ShardID,
			domainID:              request.DomainID,
			clusterAttributeScope: request.ClusterAttributeScope,
			clusterAttributeName:  request.ClusterAttributeName,
			taskCategory:          row.TaskCategory,
		}, struct{}{})
	}
	if _, ok := m.seeded.Load(key); ok {
		return nil
	}

	if err := m.persistence.UpdateHistoryDLQAckLevel(ctx, InternalUpdateHistoryDLQAckLevelRequest{
		Row: InternalHistoryDLQAckLevel{
			ShardID:               request.ShardID,
			DomainID:              request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			TaskCategory:          key.taskCategory,
			AckLevelVisibilityTS:  MinimumHistoryTaskKey.GetScheduledTime(),
			AckLevelTaskID:        MinimumHistoryTaskKey.GetTaskID(),
			LastUpdatedAt:         m.timeSrc.Now().UTC(),
		},
	}); err != nil {
		return fmt.Errorf("failed to seed initial DLQ ack level: %w", err)
	}

	m.seeded.Store(key, struct{}{})
	return nil
}

// GetHistoryDLQAckLevels returns DLQ partitions for the given shard and task category with their stored ack levels.
// Optionally filter to a specific partition by setting DomainID/ClusterAttributeScope/ClusterAttributeName.
func (m *historyTaskDLQManagerImpl) GetHistoryDLQAckLevels(
	ctx context.Context,
	request HistoryDLQGetAckLevelsRequest,
) ([]HistoryDLQAckLevel, error) {
	resp, err := m.persistence.GetHistoryDLQAckLevels(ctx, request)
	if err != nil {
		return nil, err
	}
	filterByCategory := request.TaskCategory.ID() != 0
	taskTypeID := request.TaskCategory.ID()
	out := make([]HistoryDLQAckLevel, 0, len(resp.AckLevels))
	for _, row := range resp.AckLevels {
		if filterByCategory && row.TaskCategory != taskTypeID {
			continue
		}
		category, err := HistoryTaskCategoryFromID(row.TaskCategory)
		if err != nil {
			m.logger.Warn("skipping ack level with unknown task category ID",
				tag.ShardID(row.ShardID),
				tag.Dynamic("task-category-id", row.TaskCategory),
			)
			continue
		}
		out = append(out, HistoryDLQAckLevel{
			ShardID:               row.ShardID,
			DomainID:              row.DomainID,
			ClusterAttributeScope: row.ClusterAttributeScope,
			ClusterAttributeName:  row.ClusterAttributeName,
			TaskCategory:          category,
			AckLevelVisibilityTS:  row.AckLevelVisibilityTS,
			AckLevelTaskID:        row.AckLevelTaskID,
		})
	}
	return out, nil
}

// GetHistoryDLQTasks returns deserialized tasks from a DLQ partition.
func (m *historyTaskDLQManagerImpl) GetHistoryDLQTasks(
	ctx context.Context,
	request HistoryDLQGetTasksRequest,
) (HistoryDLQGetTasksResponse, error) {
	resp, err := m.persistence.GetHistoryDLQTasks(ctx, request)
	if err != nil {
		return HistoryDLQGetTasksResponse{}, err
	}

	tasks := make([]Task, 0, len(resp.Tasks))
	pageSizeBytes := 0
	for _, raw := range resp.Tasks {
		pageSizeBytes += len(raw.TaskPayload.GetData())
		task, err := m.taskSerializer.DeserializeTask(request.TaskCategory, raw.TaskPayload)
		if err != nil {
			return HistoryDLQGetTasksResponse{}, fmt.Errorf("failed to deserialize history DLQ task: %w", err)
		}
		tasks = append(tasks, task)
	}
	return HistoryDLQGetTasksResponse{Tasks: tasks, NextPageToken: resp.NextPageToken, PageSizeBytes: pageSizeBytes}, nil
}

// UpdateHistoryDLQAckLevel persists the new ack level for a partition.
func (m *historyTaskDLQManagerImpl) UpdateHistoryDLQAckLevel(
	ctx context.Context,
	request HistoryDLQUpdateAckLevelRequest,
) error {
	return m.persistence.UpdateHistoryDLQAckLevel(ctx, InternalUpdateHistoryDLQAckLevelRequest{
		Row: InternalHistoryDLQAckLevel{
			ShardID:               request.ShardID,
			DomainID:              request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			TaskCategory:          request.TaskCategory.ID(),
			AckLevelVisibilityTS:  request.UpdatedInclusiveReadLevel.GetScheduledTime(),
			AckLevelTaskID:        request.UpdatedInclusiveReadLevel.GetTaskID(),
			LastUpdatedAt:         m.timeSrc.Now().UTC(),
		},
	})
}

// DeleteHistoryDLQTasks removes tasks with key < ExclusiveMaxTaskKey from a DLQ partition.
func (m *historyTaskDLQManagerImpl) DeleteHistoryDLQTasks(
	ctx context.Context,
	request HistoryDLQDeleteTasksRequest,
) error {
	return m.persistence.RangeDeleteHistoryDLQTasks(ctx, request)
}

func (m *historyTaskDLQManagerImpl) GetName() string { return m.persistence.GetName() }
func (m *historyTaskDLQManagerImpl) Close()          { m.persistence.Close() }
