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
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
)

var initialAckLevelVisibilityTS = time.Unix(0, 0).UTC()

// HistoryTaskSerializer serializes and deserializes history tasks. It is a subset of
// serialization.TaskSerializer, declared here to avoid an import cycle between the
// persistence and persistence/serialization packages.
type HistoryTaskSerializer interface {
	SerializeTask(HistoryTaskCategory, Task) (DataBlob, error)
	DeserializeTask(HistoryTaskCategory, *DataBlob) (Task, error)
}

type historyTaskDLQManagerImpl struct {
	persistence    HistoryDLQTaskStore
	taskSerializer HistoryTaskSerializer
	logger         log.Logger
	timeSrc        clock.TimeSource
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
	if err := m.persistence.CreateHistoryDLQAckLevelIfNotExists(ctx, InternalHistoryDLQAckLevel{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
		TaskType:              request.Task.GetTaskCategory().ID(),
		AckLevelVisibilityTS:  initialAckLevelVisibilityTS,
		AckLevelTaskID:        -1,
		LastUpdatedAt:         m.timeSrc.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("failed to create initial DLQ ack level: %w", err)
	}
	return m.persistence.CreateHistoryDLQTask(ctx, InternalCreateHistoryDLQTaskRequest{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
		TaskType:              request.Task.GetTaskType(),
		TaskID:                request.Task.GetTaskID(),
		VisibilityTimestamp:   request.Task.GetVisibilityTimestamp(),
		CreatedAt:             m.timeSrc.Now().UTC(),
		TaskBlob:              &DataBlob{Data: blob.Data, Encoding: blob.Encoding},
	})
}

// GetAckLevels returns all DLQ partitions for a shard with their stored ack levels.
func (m *historyTaskDLQManagerImpl) GetAckLevels(ctx context.Context, shardID int) ([]HistoryDLQAckLevel, error) {
	return m.getAckLevels(ctx, InternalGetHistoryDLQAckLevelsRequest{ShardID: shardID})
}

// GetAckLevelsForPartition returns ack levels for all task types within a specific partition.
func (m *historyTaskDLQManagerImpl) GetAckLevelsForPartition(
	ctx context.Context,
	request HistoryDLQGetAckLevelsRequest,
) ([]HistoryDLQAckLevel, error) {
	return m.getAckLevels(ctx, InternalGetHistoryDLQAckLevelsRequest{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
	})
}

func (m *historyTaskDLQManagerImpl) getAckLevels(
	ctx context.Context,
	request InternalGetHistoryDLQAckLevelsRequest,
) ([]HistoryDLQAckLevel, error) {
	resp, err := m.persistence.GetHistoryDLQAckLevels(ctx, request)
	if err != nil {
		return nil, err
	}
	out := make([]HistoryDLQAckLevel, 0, len(resp.AckLevels))
	for _, row := range resp.AckLevels {
		out = append(out, HistoryDLQAckLevel{
			ShardID:               row.ShardID,
			DomainID:              row.DomainID,
			ClusterAttributeScope: row.ClusterAttributeScope,
			ClusterAttributeName:  row.ClusterAttributeName,
			TaskType:              row.TaskType,
			AckLevelVisibilityTS:  row.AckLevelVisibilityTS,
			AckLevelTaskID:        row.AckLevelTaskID,
		})
	}
	return out, nil
}

// GetTasks returns deserialized tasks from a DLQ partition.
func (m *historyTaskDLQManagerImpl) GetTasks(
	ctx context.Context,
	request HistoryDLQGetTasksRequest,
) (HistoryDLQGetTasksResponse, error) {
	resp, err := m.persistence.GetHistoryDLQTasks(ctx, InternalGetHistoryDLQTasksRequest{
		ShardID:                  request.ShardID,
		DomainID:                 request.DomainID,
		ClusterAttributeScope:    request.ClusterAttributeScope,
		ClusterAttributeName:     request.ClusterAttributeName,
		TaskType:                 request.TaskType,
		ExclusiveMinVisibilityTS: request.InclusiveMinTaskKey.GetScheduledTime(),
		ExclusiveMinTaskID:       request.InclusiveMinTaskKey.GetTaskID() - 1,
		InclusiveMaxVisibilityTS: request.ExclusiveMaxTaskKey.GetScheduledTime(),
		InclusiveMaxTaskID:       request.ExclusiveMaxTaskKey.GetTaskID() - 1,
		PageSize:                 request.PageSize,
		NextPageToken:            request.NextPageToken,
	})
	if err != nil {
		return HistoryDLQGetTasksResponse{}, err
	}

	category, err := historyTaskCategoryForType(request.TaskType)
	if err != nil {
		return HistoryDLQGetTasksResponse{}, err
	}

	tasks := make([]Task, 0, len(resp.Tasks))
	for _, raw := range resp.Tasks {
		task, err := m.taskSerializer.DeserializeTask(category, raw.TaskPayload)
		if err != nil {
			return HistoryDLQGetTasksResponse{}, fmt.Errorf("failed to deserialize history DLQ task: %w", err)
		}
		tasks = append(tasks, task)
	}
	return HistoryDLQGetTasksResponse{Tasks: tasks, NextPageToken: resp.NextPageToken}, nil
}

// UpdateAckLevel persists the new ack level for a partition.
func (m *historyTaskDLQManagerImpl) UpdateAckLevel(
	ctx context.Context,
	request HistoryDLQUpdateAckLevelRequest,
) error {
	return m.persistence.UpdateHistoryDLQAckLevel(ctx, InternalUpdateHistoryDLQAckLevelRequest{
		Row: InternalHistoryDLQAckLevel{
			ShardID:               request.ShardID,
			DomainID:              request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			TaskType:              request.TaskType,
			AckLevelVisibilityTS:  request.UpdatedInclusiveReadLevel.GetScheduledTime(),
			AckLevelTaskID:        request.UpdatedInclusiveReadLevel.GetTaskID(),
			LastUpdatedAt:         m.timeSrc.Now().UTC(),
		},
	})
}

// DeleteTasks removes tasks with key < ExclusiveMaxTaskKey from a DLQ partition.
func (m *historyTaskDLQManagerImpl) DeleteTasks(
	ctx context.Context,
	request HistoryDLQDeleteTasksRequest,
) error {
	return m.persistence.RangeDeleteHistoryDLQTasks(ctx, InternalRangeDeleteHistoryDLQTasksRequest{
		ShardID:               request.ShardID,
		DomainID:              request.DomainID,
		ClusterAttributeScope: request.ClusterAttributeScope,
		ClusterAttributeName:  request.ClusterAttributeName,
		TaskType:              request.TaskType,
		AckLevelVisibilityTS:  request.ExclusiveMaxTaskKey.GetScheduledTime(),
		AckLevelTaskID:        request.ExclusiveMaxTaskKey.GetTaskID() - 1,
	})
}

// historyTaskCategoryForType maps a HistoryTaskCategoryID constant to its HistoryTaskCategory.
func historyTaskCategoryForType(taskType int) (HistoryTaskCategory, error) {
	switch taskType {
	case HistoryTaskCategoryIDTransfer:
		return HistoryTaskCategoryTransfer, nil
	case HistoryTaskCategoryIDTimer:
		return HistoryTaskCategoryTimer, nil
	case HistoryTaskCategoryIDReplication:
		return HistoryTaskCategoryReplication, nil
	default:
		return HistoryTaskCategory{}, fmt.Errorf("unknown history task type: %d", taskType)
	}
}

func (m *historyTaskDLQManagerImpl) GetName() string { return m.persistence.GetName() }
func (m *historyTaskDLQManagerImpl) Close()          { m.persistence.Close() }
