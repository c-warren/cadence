//go:generate mockgen -package $GOPACKAGE -destination queue_reader_mock.go github.com/uber/cadence/service/history/queuev2 QueueReader
package queuev2

import (
	"context"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/shard"
)

type (
	QueueReader interface {
		GetTask(context.Context, *GetTaskRequest) (*GetTaskResponse, error)
		LookAHead(ctx context.Context, req *LookAHeadRequest) (*LookAHeadResponse, error)
	}

	LookAHeadRequest struct {
		InclusiveMinTaskKey persistence.HistoryTaskKey
	}

	LookAHeadResponse struct {
		Task             persistence.Task
		LookAheadMaxTime time.Time
	}

	GetTaskRequest struct {
		Progress  *GetTaskProgress
		Predicate Predicate
		PageSize  int
	}

	// GetTaskProgress contains the range of the slice to read, the next page token, and the next task key
	GetTaskProgress struct {
		Range
		NextPageToken []byte
		NextTaskKey   persistence.HistoryTaskKey
	}

	GetTaskResponse struct {
		Tasks    []persistence.Task
		Progress *GetTaskProgress
	}

	simpleQueueReader struct {
		shard                      shard.Context
		category                   persistence.HistoryTaskCategory
		maxPollInterval            dynamicproperties.DurationPropertyFn
		maxPollIntervalJitterCoeff dynamicproperties.FloatPropertyFn
	}
)

func NewQueueReader(
	shard shard.Context,
	category persistence.HistoryTaskCategory,
	maxPollInterval dynamicproperties.DurationPropertyFn,
	maxPollIntervalJitterCoeff dynamicproperties.FloatPropertyFn,
) QueueReader {
	return &simpleQueueReader{
		shard:                      shard,
		category:                   category,
		maxPollInterval:            maxPollInterval,
		maxPollIntervalJitterCoeff: maxPollIntervalJitterCoeff,
	}
}

func (r *simpleQueueReader) GetTask(ctx context.Context, req *GetTaskRequest) (*GetTaskResponse, error) {
	resp, err := r.shard.GetExecutionManager().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		TaskCategory:        r.category,
		InclusiveMinTaskKey: req.Progress.InclusiveMinTaskKey,
		ExclusiveMaxTaskKey: req.Progress.ExclusiveMaxTaskKey,
		PageSize:            req.PageSize,
		NextPageToken:       req.Progress.NextPageToken,
		ShardID:             common.Ptr(r.shard.GetShardID()),
	})
	if err != nil {
		return nil, err
	}

	nextTaskKey := req.Progress.ExclusiveMaxTaskKey
	tasks := make([]persistence.Task, 0, len(resp.Tasks))
	for _, task := range resp.Tasks {
		// filter out tasks that don't match the predicate
		if req.Predicate.Check(task) {
			tasks = append(tasks, task)
		}
	}
	// If there are more tasks to read, set the next task key to the next task key of the last task
	if len(resp.NextPageToken) != 0 && len(resp.Tasks) > 0 {
		nextTaskKey = resp.Tasks[len(resp.Tasks)-1].GetTaskKey().Next()
	}

	return &GetTaskResponse{
		Tasks: tasks,
		Progress: &GetTaskProgress{
			Range:         req.Progress.Range,
			NextPageToken: resp.NextPageToken,
			NextTaskKey:   nextTaskKey,
		},
	}, nil
}

func (r *simpleQueueReader) LookAHead(ctx context.Context, req *LookAHeadRequest) (*LookAHeadResponse, error) {
	maxTime := req.InclusiveMinTaskKey.GetScheduledTime().Add(
		backoff.JitDuration(r.maxPollInterval(), r.maxPollIntervalJitterCoeff()),
	)
	resp, err := r.shard.GetExecutionManager().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		TaskCategory:        r.category,
		InclusiveMinTaskKey: req.InclusiveMinTaskKey,
		ExclusiveMaxTaskKey: persistence.NewHistoryTaskKey(maxTime, 0),
		PageSize:            1,
		ShardID:             common.Ptr(r.shard.GetShardID()),
	})
	if err != nil {
		return nil, err
	}
	var firstTask persistence.Task
	if len(resp.Tasks) > 0 {
		firstTask = resp.Tasks[0]
	}
	return &LookAHeadResponse{
		Task:             firstTask,
		LookAheadMaxTime: maxTime,
	}, nil
}
