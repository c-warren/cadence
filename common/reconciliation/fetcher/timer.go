package fetcher

import (
	"context"
	"time"

	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

// TimerIterator is used to retrieve Concrete executions.
func TimerIterator(
	ctx context.Context,
	retryer persistence.Retryer,
	minTimestamp time.Time,
	maxTimestamp time.Time,
	pageSize int,
) pagination.Iterator {
	return pagination.NewIterator(ctx, nil, getUserTimers(retryer, minTimestamp, maxTimestamp, pageSize))
}

func getUserTimers(
	pr persistence.Retryer,
	minTimestamp time.Time,
	maxTimestamp time.Time,
	pageSize int,
) pagination.FetchFn {
	return func(ctx context.Context, token pagination.PageToken) (pagination.Page, error) {
		req := &persistence.GetHistoryTasksRequest{
			TaskCategory:        persistence.HistoryTaskCategoryTimer,
			InclusiveMinTaskKey: persistence.NewHistoryTaskKey(minTimestamp, 0),
			ExclusiveMaxTaskKey: persistence.NewHistoryTaskKey(maxTimestamp, 0),
			PageSize:            pageSize,
		}
		if token != nil {
			req.NextPageToken = token.([]byte)
		}
		resp, err := pr.GetHistoryTasks(ctx, req)

		if err != nil {
			return pagination.Page{}, err
		}

		var timers []pagination.Entity

		for _, t := range resp.Tasks {
			if t.GetTaskType() != persistence.TaskTypeUserTimer {
				continue
			}

			timer := &entity.Timer{
				ShardID:             pr.GetShardID(),
				DomainID:            t.GetDomainID(),
				WorkflowID:          t.GetWorkflowID(),
				RunID:               t.GetRunID(),
				TaskType:            t.GetTaskType(),
				VisibilityTimestamp: t.GetVisibilityTimestamp(),
			}

			if err := timer.Validate(); err != nil {
				return pagination.Page{}, err
			}
			timers = append(timers, timer)
		}
		var nextToken interface{} = resp.NextPageToken
		if len(resp.NextPageToken) == 0 {
			nextToken = nil
		}

		page := pagination.Page{
			CurrentToken: token,
			NextToken:    nextToken,
			Entities:     timers,
		}
		return page, nil
	}
}
