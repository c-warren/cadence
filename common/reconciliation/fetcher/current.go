package fetcher

import (
	"context"

	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

// CurrentExecutionIterator is used to retrieve Concrete executions.
func CurrentExecutionIterator(
	ctx context.Context,
	retryer persistence.Retryer,
	pageSize int,
) pagination.Iterator {
	return pagination.NewIterator(ctx, nil, getCurrentExecution(retryer, pageSize))
}

// CurrentExecution returns a single execution
func CurrentExecution(
	ctx context.Context,
	retryer persistence.Retryer,
	request ExecutionRequest,
) (entity.Entity, error) {
	req := persistence.GetCurrentExecutionRequest{
		DomainID:   request.DomainID,
		WorkflowID: request.WorkflowID,
		DomainName: request.DomainName,
	}
	e, err := retryer.GetCurrentExecution(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &entity.CurrentExecution{
		CurrentRunID: e.RunID,
		Execution: entity.Execution{
			ShardID:    retryer.GetShardID(),
			DomainID:   request.DomainID,
			WorkflowID: request.WorkflowID,
			RunID:      e.RunID,
			State:      e.State,
		},
	}, nil
}

func getCurrentExecution(
	pr persistence.Retryer,
	pageSize int,
) pagination.FetchFn {
	return func(ctx context.Context, token pagination.PageToken) (pagination.Page, error) {
		req := &persistence.ListCurrentExecutionsRequest{
			PageSize: pageSize,
		}
		if token != nil {
			req.PageToken = token.([]byte)
		}
		resp, err := pr.ListCurrentExecutions(ctx, req)
		if err != nil {
			return pagination.Page{}, err
		}
		executions := make([]pagination.Entity, len(resp.Executions))
		for i, e := range resp.Executions {
			currentExec := &entity.CurrentExecution{
				CurrentRunID: e.CurrentRunID,
				Execution: entity.Execution{
					ShardID:    pr.GetShardID(),
					DomainID:   e.DomainID,
					WorkflowID: e.WorkflowID,
					RunID:      e.RunID,
					State:      e.State,
				},
			}
			if err := currentExec.Validate(); err != nil {
				return pagination.Page{}, err
			}
			executions[i] = currentExec
		}
		var nextToken interface{} = resp.PageToken
		if len(resp.PageToken) == 0 {
			nextToken = nil
		}
		page := pagination.Page{
			CurrentToken: token,
			NextToken:    nextToken,
			Entities:     executions,
		}
		return page, nil
	}
}
