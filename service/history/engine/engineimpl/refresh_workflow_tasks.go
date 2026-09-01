package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
)

func (e *historyEngineImpl) RefreshWorkflowTasks(
	ctx context.Context,
	domainUUID string,
	workflowExecution types.WorkflowExecution,
) (retError error) {
	domainEntry, err := e.shard.GetDomainCache().GetDomainByID(domainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	wfContext, release, err := e.executionCache.GetOrCreateWorkflowExecution(ctx, domainID, workflowExecution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := wfContext.LoadWorkflowExecution(ctx)
	if err != nil {
		return err
	}

	mutableStateTaskRefresher := execution.NewMutableStateTaskRefresher(
		e.shard.GetConfig(),
		e.shard.GetClusterMetadata(),
		e.shard.GetDomainCache(),
		e.shard.GetEventsCache(),
		e.shard.GetShardID(),
		e.logger,
	)

	err = mutableStateTaskRefresher.RefreshTasks(ctx, mutableState.GetExecutionInfo().StartTimestamp, mutableState)
	if err != nil {
		return err
	}

	err = wfContext.UpdateWorkflowExecutionTasks(ctx, e.shard.GetTimeSource().Now())
	if err != nil {
		return err
	}
	return nil
}
