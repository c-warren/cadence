package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(
	ctx context.Context,
	request *types.RemoveSignalMutableStateRequest,
) error {

	domainEntry, err := e.getActiveDomainByWorkflow(ctx, request.DomainUUID, request.WorkflowExecution.GetWorkflowID(), request.WorkflowExecution.GetRunID())
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	workflowExecution := types.WorkflowExecution{
		WorkflowID: request.WorkflowExecution.WorkflowID,
		RunID:      request.WorkflowExecution.RunID,
	}

	return workflow.UpdateWithAction(ctx, e.logger, e.executionCache, domainID, workflowExecution, false, e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return workflow.ErrNotExists
			}

			mutableState.DeleteSignalRequested(request.GetRequestID())

			return nil
		})
}
