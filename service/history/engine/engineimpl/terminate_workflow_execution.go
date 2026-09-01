package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

func (e *historyEngineImpl) TerminateWorkflowExecution(
	ctx context.Context,
	terminateRequest *types.HistoryTerminateWorkflowExecutionRequest,
) error {
	request := terminateRequest.TerminateRequest
	parentExecution := terminateRequest.ExternalWorkflowExecution
	childWorkflowOnly := terminateRequest.GetChildWorkflowOnly()
	workflowExecution := types.WorkflowExecution{
		WorkflowID: request.WorkflowExecution.WorkflowID,
	}
	// If firstExecutionRunID is set on the request always try to cancel currently running execution
	if request.GetFirstExecutionRunID() == "" {
		workflowExecution.RunID = request.WorkflowExecution.RunID
	}

	domainEntry, err := e.getActiveDomainByWorkflow(ctx, terminateRequest.DomainUUID, workflowExecution.WorkflowID, workflowExecution.RunID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	return workflow.UpdateCurrentWithActionFunc(
		ctx,
		e.logger,
		e.executionCache,
		e.executionManager,
		e.shard.GetShardID(),
		domainID,
		e.shard.GetDomainCache(),
		workflowExecution,
		e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) (*workflow.UpdateAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, workflow.ErrAlreadyCompleted
			}

			executionInfo := mutableState.GetExecutionInfo()
			if request.GetFirstExecutionRunID() != "" {
				firstRunID := executionInfo.FirstExecutionRunID
				if firstRunID == "" {
					// This is needed for backwards compatibility.  Workflow execution create with Cadence release v0.25.0 or earlier
					// does not have FirstExecutionRunID stored as part of mutable state.  If this is not set then load it from
					// workflow execution started event.
					startEvent, err := mutableState.GetStartEvent(ctx)
					if err != nil {
						return nil, err
					}
					firstRunID = startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstExecutionRunID()
				}
				if request.GetFirstExecutionRunID() != firstRunID {
					return nil, &types.EntityNotExistsError{Message: "Workflow execution not found"}
				}
			}
			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowID
				parentRunID := executionInfo.ParentRunID
				if parentExecution.GetWorkflowID() != parentWorkflowID ||
					parentExecution.GetRunID() != parentRunID {
					return nil, workflow.ErrParentMismatch
				}
			}

			eventBatchFirstEventID := mutableState.GetNextEventID()
			return workflow.UpdateWithoutDecision, execution.TerminateWorkflow(
				mutableState,
				eventBatchFirstEventID,
				request.GetReason(),
				request.GetDetails(),
				request.GetIdentity(),
			)
		})
}
