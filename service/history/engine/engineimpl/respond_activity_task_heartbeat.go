package engineimpl

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

// RecordActivityTaskHeartbeat records an heartbeat for a task.
// This method can be used for two purposes.
// - For reporting liveness of the activity.
// - For reporting progress of the activity, this can be done even if the liveness is not configured.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *types.HistoryRecordActivityTaskHeartbeatRequest,
) (*types.RecordActivityTaskHeartbeatResponse, error) {
	request := req.HeartbeatRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, workflow.ErrDeserializingToken
	}

	domainEntry, err := e.getActiveDomainByWorkflow(ctx, req.DomainUUID, token.WorkflowID, token.RunID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	workflowExecution := types.WorkflowExecution{
		WorkflowID: token.WorkflowID,
		RunID:      token.RunID,
	}

	var cancelRequested bool
	err = workflow.UpdateWithAction(ctx, e.logger, e.executionCache, domainID, workflowExecution, false, e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				e.logger.Debug("Heartbeat failed")
				return workflow.ErrAlreadyCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == constants.EmptyEventID { // client call RecordActivityHeartbeatByID, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.StaleMutableStateCounter)
				e.logger.Error("Encounter stale mutable state in RecordActivityTaskHeartbeat",
					tag.WorkflowDomainName(domainEntry.GetInfo().Name),
					tag.WorkflowID(workflowExecution.GetWorkflowID()),
					tag.WorkflowRunID(workflowExecution.GetRunID()),
					tag.WorkflowScheduleID(scheduleID),
					tag.WorkflowNextEventID(mutableState.GetNextEventID()),
				)
				return workflow.ErrStaleState
			}

			if !isRunning || ai.StartedID == constants.EmptyEventID ||
				(token.ScheduleID != constants.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				e.logger.Warn(fmt.Sprintf(
					"Encounter non existing activity in RecordActivityTaskHeartbeat: isRunning: %t, ai: %#v, token: %#v.",
					isRunning, ai, token),
					tag.WorkflowDomainName(domainEntry.GetInfo().Name),
					tag.WorkflowID(workflowExecution.GetWorkflowID()),
					tag.WorkflowRunID(workflowExecution.GetRunID()),
					tag.WorkflowScheduleID(scheduleID),
					tag.WorkflowNextEventID(mutableState.GetNextEventID()),
				)

				return workflow.ErrActivityTaskNotFound
			}

			cancelRequested = ai.CancelRequested

			e.logger.Debug(fmt.Sprintf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, CancelRequested: %v",
				scheduleID, ai, cancelRequested))

			// Save progress and last HB reported time.
			mutableState.UpdateActivityProgress(ai, request)

			return nil
		})

	if err != nil {
		return &types.RecordActivityTaskHeartbeatResponse{}, err
	}

	return &types.RecordActivityTaskHeartbeatResponse{CancelRequested: cancelRequested}, nil
}
