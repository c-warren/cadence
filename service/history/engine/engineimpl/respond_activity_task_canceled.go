package engineimpl

import (
	"context"
	"time"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	req *types.HistoryRespondActivityTaskCanceledRequest,
) error {

	request := req.CancelRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return workflow.ErrDeserializingToken
	}

	domainEntry, err := e.getActiveDomainByWorkflow(ctx, req.DomainUUID, token.WorkflowID, token.RunID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID
	domainName := domainEntry.GetInfo().Name

	workflowExecution := types.WorkflowExecution{
		WorkflowID: token.WorkflowID,
		RunID:      token.RunID,
	}

	var activityStartedTime time.Time
	var taskList string
	err = workflow.UpdateWithAction(ctx, e.logger, e.executionCache, domainID, workflowExecution, true, e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return workflow.ErrAlreadyCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == constants.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCanceledScope, metrics.StaleMutableStateCounter)
				e.logger.Error("Encounter stale mutable state in RecordActivityTaskCanceled",
					tag.WorkflowDomainName(domainName),
					tag.WorkflowID(workflowExecution.GetWorkflowID()),
					tag.WorkflowRunID(workflowExecution.GetRunID()),
					tag.WorkflowScheduleID(scheduleID),
					tag.WorkflowNextEventID(mutableState.GetNextEventID()),
				)
				return workflow.ErrStaleState
			}

			if !isRunning || ai.StartedID == constants.EmptyEventID ||
				(token.ScheduleID != constants.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return workflow.ErrActivityTaskNotFound
			}

			if _, err := mutableState.AddActivityTaskCanceledEvent(
				scheduleID,
				ai.StartedID,
				ai.CancelRequestID,
				request.Details,
				request.Identity); err != nil {
				// Unable to add ActivityTaskCanceled event to history
				return &types.InternalServiceError{Message: "Unable to add ActivityTaskCanceled event to history."}
			}

			activityStartedTime = ai.StartedTime
			taskList = ai.TaskList
			return nil
		})
	if err == nil && !activityStartedTime.IsZero() {
		scope := e.metricsClient.Scope(metrics.HistoryClientRespondActivityTaskCanceledScope).
			Tagged(
				metrics.DomainTag(domainName),
				metrics.WorkflowTypeTag(token.WorkflowType),
				metrics.ActivityTypeTag(token.ActivityType),
				metrics.TaskListTag(taskList),
			)
		e2eLatency := time.Since(activityStartedTime)
		scope.RecordTimer(metrics.ActivityE2ELatency, e2eLatency)
		scope.ExponentialHistogram(metrics.ActivityE2ELatencyHistogram, e2eLatency)
	}
	return err
}
