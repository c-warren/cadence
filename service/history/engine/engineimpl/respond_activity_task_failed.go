package engineimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(
	ctx context.Context,
	req *types.HistoryRespondActivityTaskFailedRequest,
) error {
	request := req.FailedRequest
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
	err = workflow.UpdateWithActionFunc(
		ctx,
		e.logger,
		e.executionCache,
		domainID,
		workflowExecution,
		e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) (*workflow.UpdateAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, workflow.ErrAlreadyCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == constants.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskFailedScope, metrics.StaleMutableStateCounter)
				e.logger.Error("Encounter stale mutable state in RecordActivityTaskFailed",
					tag.WorkflowDomainName(domainName),
					tag.WorkflowID(workflowExecution.GetWorkflowID()),
					tag.WorkflowRunID(workflowExecution.GetRunID()),
					tag.WorkflowScheduleID(scheduleID),
					tag.WorkflowNextEventID(mutableState.GetNextEventID()),
				)
				return nil, workflow.ErrStaleState
			}

			if !isRunning || ai.StartedID == constants.EmptyEventID ||
				(token.ScheduleID != constants.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				e.logger.Warn(fmt.Sprintf(
					"Encounter non existing activity in RecordActivityTaskFailed: isRunning: %t, ai: %#v, token: %#v.",
					isRunning, ai, token),
					tag.WorkflowDomainName(domainName),
					tag.WorkflowID(workflowExecution.GetWorkflowID()),
					tag.WorkflowRunID(workflowExecution.GetRunID()),
					tag.WorkflowScheduleID(scheduleID),
					tag.WorkflowNextEventID(mutableState.GetNextEventID()),
				)
				return nil, workflow.ErrActivityTaskNotFound
			}

			postActions := &workflow.UpdateAction{}
			ok, err := mutableState.RetryActivity(ai, req.FailedRequest.GetReason(), req.FailedRequest.GetDetails(), req.FailedRequest.FailureOptions)
			if err != nil {
				return nil, err
			}
			if !ok {
				// no more retry, and we want to record the failure event
				if _, err := mutableState.AddActivityTaskFailedEvent(scheduleID, ai.StartedID, request); err != nil {
					// Unable to add ActivityTaskFailed event to history
					return nil, &types.InternalServiceError{Message: "Unable to add ActivityTaskFailed event to history."}
				}
				postActions.CreateDecision = true
			}

			activityStartedTime = ai.StartedTime
			taskList = ai.TaskList
			return postActions, nil
		},
	)
	if err == nil && !activityStartedTime.IsZero() {
		scope := e.metricsClient.Scope(metrics.HistoryRespondActivityTaskFailedScope).
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
