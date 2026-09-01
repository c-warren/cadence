package types

// EventTypeValues returns all recognized values of EventType.
func EventTypeValues() []EventType {
	return []EventType{
		EventTypeWorkflowExecutionStarted,
		EventTypeWorkflowExecutionCompleted,
		EventTypeWorkflowExecutionFailed,
		EventTypeWorkflowExecutionTimedOut,
		EventTypeDecisionTaskScheduled,
		EventTypeDecisionTaskStarted,
		EventTypeDecisionTaskCompleted,
		EventTypeDecisionTaskTimedOut,
		EventTypeDecisionTaskFailed,
		EventTypeActivityTaskScheduled,
		EventTypeActivityTaskStarted,
		EventTypeActivityTaskCompleted,
		EventTypeActivityTaskFailed,
		EventTypeActivityTaskTimedOut,
		EventTypeActivityTaskCancelRequested,
		EventTypeRequestCancelActivityTaskFailed,
		EventTypeActivityTaskCanceled,
		EventTypeTimerStarted,
		EventTypeTimerFired,
		EventTypeCancelTimerFailed,
		EventTypeTimerCanceled,
		EventTypeWorkflowExecutionCancelRequested,
		EventTypeWorkflowExecutionCanceled,
		EventTypeRequestCancelExternalWorkflowExecutionInitiated,
		EventTypeRequestCancelExternalWorkflowExecutionFailed,
		EventTypeExternalWorkflowExecutionCancelRequested,
		EventTypeMarkerRecorded,
		EventTypeWorkflowExecutionSignaled,
		EventTypeWorkflowExecutionTerminated,
		EventTypeWorkflowExecutionContinuedAsNew,
		EventTypeStartChildWorkflowExecutionInitiated,
		EventTypeStartChildWorkflowExecutionFailed,
		EventTypeChildWorkflowExecutionStarted,
		EventTypeChildWorkflowExecutionCompleted,
		EventTypeChildWorkflowExecutionFailed,
		EventTypeChildWorkflowExecutionCanceled,
		EventTypeChildWorkflowExecutionTimedOut,
		EventTypeChildWorkflowExecutionTerminated,
		EventTypeSignalExternalWorkflowExecutionInitiated,
		EventTypeSignalExternalWorkflowExecutionFailed,
		EventTypeExternalWorkflowExecutionSignaled,
		EventTypeUpsertWorkflowSearchAttributes,
	}
}

// DecisionTypeValues returns all recognized values of DecisionType.
func DecisionTypeValues() []DecisionType {
	return []DecisionType{
		DecisionTypeScheduleActivityTask,
		DecisionTypeRequestCancelActivityTask,
		DecisionTypeStartTimer,
		DecisionTypeCompleteWorkflowExecution,
		DecisionTypeFailWorkflowExecution,
		DecisionTypeCancelTimer,
		DecisionTypeCancelWorkflowExecution,
		DecisionTypeRequestCancelExternalWorkflowExecution,
		DecisionTypeRecordMarker,
		DecisionTypeContinueAsNewWorkflowExecution,
		DecisionTypeStartChildWorkflowExecution,
		DecisionTypeSignalExternalWorkflowExecution,
		DecisionTypeUpsertWorkflowSearchAttributes,
	}
}
