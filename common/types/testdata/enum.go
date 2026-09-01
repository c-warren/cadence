package testdata

import "github.com/uber/cadence/common/types"

var (
	ArchivalStatus                             = types.ArchivalStatusEnabled
	CancelExternalWorkflowExecutionFailedCause = types.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution
	ChildWorkflowExecutionFailedCause          = types.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning
	ContinueAsNewInitiator                     = types.ContinueAsNewInitiatorRetryPolicy
	DecisionTaskFailedCause                    = types.DecisionTaskFailedCauseBadCancelWorkflowExecutionAttributes
	DecisionTaskTimedOutCause                  = types.DecisionTaskTimedOutCauseReset
	DecisionType                               = types.DecisionTypeCancelTimer
	DomainStatus                               = types.DomainStatusRegistered
	EncodingType                               = types.EncodingTypeJSON
	EventType                                  = types.EventTypeWorkflowExecutionStarted
	HistoryEventFilterType                     = types.HistoryEventFilterTypeCloseEvent
	IndexedValueType                           = types.IndexedValueTypeInt
	ParentClosePolicy                          = types.ParentClosePolicyTerminate
	ParentClosePolicy2                         = types.ParentClosePolicyRequestCancel
	PendingActivityState                       = types.PendingActivityStateCancelRequested
	PendingDecisionState                       = types.PendingDecisionStateStarted
	QueryConsistencyLevel                      = types.QueryConsistencyLevelStrong
	QueryRejectCondition                       = types.QueryRejectConditionNotCompletedCleanly
	QueryResultType                            = types.QueryResultTypeFailed
	QueryTaskCompletedType                     = types.QueryTaskCompletedTypeFailed
	SignalExternalWorkflowExecutionFailedCause = types.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution
	TaskListKind                               = types.TaskListKindSticky
	TaskListType                               = types.TaskListTypeActivity
	TimeoutType                                = types.TimeoutTypeScheduleToStart
	WorkflowExecutionCloseStatus               = types.WorkflowExecutionCloseStatusContinuedAsNew
	WorkflowIDReusePolicy                      = types.WorkflowIDReusePolicyTerminateIfRunning
)
