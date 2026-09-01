package timeout

import (
	"time"

	"github.com/uber/cadence/common/types"
)

type TimeoutType string

const (
	TimeoutTypeExecution     TimeoutType = "The Workflow Execution has timed out"
	TimeoutTypeActivity      TimeoutType = "Activity task has timed out"
	TimeoutTypeDecision      TimeoutType = "Decision task has timed out"
	TimeoutTypeChildWorkflow TimeoutType = "Child Workflow Execution has timed out"
)

func (tt TimeoutType) String() string {
	return string(tt)
}

type ExecutionTimeoutMetadata struct {
	ExecutionTime     time.Duration
	ConfiguredTimeout time.Duration
	Tasklist          *types.TaskList
	LastOngoingEvent  *types.HistoryEvent
}

type ChildWfTimeoutMetadata struct {
	ExecutionTime     time.Duration
	ConfiguredTimeout time.Duration
	Execution         *types.WorkflowExecution
}

type ActivityTimeoutMetadata struct {
	TimeoutType       *types.TimeoutType
	ConfiguredTimeout time.Duration
	TimeElapsed       time.Duration
	RetryPolicy       *types.RetryPolicy
	HeartBeatTimeout  time.Duration
	Tasklist          *types.TaskList
}

type DecisionTimeoutMetadata struct {
	ConfiguredTimeout time.Duration
}

type PollersMetadata struct {
	TaskListName    string
	TaskListBacklog int64
}

type HeartbeatingMetadata struct {
	TimeElapsed time.Duration
	RetryPolicy *types.RetryPolicy
}

type TimeoutIssuesMetadata struct {
	ExecutionTimeout *ExecutionTimeoutMetadata
	ActivityTimeout  *ActivityTimeoutMetadata
	ChildWfTimeout   *ChildWfTimeoutMetadata
	DecisionTimeout  *DecisionTimeoutMetadata
}

type TimeoutRootcauseMetadata struct {
	PollersMetadata      *PollersMetadata
	HeartBeatingMetadata *HeartbeatingMetadata
}
