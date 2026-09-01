//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/common/task

package task

import "github.com/uber/cadence/common"

type (
	// Processor is the generic coroutine pool interface
	// which process tasks
	Processor interface {
		common.Daemon
		Submit(task Task) error
	}

	// Scheduler is the generic interface for scheduling tasks with priority
	// and processing them
	Scheduler[T Task] interface {
		common.Daemon
		Submit(task T) error
		TrySubmit(task T) (bool, error)
	}

	// SchedulerType respresents the type of the task scheduler implementation
	SchedulerType int

	// State represents the current state of a task
	State int

	// Task is the interface for tasks
	Task interface {
		// Execute process this task
		Execute() error
		// HandleErr handle the error returned by Execute
		HandleErr(err error) error
		// RetryErr check whether to retry after HandleErr(Execute())
		RetryErr(err error) bool
		// Ack marks the task as successful completed
		Ack()
		// Nack marks the task as unsuccessful completed
		Nack(err error)
		// Cancel marks the task as canceled
		Cancel()
		// State returns the current task state
		State() State
	}

	// PriorityTask is the interface for tasks which have and can be assigned a priority
	PriorityTask interface {
		Task
		// Priority returns the priority of the task, or noPriority if no priority was previously assigned
		Priority() int
		// SetPriority sets the priority of the task
		SetPriority(int)
	}

	// SequentialTaskQueueFactory is the function which generate a new SequentialTaskQueue
	// for a give SequentialTask
	SequentialTaskQueueFactory func(task Task) SequentialTaskQueue

	// SequentialTaskQueue is the generic task queue interface which group
	// sequential tasks to be executed one by one
	SequentialTaskQueue interface {
		// QueueID return the ID of the queue, as well as the tasks inside (same)
		QueueID() interface{}
		// Add push an task to the task set
		Add(task Task)
		// Remove pop an task from the task set
		Remove() Task
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}

	// Schedule represents a stateless schedule definition
	Schedule[V any] interface {
		// NewIterator creates a new stateful iterator for this schedule
		NewIterator() Iterator[V]

		// Len returns the length of the schedule
		Len() int
	}

	// Iterator represents a stateful iteration through a schedule
	Iterator[V any] interface {
		// Next returns the next value in the iteration
		// Returns (value, true) if available, (zero value, false) if exhausted
		TryNext() (V, bool)
	}
)

const (
	// SchedulerTypeFIFO is the scheduler type for FIFO scheduler implementation
	SchedulerTypeFIFO SchedulerType = iota + 1
	// SchedulerTypeWRR is the scheduler type for weighted round robin scheduler implementation
	SchedulerTypeWRR
)

const (
	// TaskStatePending is the state for a task when it's waiting to be processed or currently being processed
	TaskStatePending State = iota + 1
	// TaskStateAcked is the state for a task if it has been successfully completed
	TaskStateAcked
	// TaskStateCanceled is the state for a task if it has been canceled
	TaskStateCanceled
)
