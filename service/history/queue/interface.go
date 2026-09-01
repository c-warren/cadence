//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/service/history/queue

package queue

import (
	"context"

	"github.com/uber/cadence/common"
	hcommon "github.com/uber/cadence/service/history/common"
	"github.com/uber/cadence/service/history/task"
)

type (
	// DomainFilter filters domain
	DomainFilter struct {
		DomainIDs map[string]struct{}
		// by default, a DomainFilter matches domains listed in the Domains field
		// if reverseMatch is true then the DomainFilter matches domains that are
		// not in the Domains field.
		ReverseMatch bool
	}

	// ProcessingQueueState indicates the scope of a task processing queue and its current progress
	ProcessingQueueState interface {
		Level() int
		AckLevel() task.Key
		ReadLevel() task.Key
		MaxLevel() task.Key
		DomainFilter() DomainFilter
	}

	// ProcessingQueue is responsible for keeping track of the state of tasks
	// within the scope defined by its state; it can also be split into multiple
	// ProcessingQueues with non-overlapping scope or be merged with another
	// ProcessingQueue
	ProcessingQueue interface {
		State() ProcessingQueueState
		Split(ProcessingQueueSplitPolicy) []ProcessingQueue
		Merge(ProcessingQueue) []ProcessingQueue
		AddTasks(map[task.Key]task.Task, task.Key)
		GetTask(task.Key) (task.Task, error)
		GetTasks() []task.Task
		UpdateAckLevel() (task.Key, int) // return new ack level and number of pending tasks
		// TODO: add Offload() method
	}

	// ProcessingQueueSplitPolicy determines if one ProcessingQueue should be split
	// into multiple ProcessingQueues
	ProcessingQueueSplitPolicy interface {
		Evaluate(ProcessingQueue) []ProcessingQueueState
	}

	// ProcessingQueueCollection manages a list of non-overlapping ProcessingQueues
	// and keep track of the current active ProcessingQueue
	ProcessingQueueCollection interface {
		Level() int
		Queues() []ProcessingQueue
		ActiveQueue() ProcessingQueue
		AddTasks(map[task.Key]task.Task, task.Key)
		GetTask(task.Key) (task.Task, error)
		GetTasks() []task.Task
		UpdateAckLevels() (task.Key, int) // return min of all new ack levels and number of total pending tasks
		Split(ProcessingQueueSplitPolicy) []ProcessingQueue
		Merge([]ProcessingQueue)
		// TODO: add Offload() method
	}

	// Processor is the interface for task queue processor
	Processor interface {
		common.Daemon
		FailoverDomain(domainIDs map[string]struct{})
		NotifyNewTask(clusterName string, info *hcommon.NotifyTaskInfo)
		HandleAction(ctx context.Context, clusterName string, action *Action) (*ActionResult, error)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}
)
