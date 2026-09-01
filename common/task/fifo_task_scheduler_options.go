package task

import (
	"fmt"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

// FIFOTaskSchedulerOptions configs FIFO task scheduler
type FIFOTaskSchedulerOptions struct {
	QueueSize       int
	WorkerCount     dynamicproperties.IntPropertyFn
	DispatcherCount int
	RetryPolicy     backoff.RetryPolicy
}

func (o *FIFOTaskSchedulerOptions) String() string {
	return fmt.Sprintf("{QueueSize: %v, WorkerCount: %v, DispatcherCount: %v}", o.QueueSize, o.WorkerCount(), o.DispatcherCount)
}
