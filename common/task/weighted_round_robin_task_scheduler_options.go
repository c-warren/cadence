package task

import (
	"fmt"
)

// WeightedRoundRobinTaskSchedulerOptions configs WRR task scheduler
type WeightedRoundRobinTaskSchedulerOptions[K comparable, T Task] struct {
	QueueSize            int
	DispatcherCount      int
	TaskToChannelKeyFn   func(T) K
	ChannelKeyToWeightFn func(K) int
}

func (o *WeightedRoundRobinTaskSchedulerOptions[K, T]) String() string {
	return fmt.Sprintf("{QueueSize: %v, DispatcherCount: %v}", o.QueueSize, o.DispatcherCount)
}
