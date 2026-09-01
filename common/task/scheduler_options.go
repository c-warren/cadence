package task

import (
	"fmt"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

type SchedulerOptions[K comparable, T Task] struct {
	SchedulerType        SchedulerType
	FIFOSchedulerOptions *FIFOTaskSchedulerOptions
	WRRSchedulerOptions  *WeightedRoundRobinTaskSchedulerOptions[K, T]
}

func NewSchedulerOptions[K comparable, T Task](
	schedulerType int,
	queueSize int,
	workerCount dynamicproperties.IntPropertyFn,
	dispatcherCount int,
	taskToChannelKeyFn func(T) K,
	channelKeyToWeightFn func(K) int,
) (*SchedulerOptions[K, T], error) {
	options := &SchedulerOptions[K, T]{
		SchedulerType: SchedulerType(schedulerType),
	}
	switch options.SchedulerType {
	case SchedulerTypeFIFO:
		options.FIFOSchedulerOptions = &FIFOTaskSchedulerOptions{
			QueueSize:       queueSize,
			WorkerCount:     workerCount,
			DispatcherCount: dispatcherCount,
			RetryPolicy:     common.CreateTaskProcessingRetryPolicy(),
		}
	case SchedulerTypeWRR:
		options.WRRSchedulerOptions = &WeightedRoundRobinTaskSchedulerOptions[K, T]{
			QueueSize:            queueSize,
			DispatcherCount:      dispatcherCount,
			TaskToChannelKeyFn:   taskToChannelKeyFn,
			ChannelKeyToWeightFn: channelKeyToWeightFn,
		}
	default:
		return nil, fmt.Errorf("unknown task scheduler type: %v", schedulerType)
	}
	return options, nil
}

func (o *SchedulerOptions[K, T]) String() string {
	return fmt.Sprintf("{schedulerType:%v, fifoSchedulerOptions:%s, wrrSchedulerOptions:%s}",
		o.SchedulerType, o.FIFOSchedulerOptions, o.WRRSchedulerOptions)
}
