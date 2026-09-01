package replication

import (
	"strconv"
	"sync/atomic"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/rangeiter"
	"github.com/uber/cadence/service/history/config"
)

// DynamicTaskBatchSizer is responsible for the batch size used to retrieve ReplicationTasks by TaskAckManager
// It adjusts the task batch size based on the error and the getTasksResult,
// and use the following rules:
//
//  1. If there is an error, decrease the task batch size.
//     In case of an increased load to the database, we should reduce the load to the database
//     Emitted metric with tag reason:"error"
//
//  2. If the task batch size is shrunk, decrease the task batch size.
//     The payload size of messages is too big, so we should decrease
//     the number of messages to be sure that future messages will not be shrunk
//     Emitted metric with tag reason:"shrunk"
//
//  3. If the read level of a passive cluster has not been changed and there are no fetched tasks,
//     not change the task batch size. There is no need to change because the replication is not stuck,
//     there just are no new tasks
//     Metric is not emitted
//
//  4. If the read level of a passive cluster has not been changed and if there are fetched tasks,
//     and number of previously fetched tasks is not zero, decrease the task batch size.
//     The replication is stuck on the passive side
//     Emitted metric with tag reason:"possible_stuck"
//
//  5. If the read level of a passive cluster has not been changed and if there are fetched tasks,
//     and number of previously fetched tasks is zero, not change the task batch size.
//     The replication is not stuck, and there are new tasks to be replicated
//     Metric is not emitted
//
//  6. If the read level of a passive cluster has been changed and if there are more tasks in db,
//     increase the task batch size. We should retrieve the maximum possible value at the next time,
//     as there are more tasks to be replicated
//     Emitted metric with tag reason:"more_tasks"
//
//  7. If the read level of a passive cluster has been changed and if there are no more tasks in db,
//     not change the size. The existing size is already enough, and there are no more tasks to be replicated
//     Metric is not emitted
type DynamicTaskBatchSizer interface {
	analyse(err error, state *getTasksResult)
	value() int
}

// dynamicTaskBatchSizerImpl is the implementation of DynamicTaskBatchSizer
type dynamicTaskBatchSizerImpl struct {
	// isFetchedTasks indicates that there are fetched tasks in the last GetTasks call
	isFetchedTasks atomic.Bool
	iter           rangeiter.Iterator[int]
	logger         log.Logger
	scope          metrics.Scope
}

// NewDynamicTaskBatchSizer creates a new dynamicTaskBatchSizerImpl
func NewDynamicTaskBatchSizer(shardID int, logger log.Logger, config *config.Config, metricsClient metrics.Client) DynamicTaskBatchSizer {
	logger = logger.WithTags(tag.ComponentReplicationDynamicTaskBatchSizer)
	return &dynamicTaskBatchSizerImpl{
		logger: logger,
		scope: metricsClient.Scope(
			metrics.ReplicatorQueueProcessorScope,
			metrics.InstanceTag(strconv.Itoa(shardID)),
		),
		iter: rangeiter.NewDynamicConfigLinearIterator(
			func() int { return config.ReplicatorProcessorMinTaskBatchSize(shardID) },
			func() int { return config.ReplicatorProcessorMaxTaskBatchSize(shardID) },
			func() int { return config.ReplicatorProcessorBatchSizeStepCount(shardID) },
			logger,
		),
	}
}

func (d *dynamicTaskBatchSizerImpl) analyse(err error, state *getTasksResult) {
	switch {
	case err != nil:
		d.decrease("error")

	case state.isShrunk:
		d.decrease("shrunk")

	case state.previousReadTaskID == state.lastReadTaskID &&
		len(state.taskInfos) > 0 && d.isFetchedTasks.Load():
		d.decrease("possible_stuck")

	case state.msgs.HasMore:
		d.increase("more_tasks")
	}

	// update isFetchedTasks
	if state == nil {
		d.isFetchedTasks.Store(false)
		return
	}

	d.isFetchedTasks.Store(len(state.taskInfos) != 0)
}

func (d *dynamicTaskBatchSizerImpl) value() int {
	return d.iter.Value()
}

func (d *dynamicTaskBatchSizerImpl) decrease(reason string) {
	oldVal, newVal := d.iter.Value(), d.iter.Previous()

	if oldVal != newVal {
		d.emitMetric(reason, "decrease")
	}
	d.logger.Debug("Decrease task batch size", tag.Reason(reason), tag.ReplicationTaskBatchSize(newVal))
}

func (d *dynamicTaskBatchSizerImpl) increase(reason string) {
	oldVal, newVal := d.iter.Value(), d.iter.Next()

	if oldVal != newVal {
		d.emitMetric(reason, "increase")
	}
	d.logger.Debug("Increase task batch size", tag.Reason(reason), tag.ReplicationTaskBatchSize(newVal))
}

func (d *dynamicTaskBatchSizerImpl) emitMetric(reason, decision string) {
	d.scope.Tagged(
		metrics.ReasonTag(reason),
		metrics.DecisionTag(decision),
	).IncCounter(metrics.ReplicationDynamicTaskBatchSizerDecision)
}
