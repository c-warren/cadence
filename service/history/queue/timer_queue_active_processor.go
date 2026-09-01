package queue

import (
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

func newTimerQueueActiveProcessor(
	clusterName string,
	shard shard.Context,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) *timerQueueProcessorBase {
	config := shard.GetConfig()
	options := newTimerQueueProcessorOptions(config, true, false)

	logger = logger.WithTags(tag.ClusterName(clusterName))

	taskFilter := func(timer persistence.Task) (bool, error) {
		if timer.GetTaskCategory() != persistence.HistoryTaskCategoryTimer {
			return false, errUnexpectedQueueTask
		}
		if notRegistered, err := isDomainNotRegistered(shard, timer.GetDomainID()); notRegistered && err == nil {
			// Allow deletion tasks for deprecated domains
			if timer.GetTaskType() == persistence.TaskTypeDeleteHistoryEvent {
				return true, nil
			}

			logger.Info("Domain is not in registered status, skip task in active timer queue.", tag.WorkflowDomainID(timer.GetDomainID()), tag.Value(timer))
			return false, nil
		}

		return taskAllocator.VerifyActiveTask(timer.GetDomainID(), timer.GetWorkflowID(), timer.GetRunID(), timer)
	}

	updateMaxReadLevel := func() task.Key {
		return newTimerTaskKey(shard.UpdateIfNeededAndGetQueueMaxReadLevel(persistence.HistoryTaskCategoryTimer, clusterName).GetScheduledTime(), 0)
	}

	updateClusterAckLevel := func(ackLevel task.Key) error {
		return shard.UpdateQueueClusterAckLevel(persistence.HistoryTaskCategoryTimer, clusterName, persistence.NewHistoryTaskKey(ackLevel.(timerTaskKey).visibilityTimestamp, 0))
	}

	updateProcessingQueueStates := func(states []ProcessingQueueState) error {
		pStates := convertToPersistenceTimerProcessingQueueStates(states)
		return shard.UpdateTimerProcessingQueueStates(clusterName, pStates)
	}

	queueShutdown := func() error {
		return nil
	}

	return newTimerQueueProcessorBase(
		clusterName,
		shard,
		loadTimerProcessingQueueStates(clusterName, shard, options, logger),
		taskProcessor,
		clock.NewTimerGate(shard.GetTimeSource()),
		options,
		updateMaxReadLevel,
		updateClusterAckLevel,
		updateProcessingQueueStates,
		queueShutdown,
		taskFilter,
		taskExecutor,
		logger,
		shard.GetMetricsClient(),
	)
}
