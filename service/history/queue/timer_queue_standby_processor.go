package queue

import (
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

func newTimerQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) (*timerQueueProcessorBase, clock.EventTimerGate) {
	config := shard.GetConfig()
	options := newTimerQueueProcessorOptions(config, false, false)

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

			logger.Info("Domain is not in registered status, skip task in standby timer queue.", tag.WorkflowDomainID(timer.GetDomainID()), tag.Value(timer))
			return false, nil
		}
		if timer.GetTaskType() == persistence.TaskTypeWorkflowTimeout ||
			timer.GetTaskType() == persistence.TaskTypeDeleteHistoryEvent {
			domainEntry, err := shard.GetDomainCache().GetDomainByID(timer.GetDomainID())
			if err == nil {
				if domainEntry.HasReplicationCluster(clusterName) {
					// guarantee the processing of workflow execution history deletion
					return true, nil
				}
			} else {
				if _, ok := err.(*types.EntityNotExistsError); !ok {
					// retry the task if failed to find the domain
					logger.Warn("Cannot find domain", tag.WorkflowDomainID(timer.GetDomainID()))
					return false, err
				}
				logger.Warn("Cannot find domain, default to not process task.", tag.WorkflowDomainID(timer.GetDomainID()), tag.Value(timer))
				return false, nil
			}
		}
		return taskAllocator.VerifyStandbyTask(clusterName, timer.GetDomainID(), timer.GetWorkflowID(), timer.GetRunID(), timer)
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

	remoteTimerGate := clock.NewEventTimerGate(shard.GetCurrentTime(clusterName))

	return newTimerQueueProcessorBase(
		clusterName,
		shard,
		loadTimerProcessingQueueStates(clusterName, shard, options, logger),
		taskProcessor,
		remoteTimerGate,
		options,
		updateMaxReadLevel,
		updateClusterAckLevel,
		updateProcessingQueueStates,
		queueShutdown,
		taskFilter,
		taskExecutor,
		logger,
		shard.GetMetricsClient(),
	), remoteTimerGate
}
