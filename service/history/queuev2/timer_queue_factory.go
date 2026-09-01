package queuev2

import (
	"context"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/ndc"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	timerQueueFactory struct {
		taskProcessor  task.Processor
		archivalClient archiver.Client
	}
)

func NewTimerQueueFactory(
	taskProcessor task.Processor,
	archivalClient archiver.Client,
) queue.Factory {
	return &timerQueueFactory{
		taskProcessor:  taskProcessor,
		archivalClient: archivalClient,
	}
}

func (f *timerQueueFactory) Category() persistence.HistoryTaskCategory {
	return persistence.HistoryTaskCategoryTimer
}

func (f *timerQueueFactory) isQueueV2Enabled(shard shard.Context) bool {
	return shard.GetConfig().EnableTimerQueueV2(shard.GetShardID())
}

func (f *timerQueueFactory) CreateQueue(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	if f.isQueueV2Enabled(shard) {
		return f.createQueuev2(shard, executionCache, openExecutionCheck)
	}
	return f.createQueuev1(shard, executionCache, openExecutionCheck)
}

func (f *timerQueueFactory) createQueuev1(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	return queue.NewTimerQueueProcessor(
		shard,
		f.taskProcessor,
		executionCache,
		f.archivalClient,
		openExecutionCheck,
	)
}

func (f *timerQueueFactory) createQueuev2(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	logger := shard.GetLogger().WithTags(tag.ComponentTimerQueueV2)
	activeTaskExecutor := task.NewTimerActiveTaskExecutor(
		shard,
		f.archivalClient,
		executionCache,
		logger,
		shard.GetMetricsClient(),
		shard.GetConfig(),
	)
	historyResender := ndc.NewHistoryResender(
		shard.GetDomainCache(),
		shard.GetService().GetClientBean(),
		func(ctx context.Context, request *types.ReplicateEventsV2Request) error {
			return shard.GetEngine().ReplicateEventsV2(ctx, request)
		},
		shard.GetConfig().StandbyTaskReReplicationContextTimeout,
		openExecutionCheck,
		logger,
	)
	standbyTaskExecutor := task.NewTimerStandbyTaskExecutor(
		shard,
		f.archivalClient,
		executionCache,
		historyResender,
		logger,
		shard.GetMetricsClient(),
		shard.GetClusterMetadata().GetCurrentClusterName(),
		shard.GetConfig(),
		shard.GetHistoryTaskDLQWriter(),
	)
	executorWrapper := task.NewExecutorWrapper(
		shard.GetClusterMetadata().GetCurrentClusterName(),
		shard.GetActiveClusterManager(),
		activeTaskExecutor,
		standbyTaskExecutor,
		logger,
	)
	config := shard.GetConfig()
	metricsScope := shard.GetMetricsClient().Scope(metrics.TimerQueueProcessorV2Scope).Tagged(metrics.ShardIDTag(shard.GetShardID()))
	options := &Options{
		PageSize:                             config.TimerTaskBatchSize,
		DeleteBatchSize:                      config.TimerTaskDeleteBatchSize,
		MaxPollRPS:                           config.TimerProcessorMaxPollRPS,
		MaxPollInterval:                      config.TimerProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:     config.TimerProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                    config.TimerProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:   config.TimerProcessorUpdateAckIntervalJitterCoefficient,
		MaxPendingTasksCount:                 config.QueueMaxPendingTaskCount,
		PollBackoffInterval:                  config.QueueProcessorPollBackoffInterval,
		PollBackoffIntervalJitterCoefficient: config.QueueProcessorPollBackoffIntervalJitterCoefficient,
		VirtualSliceForceAppendInterval:      config.VirtualSliceForceAppendInterval,
		MaxStartJitterInterval:               dynamicproperties.GetDurationPropertyFn(0),
		RedispatchInterval:                   config.ActiveTaskRedispatchInterval,
		CriticalPendingTaskCount:             config.QueueCriticalPendingTaskCount,
		EnablePendingTaskCountAlert:          func() bool { return config.EnableTimerQueueV2PendingTaskCountAlert(shard.GetShardID()) },
		MaxVirtualQueueCount:                 config.QueueMaxVirtualQueueCount,
	}

	var cachedReader CachedQueueReader
	reader := NewQueueReader(
		shard,
		persistence.HistoryTaskCategoryTimer,
		options.MaxPollInterval,
		options.MaxPollIntervalJitterCoefficient,
	)
	if !isCachedQueueReaderDisabled(config.TimerProcessorCachedQueueReaderMode(shard.GetShardID())) {
		cachedReader = newCachedQueueReader(reader, newInMemQueue(), shard, metricsScope)
		reader = cachedReader
	}

	base := newScheduledQueue(
		shard,
		persistence.HistoryTaskCategoryTimer,
		f.taskProcessor,
		executorWrapper,
		logger,
		shard.GetMetricsClient(),
		metricsScope,
		reader,
		options,
	)

	if cachedReader != nil {
		return newCachedScheduledQueue(base, cachedReader)
	}

	return base
}
