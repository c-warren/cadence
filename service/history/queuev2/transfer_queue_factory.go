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
	"github.com/uber/cadence/service/history/reset"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/history/workflowcache"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	transferQueueFactory struct {
		taskProcessor  task.Processor
		archivalClient archiver.Client
		wfIDCache      workflowcache.WFCache
	}
)

func NewTransferQueueFactory(
	taskProcessor task.Processor,
	archivalClient archiver.Client,
	wfIDCache workflowcache.WFCache,
) queue.Factory {
	return &transferQueueFactory{taskProcessor, archivalClient, wfIDCache}
}

func (f *transferQueueFactory) Category() persistence.HistoryTaskCategory {
	return persistence.HistoryTaskCategoryTransfer
}

func (f *transferQueueFactory) isQueueV2Enabled(shard shard.Context) bool {
	return shard.GetConfig().EnableTransferQueueV2(shard.GetShardID())
}

func (f *transferQueueFactory) CreateQueue(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	if f.isQueueV2Enabled(shard) {
		return f.createQueuev2(shard, executionCache, openExecutionCheck)
	}
	return f.createQueuev1(shard, executionCache, openExecutionCheck)
}

func (f *transferQueueFactory) createQueuev1(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	workflowResetter := reset.NewWorkflowResetter(shard, executionCache, shard.GetLogger())
	return queue.NewTransferQueueProcessor(
		shard,
		f.taskProcessor,
		executionCache,
		workflowResetter,
		f.archivalClient,
		openExecutionCheck,
		f.wfIDCache,
	)
}

func (f *transferQueueFactory) createQueuev2(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) queue.Processor {
	logger := shard.GetLogger().WithTags(tag.ComponentTransferQueueV2)
	workflowResetter := reset.NewWorkflowResetter(shard, executionCache, logger)
	activeTaskExecutor := task.NewTransferActiveTaskExecutor(
		shard,
		f.archivalClient,
		executionCache,
		workflowResetter,
		logger,
		shard.GetConfig(),
		f.wfIDCache,
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
	standbyTaskExecutor := task.NewTransferStandbyTaskExecutor(
		shard,
		f.archivalClient,
		executionCache,
		historyResender,
		logger,
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
	queueReader := NewQueueReader(
		shard,
		persistence.HistoryTaskCategoryTransfer,
		config.TransferProcessorMaxPollInterval,
		config.TransferProcessorMaxPollIntervalJitterCoefficient,
	)
	return NewImmediateQueue(
		shard,
		persistence.HistoryTaskCategoryTransfer,
		f.taskProcessor,
		executorWrapper,
		logger,
		shard.GetMetricsClient(),
		shard.GetMetricsClient().Scope(metrics.TransferQueueProcessorV2Scope).Tagged(metrics.ShardIDTag(shard.GetShardID())),
		queueReader,
		&Options{
			PageSize:                             config.TransferTaskBatchSize,
			DeleteBatchSize:                      config.TransferTaskDeleteBatchSize,
			MaxPollRPS:                           config.TransferProcessorMaxPollRPS,
			MaxPollInterval:                      config.TransferProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:     config.TransferProcessorMaxPollIntervalJitterCoefficient,
			UpdateAckInterval:                    config.TransferProcessorUpdateAckInterval,
			UpdateAckIntervalJitterCoefficient:   config.TransferProcessorUpdateAckIntervalJitterCoefficient,
			MaxPendingTasksCount:                 config.QueueMaxPendingTaskCount,
			PollBackoffInterval:                  config.QueueProcessorPollBackoffInterval,
			PollBackoffIntervalJitterCoefficient: config.QueueProcessorPollBackoffIntervalJitterCoefficient,
			VirtualSliceForceAppendInterval:      config.VirtualSliceForceAppendInterval,
			EnableValidator:                      config.TransferProcessorEnableValidator,
			ValidationInterval:                   config.TransferProcessorValidationInterval,
			MaxStartJitterInterval:               dynamicproperties.GetDurationPropertyFn(0),
			RedispatchInterval:                   config.ActiveTaskRedispatchInterval,
			CriticalPendingTaskCount:             config.QueueCriticalPendingTaskCount,
			EnablePendingTaskCountAlert:          func() bool { return config.EnableTransferQueueV2PendingTaskCountAlert(shard.GetShardID()) },
			MaxVirtualQueueCount:                 config.QueueMaxVirtualQueueCount,
		},
	)
}
