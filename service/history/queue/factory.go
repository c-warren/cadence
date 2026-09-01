package queue

import (
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/reset"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/history/workflowcache"
	"github.com/uber/cadence/service/worker/archiver"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination factory_mock.go -self_package github.com/uber/cadence/service/history/queue

type (
	Factory interface {
		Category() persistence.HistoryTaskCategory
		CreateQueue(shard.Context, execution.Cache, invariant.Invariant) Processor
	}

	transferQueueFactory struct {
		taskProcessor  task.Processor
		archivalClient archiver.Client
		wfIDCache      workflowcache.WFCache
	}

	timerQueueFactory struct {
		taskProcessor  task.Processor
		archivalClient archiver.Client
	}
)

func NewTransferQueueFactory(
	taskProcessor task.Processor,
	archivalClient archiver.Client,
	wfIDCache workflowcache.WFCache,
) Factory {
	return &transferQueueFactory{
		taskProcessor:  taskProcessor,
		archivalClient: archivalClient,
		wfIDCache:      wfIDCache,
	}
}

func (f *transferQueueFactory) Category() persistence.HistoryTaskCategory {
	return persistence.HistoryTaskCategoryTransfer
}

func (f *transferQueueFactory) CreateQueue(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) Processor {
	workflowResetter := reset.NewWorkflowResetter(shard, executionCache, shard.GetLogger())
	return NewTransferQueueProcessor(
		shard,
		f.taskProcessor,
		executionCache,
		workflowResetter,
		f.archivalClient,
		openExecutionCheck,
		f.wfIDCache,
	)
}

func (f *timerQueueFactory) Category() persistence.HistoryTaskCategory {
	return persistence.HistoryTaskCategoryTimer
}

func NewTimerQueueFactory(
	taskProcessor task.Processor,
	archivalClient archiver.Client,
) Factory {
	return &timerQueueFactory{
		taskProcessor:  taskProcessor,
		archivalClient: archivalClient,
	}
}

func (f *timerQueueFactory) CreateQueue(
	shard shard.Context,
	executionCache execution.Cache,
	openExecutionCheck invariant.Invariant,
) Processor {
	return NewTimerQueueProcessor(
		shard,
		f.taskProcessor,
		executionCache,
		f.archivalClient,
		openExecutionCheck,
	)
}
