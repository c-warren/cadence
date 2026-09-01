package archiver

import (
	"time"

	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

type dynamicConfigResult struct {
	ArchiverConcurrency   int
	ArchivalsPerIteration int
	TimelimitPerIteration time.Duration
}

func archivalWorkflow(ctx workflow.Context, carryover []ArchiveRequest) error {
	return archivalWorkflowHelper(ctx, globalLogger, globalMetricsClient, globalConfig, nil, nil, carryover)
}

func archivalWorkflowHelper(
	ctx workflow.Context,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
	handler Handler, // enables tests to inject mocks
	pump Pump, // enables tests to inject mocks
	carryover []ArchiveRequest,
) error {
	metricsClient = NewReplayMetricsClient(metricsClient, ctx)
	metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount)
	archivalLatencyStart := workflow.Now(ctx)
	sw := metricsClient.StartTimer(metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency)
	stopTimers := func() {
		sw.Stop()
		metricsClient.Scope(metrics.ArchiverArchivalWorkflowScope).ExponentialHistogram(metrics.CadenceLatencyHistogram, workflow.Now(ctx).Sub(archivalLatencyStart))
	}
	workflowInfo := workflow.GetInfo(ctx)
	logger = logger.WithTags(
		tag.WorkflowID(workflowInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(workflowInfo.WorkflowExecution.RunID),
		tag.WorkflowTaskListName(workflowInfo.TaskListName),
		tag.WorkflowType(workflowInfo.WorkflowType.Name))
	logger = log.NewReplayLogger(logger, ctx, false)

	logger.Info("archival system workflow started")
	var dcResult dynamicConfigResult
	_ = workflow.SideEffect(
		ctx,
		func(ctx workflow.Context) interface{} {
			timeLimit := config.TimeLimitPerArchivalIteration()
			maxTimeLimit := MaxArchivalIterationTimeout()
			if timeLimit > maxTimeLimit {
				timeLimit = maxTimeLimit
			}
			return dynamicConfigResult{
				ArchiverConcurrency:   config.ArchiverConcurrency(),
				ArchivalsPerIteration: config.ArchivalsPerIteration(),
				TimelimitPerIteration: timeLimit,
			}
		}).Get(&dcResult)
	requestCh := workflow.NewBufferedChannel(ctx, dcResult.ArchivalsPerIteration)
	if handler == nil {
		handler = NewHandler(ctx, logger, metricsClient, dcResult.ArchiverConcurrency, requestCh)
	}
	handlerSW := metricsClient.StartTimer(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency)
	handler.Start()
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	if pump == nil {
		pump = NewPump(ctx, logger, metricsClient, carryover, dcResult.TimelimitPerIteration, dcResult.ArchivalsPerIteration, requestCh, signalCh)
	}
	pumpResult := pump.Run()
	metricsClient.AddCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(len(pumpResult.PumpedHashes)))
	handledHashes := handler.Finished()
	handlerSW.Stop()
	metricsClient.AddCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(len(handledHashes)))
	if !hashesEqual(pumpResult.PumpedHashes, handledHashes) {
		logger.Error("handled archival requests do not match pumped archival requests")
		metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverPumpedNotEqualHandledCount)
	}
	if pumpResult.TimeoutWithoutSignals {
		logger.Info("workflow stopping because pump did not get any signals within timeout threshold")
		metricsClient.IncCounter(metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStoppingCount)
		stopTimers()
		return nil
	}
	for {
		var request ArchiveRequest
		if ok := signalCh.ReceiveAsync(&request); !ok {
			break
		}
		pumpResult.UnhandledCarryover = append(pumpResult.UnhandledCarryover, request)
	}
	logger.Info("archival system workflow continue as new")
	ctx = workflow.WithExecutionStartToCloseTimeout(ctx, workflowStartToCloseTimeout)
	ctx = workflow.WithWorkflowTaskStartToCloseTimeout(ctx, workflowTaskStartToCloseTimeout)
	stopTimers()
	return workflow.NewContinueAsNewError(ctx, archivalWorkflowFnName, pumpResult.UnhandledCarryover)
}
