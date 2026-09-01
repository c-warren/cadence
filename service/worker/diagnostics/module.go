package diagnostics

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/worker/diagnostics/invariant"
)

type DiagnosticsWorkflow interface {
	Start() error
	Stop()
}

type dw struct {
	svcClient       workflowserviceclient.Interface
	clientBean      client.Bean
	metricsClient   metrics.Client
	messagingClient messaging.Client
	logger          log.Logger
	tallyScope      tally.Scope
	worker          worker.Worker
	invariants      []invariant.Invariant
	clusterMetadata cluster.Metadata
}

type Params struct {
	ServiceClient   workflowserviceclient.Interface
	ClientBean      client.Bean
	MetricsClient   metrics.Client
	MessagingClient messaging.Client
	Logger          log.Logger
	TallyScope      tally.Scope
	Invariants      []invariant.Invariant
	ClusterMetadata cluster.Metadata
}

// New creates a new diagnostics workflow.
func New(params Params) DiagnosticsWorkflow {
	return &dw{
		svcClient:       params.ServiceClient,
		metricsClient:   params.MetricsClient,
		messagingClient: params.MessagingClient,
		tallyScope:      params.TallyScope,
		clientBean:      params.ClientBean,
		logger:          params.Logger,
		invariants:      params.Invariants,
		clusterMetadata: params.ClusterMetadata,
	}
}

// Start starts the worker
func (w *dw) Start() error {
	workerOpts := worker.Options{
		MetricsScope:                     w.tallyScope,
		BackgroundActivityContext:        context.Background(),
		Tracer:                           opentracing.GlobalTracer(),
		MaxConcurrentActivityTaskPollers: 10,
		MaxConcurrentDecisionTaskPollers: 10,
	}
	newWorker := worker.New(w.svcClient, constants.SystemLocalDomainName, tasklist, workerOpts)
	newWorker.RegisterWorkflowWithOptions(w.DiagnosticsWorkflow, workflow.RegisterOptions{Name: diagnosticsWorkflow})
	newWorker.RegisterWorkflowWithOptions(w.DiagnosticsStarterWorkflow, workflow.RegisterOptions{Name: diagnosticsStarterWorkflow})
	newWorker.RegisterActivityWithOptions(w.identifyIssues, activity.RegisterOptions{Name: identifyIssuesActivity})
	newWorker.RegisterActivityWithOptions(w.rootCauseIssues, activity.RegisterOptions{Name: rootCauseIssuesActivity})
	newWorker.RegisterActivityWithOptions(w.emitUsageLogs, activity.RegisterOptions{Name: emitUsageLogsActivity})
	w.worker = newWorker
	return newWorker.Start()
}

func (w *dw) Stop() {
	w.worker.Stop()
}
