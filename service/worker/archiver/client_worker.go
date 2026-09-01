package archiver

import (
	"context"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	// ClientWorker is a cadence client worker
	ClientWorker interface {
		Start() error
		Stop()
	}

	clientWorker struct {
		worker      worker.Worker
		domainCache cache.DomainCache
	}

	// BootstrapContainer contains everything need for bootstrapping
	BootstrapContainer struct {
		PublicClient     workflowserviceclient.Interface
		MetricsClient    metrics.Client
		Logger           log.Logger
		HistoryV2Manager persistence.HistoryManager
		DomainCache      cache.DomainCache
		Config           *Config
		ArchiverProvider provider.ArchiverProvider
	}

	// Config for ClientWorker
	Config struct {
		ArchiverConcurrency             dynamicproperties.IntPropertyFn
		ArchivalsPerIteration           dynamicproperties.IntPropertyFn
		TimeLimitPerArchivalIteration   dynamicproperties.DurationPropertyFn
		AllowArchivingIncompleteHistory dynamicproperties.BoolPropertyFn
	}

	contextKey int
)

const (
	workflowIDPrefix                = "cadence-archival"
	decisionTaskList                = "cadence-archival-tl"
	signalName                      = "cadence-archival-signal"
	archivalWorkflowFnName          = "archivalWorkflow"
	workflowStartToCloseTimeout     = time.Hour * 24 * 30
	workflowTaskStartToCloseTimeout = time.Minute

	bootstrapContainerKey contextKey = iota
)

// these globals exist as a work around because no primitive exists to pass such objects to workflow code
var (
	globalLogger        log.Logger
	globalMetricsClient metrics.Client
	globalConfig        *Config
)

func init() {
	workflow.RegisterWithOptions(archivalWorkflow, workflow.RegisterOptions{Name: archivalWorkflowFnName})
	activity.RegisterWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	activity.RegisterWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	activity.RegisterWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})
}

// NewClientWorker returns a new ClientWorker
func NewClientWorker(container *BootstrapContainer) ClientWorker {
	globalLogger = container.Logger.WithTags(tag.ComponentArchiver, tag.WorkflowDomainName(constants.SystemLocalDomainName))
	globalMetricsClient = container.MetricsClient
	globalConfig = container.Config
	actCtx := context.WithValue(context.Background(), bootstrapContainerKey, container)
	wo := worker.Options{
		BackgroundActivityContext: actCtx,
	}
	return &clientWorker{
		worker:      worker.New(container.PublicClient, constants.SystemLocalDomainName, decisionTaskList, wo),
		domainCache: container.DomainCache,
	}
}

// Start the ClientWorker
func (w *clientWorker) Start() error {
	if err := w.worker.Start(); err != nil {
		w.worker.Stop()
		return err
	}
	return nil
}

// Stop the ClientWorker
func (w *clientWorker) Stop() {
	w.worker.Stop()
	w.domainCache.Stop()
}
