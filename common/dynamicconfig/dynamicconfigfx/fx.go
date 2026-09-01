package dynamicconfigfx

import (
	"context"
	"path/filepath"

	"go.uber.org/fx"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/dynamicconfig/configstore"
	csc "github.com/uber/cadence/common/dynamicconfig/configstore/config"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/dynamicconfig/openfeatureclient"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

// Module provides fx options for dynamic config initialization
var Module = fx.Options(fx.Provide(New))

// Params required to build a new dynamic config.
type Params struct {
	fx.In

	Cfg           config.Config
	Logger        log.Logger
	MetricsClient metrics.Client
	RootDir       string `name:"root-dir"`

	Lifecycle fx.Lifecycle
}

type Result struct {
	fx.Out

	Client                   dynamicconfig.Client
	Collection               *dynamicconfig.Collection
	OperationalConfigStore   configstore.Client        `name:"operational-config-store"`
	OperationalDynamicConfig *dynamicconfig.Collection `name:"operational-dynamic-config"`
}

// New creates dynamicconfig.Client from the configuration
func New(p Params) Result {
	stopped := make(chan struct{})

	if p.Cfg.DynamicConfig.Client == "" {
		p.Cfg.DynamicConfigClient.Filepath = constructPathIfNeed(p.RootDir, p.Cfg.DynamicConfigClient.Filepath)
	} else {
		p.Cfg.DynamicConfig.FileBased.Filepath = constructPathIfNeed(p.RootDir, p.Cfg.DynamicConfig.FileBased.Filepath)
	}

	p.Lifecycle.Append(fx.StopHook(func() {
		close(stopped)
	}))

	var res dynamicconfig.Client

	var err error
	if p.Cfg.DynamicConfig.Client == "" {
		p.Logger.Warn("falling back to legacy file based dynamicClientConfig")
		res, err = dynamicconfig.NewFileBasedClient(&p.Cfg.DynamicConfigClient, p.Logger, stopped)
	} else {
		switch p.Cfg.DynamicConfig.Client {
		case dynamicconfig.ConfigStoreClient:
			p.Logger.Info("initialising ConfigStore dynamic config client")
			res, err = configstore.NewConfigStoreClient(
				&p.Cfg.DynamicConfig.ConfigStore,
				&p.Cfg.Persistence,
				p.Logger,
				p.MetricsClient,
				persistence.DynamicConfig,
			)
		case dynamicconfig.FileBasedClient:
			p.Logger.Info("initialising File Based dynamic config client")
			res, err = dynamicconfig.NewFileBasedClient(&p.Cfg.DynamicConfig.FileBased, p.Logger, stopped)
		case dynamicconfig.OpenFeatureClient:
			p.Logger.Info("initialising OpenFeature dynamic config client")
			res = openfeatureclient.NewOpenFeatureClient(p.Logger)

			providerName := p.Cfg.DynamicConfig.OpenFeature.ProviderName
			providerConfig := p.Cfg.DynamicConfig.OpenFeature.Provider
			p.Lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return openfeatureclient.RegisterProvider(ctx, providerName, providerConfig)
				},
				OnStop: func(ctx context.Context) error {
					openfeatureclient.DeregisterProvider()
					return nil
				},
			})
		}
	}

	if res == nil {
		p.Logger.Info("initialising NOP dynamic config client")
		res = dynamicconfig.NewNopClient()
	} else if err != nil {
		p.Logger.Error("creating dynamic config client failed, using no-op config client instead", tag.Error(err))
		res = dynamicconfig.NewNopClient()
	}

	clusterGroupMetadata := p.Cfg.ClusterGroupMetadata
	dc := dynamicconfig.NewCollection(
		res,
		p.Logger,
		dynamicproperties.ClusterNameFilter(clusterGroupMetadata.CurrentClusterName),
	)

	// Create operational config store
	operationalConfigStore := createOperationalConfigStore(&p.Cfg.Persistence, dc, p.Logger, p.MetricsClient)
	operationalDC := dynamicconfig.NewCollection(
		operationalConfigStore,
		p.Logger,
		dynamicproperties.ClusterNameFilter(clusterGroupMetadata.CurrentClusterName),
	)

	return Result{
		Client:                   res,
		Collection:               dc,
		OperationalConfigStore:   operationalConfigStore,
		OperationalDynamicConfig: operationalDC,
	}
}

// constructPathIfNeed would append the dir as the root dir
// when the file wasn't absolute path.
func constructPathIfNeed(dir string, file string) string {
	if !filepath.IsAbs(file) {
		return dir + "/" + file
	}
	return file
}

// createOperationalConfigStore returns the primary persistence-backed configstore.Client, or a no-op when persistence doesn't support one.
func createOperationalConfigStore(
	persistenceConfig *config.Persistence,
	dc *dynamicconfig.Collection,
	logger log.Logger,
	metricsClient metrics.Client,
) configstore.Client {
	cscConfig := &csc.ClientConfig{
		PollInterval:        dc.GetDurationProperty(dynamicproperties.OperationalConfigStorePollInterval)(),
		UpdateRetryAttempts: dc.GetIntProperty(dynamicproperties.OperationalConfigStoreUpdateRetryAttempts)(),
		FetchTimeout:        dc.GetDurationProperty(dynamicproperties.OperationalConfigStoreFetchTimeout)(),
		UpdateTimeout:       dc.GetDurationProperty(dynamicproperties.OperationalConfigStoreUpdateTimeout)(),
	}
	client, err := configstore.NewConfigStoreClient(
		cscConfig,
		persistenceConfig,
		logger,
		metricsClient,
		persistence.OperationalDynamicConfig,
	)
	if err != nil {
		logger.Warn("not instantiating operational dynamic config store, this feature will not be enabled", tag.Error(err))
		return configstore.NewNopClient()
	}
	return client
}
