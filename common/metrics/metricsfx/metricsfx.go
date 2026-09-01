package metricsfx

import (
	"github.com/uber-go/tally"
	"go.uber.org/fx"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

// Module provides metrics client for fx application.
var Module = fx.Module("metricsfx",
	fx.Provide(buildClient))

// ModuleForExternalScope provides metrics client for fx application when tally.Scope is created outside.
var ModuleForExternalScope = fx.Module("metricsfx",
	fx.Provide(func(params serviceIdxParams) metrics.ServiceIdx {
		return service.GetMetricsServiceIdx(params.ServiceFullName, params.Logger)
	}),
	fx.Provide(buildClientFromTally))

type clientParams struct {
	fx.In

	Logger          log.Logger
	ServiceFullName string `name:"service-full-name"`
	SvcCfg          config.Service
	MigrationCfg    metrics.MigrationConfig
}

type clientResult struct {
	fx.Out

	Scope  tally.Scope
	Client metrics.Client
}

func buildClient(params clientParams) clientResult {
	scope := params.SvcCfg.Metrics.NewScope(params.Logger, params.ServiceFullName)
	return clientResult{
		Scope:  scope,
		Client: buildClientFromTally(scope, service.GetMetricsServiceIdx(params.ServiceFullName, params.Logger), params.MigrationCfg),
	}
}

type serviceIdxParams struct {
	fx.In

	Logger          log.Logger
	ServiceFullName string `name:"service-full-name"`
}

func buildClientFromTally(scope tally.Scope, serviceID metrics.ServiceIdx, migrationCfg metrics.MigrationConfig) metrics.Client {
	return metrics.NewClient(scope, serviceID, migrationCfg)
}
