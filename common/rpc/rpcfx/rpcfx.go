package rpcfx

import (
	"fmt"

	"go.uber.org/fx"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/rpc"
)

// Module provides rpc.Params and rpc.Factory for fx application.
var Module = fx.Module("rpcfx",
	fx.Provide(paramsBuilder),
	fx.Provide(buildFactory),
)

type paramsBuilderParams struct {
	fx.In

	ServiceFullName   string `name:"service-full-name"`
	Cfg               config.Config
	Logger            log.Logger
	DynamicCollection *dynamicconfig.Collection
	MetricsClient     metrics.Client
}

func paramsBuilder(p paramsBuilderParams) (rpc.Params, error) {
	res, err := rpc.NewParams(p.ServiceFullName, &p.Cfg, p.DynamicCollection, p.Logger, p.MetricsClient)
	if err != nil {
		return rpc.Params{}, fmt.Errorf("create rpc params: %w", err)
	}
	return res, nil
}

type factoryParams struct {
	fx.In

	Logger    log.Logger
	RPCParams rpc.Params
}

func buildFactory(p factoryParams) rpc.Factory {
	return rpc.NewFactory(p.Logger, p.RPCParams)
}
