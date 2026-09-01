package membershipfx

import (
	"fmt"

	"go.uber.org/fx"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/rpc"
	"github.com/uber/cadence/common/service"
)

// Module provides membership components for fx app.
var Module = fx.Module("membership", fx.Provide(buildMembership))

type buildMembershipParams struct {
	fx.In

	Clock         clock.TimeSource
	RPCFactory    rpc.Factory
	PeerProvider  membership.PeerProvider
	Logger        log.Logger
	MetricsClient metrics.Client
	Lifecycle     fx.Lifecycle
}

type buildMembershipResult struct {
	fx.Out

	Rings    map[string]membership.SingleProvider
	Resolver membership.Resolver
}

func buildMembership(params buildMembershipParams) (buildMembershipResult, error) {
	rings := make(map[string]membership.SingleProvider)
	for _, s := range service.ListWithRing {
		rings[s] = membership.NewHashring(s, params.PeerProvider, params.Clock, params.Logger, params.MetricsClient.Scope(metrics.HashringScope))
	}

	resolver, err := membership.NewResolver(
		params.PeerProvider,
		params.MetricsClient,
		params.Logger,
		rings,
	)
	if err != nil {
		return buildMembershipResult{}, fmt.Errorf("create resolver: %w", err)
	}

	params.Lifecycle.Append(fx.StartStopHook(startResolver(resolver, params.RPCFactory), resolver.Stop))
	params.Lifecycle.Append(fx.StopHook(params.RPCFactory.Stop))

	return buildMembershipResult{
		Rings:    rings,
		Resolver: resolver,
	}, nil
}

func startResolver(resolver membership.Resolver, rpcFactory rpc.Factory) func() error {
	return func() error {
		err := rpcFactory.Start(resolver)
		if err != nil {
			return fmt.Errorf("start rpc factory: %w", err)
		}
		resolver.Start()
		return nil
	}
}
