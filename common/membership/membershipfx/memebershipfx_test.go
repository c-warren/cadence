package membershipfx

import (
	"testing"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/rpc"
	"github.com/uber/cadence/common/service"
)

func TestFxStartStop(t *testing.T) {
	app := fxtest.New(t, fx.Provide(func() appParams {
		ctrl := gomock.NewController(t)
		provider := membership.NewMockPeerProvider(ctrl)
		provider.EXPECT().Start(gomock.Any()).Return(nil)
		provider.EXPECT().Stop(gomock.Any()).Return(nil)
		for _, s := range service.ListWithRing {
			provider.EXPECT().Subscribe(s, gomock.Any()).Return(nil)
			provider.EXPECT().GetMembers(s).Return([]membership.HostInfo{}, nil)
			// this is also called by every ring, but could be called multiple times.
			provider.EXPECT().Stop(gomock.Any()).Return(nil)
		}
		factory := rpc.NewMockFactory(ctrl)
		factory.EXPECT().Start(gomock.Any()).Return(nil)
		factory.EXPECT().Stop().Return(nil)
		return appParams{
			Clock:         clock.NewMockedTimeSource(),
			PeerProvider:  provider,
			Logger:        testlogger.New(t),
			MetricsClient: metrics.NewNoopMetricsClient(),
			RPCFactory:    factory,
		}
	}), Module, fx.Invoke(func(resolver membership.Resolver) {}))
	app.RequireStart().RequireStop()
}

type appParams struct {
	fx.Out

	Clock         clock.TimeSource
	PeerProvider  membership.PeerProvider
	Logger        log.Logger
	MetricsClient metrics.Client
	RPCFactory    rpc.Factory
}
