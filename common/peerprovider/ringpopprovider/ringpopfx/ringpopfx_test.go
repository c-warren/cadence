package ringpopfx

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/membership"
	ringpopproviderconfig "github.com/uber/cadence/common/peerprovider/ringpopprovider/config"
	"github.com/uber/cadence/common/rpc"
)

func TestFxApp(t *testing.T) {
	app := fxtest.New(t,
		fx.Provide(
			func() testSetupParams {
				ctrl := gomock.NewController(t)
				factory := rpc.NewMockFactory(ctrl)
				tch, err := tchannel.NewChannel("test-ringpop", nil)
				require.NoError(t, err)
				factory.EXPECT().GetTChannel().Return(tch)

				return testSetupParams{
					Service:    "test",
					Logger:     testlogger.New(t),
					RPCFactory: factory,
					Config: config.Config{
						Ringpop: ringpopproviderconfig.Config{
							Name:           "test-ringpop",
							BootstrapMode:  ringpopproviderconfig.BootstrapModeHosts,
							BootstrapHosts: []string{"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"},
						},
					},
				}
			}),
		Module, fx.Invoke(func(provider membership.PeerProvider) {}),
	)
	app.RequireStart().RequireStop()
}

type testSetupParams struct {
	fx.Out

	Service       string `name:"service-full-name"`
	Config        config.Config
	ServiceConfig config.Service
	Logger        log.Logger
	RPCFactory    rpc.Factory
}
