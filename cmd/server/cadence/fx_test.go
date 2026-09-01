package cadence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/uber/cadence/common/config"
)

func TestFxDependencies(t *testing.T) {
	err := fx.ValidateApp(_commonModule,
		fx.Supply(appContext{
			CfgContext: config.Context{
				Environment: "",
				Zone:        "",
			},
			ConfigDir: "",
			RootDir:   "",
		}),
		Module(""))
	require.NoError(t, err)
}
