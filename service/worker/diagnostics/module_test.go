package diagnostics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/service/worker/diagnostics/invariant"
	"github.com/uber/cadence/service/worker/diagnostics/invariant/failure"
)

func Test__Start(t *testing.T) {
	dwTest, mockResource := setuptest(t)
	err := dwTest.Start()
	require.NoError(t, err)
	dwTest.Stop()
	mockResource.Finish(t)
}

func setuptest(t *testing.T) (DiagnosticsWorkflow, *resource.Test) {
	ctrl := gomock.NewController(t)
	mockClientBean := client.NewMockBean(ctrl)
	mockResource := resource.NewTest(t, ctrl, metrics.Worker)
	sdkClient := mockResource.GetSDKClient()
	mockResource.SDKClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.DescribeDomainResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForDecisionTaskResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForActivityTaskResponse{}, nil).AnyTimes()
	return New(Params{
		ServiceClient: sdkClient,
		ClientBean:    mockClientBean,
		MetricsClient: nil,
		TallyScope:    tally.TestScope(nil),
		Invariants:    []invariant.Invariant{failure.NewInvariant()},
	}), mockResource
}
