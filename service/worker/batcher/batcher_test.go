package batcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
)

func Test__Start(t *testing.T) {
	batcher, mockResource := setuptest(t)
	err := batcher.Start()
	require.NoError(t, err)
	mockResource.Finish(t)
}

func setuptest(t *testing.T) (*Batcher, *resource.Test) {
	ctrl := gomock.NewController(t)
	mockResource := resource.NewTest(t, ctrl, metrics.Worker)

	mockClientBean := client.NewMockBean(ctrl)
	mockResource.SDKClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.DescribeDomainResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForDecisionTaskResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForActivityTaskResponse{}, nil).AnyTimes()
	sdkClient := mockResource.GetSDKClient()
	mockClientBean.EXPECT().GetFrontendClient().Return(mockResource.FrontendClient).AnyTimes()
	mockClientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(mockResource.RemoteAdminClient, nil).AnyTimes()

	return New(&BootstrapParams{
		Logger:        testlogger.New(t),
		ServiceClient: sdkClient,
		ClientBean:    mockClientBean,
		TallyScope:    tally.TestScope(nil),
		Config: Config{
			ClusterMetadata: cluster.NewMetadata(
				config.ClusterGroupMetadata{
					FailoverVersionIncrement: 12,
					PrimaryClusterName:       "test-primary-cluster",
					CurrentClusterName:       "test-primary-cluster",
					ClusterGroup: map[string]config.ClusterInformation{
						"test-primary-cluster":   {},
						"test-secondary-cluster": {},
					},
				},
				nil,
				metrics.NewClient(tally.NoopScope, metrics.Worker, metrics.MigrationConfig{}),
				testlogger.New(t),
			),
		},
	}), mockResource
}
