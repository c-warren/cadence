package domaindeprecation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
)

func Test__Start(t *testing.T) {
	domainDeprecationWorkerTest, mockResource := setupTest(t)
	err := domainDeprecationWorkerTest.Start()
	require.NoError(t, err)

	domainDeprecationWorkerTest.Stop()
	mockResource.Finish(t)
}

func setupTest(t *testing.T) (DomainDeprecationWorker, *resource.Test) {
	ctrl := gomock.NewController(t)

	mockResource := resource.NewTest(t, ctrl, metrics.Worker)
	mockResource.SDKClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.DescribeDomainResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForDecisionTaskResponse{}, nil).AnyTimes()
	mockResource.SDKClient.EXPECT().PollForActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&shared.PollForActivityTaskResponse{}, nil).AnyTimes()

	mockClientBean := client.NewMockBean(ctrl)
	mockSvcClient := mockResource.GetSDKClient()

	return New(Params{
		Config: Config{
			AdminOperationToken: dynamicproperties.GetStringPropertyFn(""),
		},
		ServiceClient: mockSvcClient,
		ClientBean:    mockClientBean,
		Tally:         tally.TestScope(nil),
		Logger:        mockResource.GetLogger(),
	}), mockResource
}
