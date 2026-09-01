package invariant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	c2 "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/constants"
)

type InactiveInactiveDomainExistsSuite struct {
	*require.Assertions
	suite.Suite
}

func TestInactiveInactiveDomainExistsSuite(t *testing.T) {
	suite.Run(t, new(InactiveInactiveDomainExistsSuite))
}

func (s *InactiveInactiveDomainExistsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *InactiveInactiveDomainExistsSuite) TestCheck() {
	testCases := []struct {
		getExecErr     error
		getExecResp    *persistence.GetWorkflowExecutionResponse
		getDomainErr   error
		expectedResult CheckResult
	}{
		{
			getExecErr: &types.EntityNotExistsError{},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   InactiveDomainExists,
				Info:            "workflow's domain is active",
				InfoDetails:     "workflow's domain is active",
			},
		},
	}

	ctrl := gomock.NewController(s.T())
	domainCache := cache.NewMockDomainCache(ctrl)
	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything, mock.Anything).Return(tc.getExecResp, tc.getExecErr)
		domainCache.EXPECT().GetDomainName(gomock.Any()).Return("test-domain-name", nil).AnyTimes()
		domainCache.EXPECT().GetDomainByID(gomock.Any()).Return(constants.TestGlobalDomainEntry, nil).AnyTimes()
		i := NewInactiveDomainExists(persistence.NewPersistenceRetryer(execManager, nil, c2.CreatePersistenceRetryPolicy()), domainCache)
		result := i.Check(context.Background(), getOpenConcreteExecution())
		s.Equal(tc.expectedResult, result)

	}
}
