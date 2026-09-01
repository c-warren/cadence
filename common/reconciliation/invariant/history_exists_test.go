package invariant

import (
	"context"
	"errors"
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
)

type HistoryExistsSuite struct {
	*require.Assertions
	suite.Suite
}

func TestHistoryExistsSuite(t *testing.T) {
	suite.Run(t, new(HistoryExistsSuite))
}

func (s *HistoryExistsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryExistsSuite) TestCheck() {
	testCases := []struct {
		getExecErr                error
		getExecResp               *persistence.GetWorkflowExecutionResponse
		getHistoryErr             error
		getHistoryResp            *persistence.ReadHistoryBranchResponse
		expectedResult            CheckResult
		expectedResourcePopulated bool
	}{
		{
			getExecErr:     errors.New("got error checking workflow exists"),
			getHistoryResp: &persistence.ReadHistoryBranchResponse{},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   HistoryExists,
				Info:            "failed to check if concrete execution still exists",
				InfoDetails:     "got error checking workflow exists",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecErr:     &types.EntityNotExistsError{},
			getHistoryResp: &persistence.ReadHistoryBranchResponse{},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   HistoryExists,
				Info:            "determined execution was healthy because concrete execution no longer exists",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			getHistoryErr:  &types.EntityNotExistsError{Message: "got entity not exists error"},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   HistoryExists,
				Info:            "concrete execution exists but history does not exist",
				InfoDetails:     "got entity not exists error",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			getHistoryErr:  errors.New("error fetching history"),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   HistoryExists,
				Info:            "failed to verify if history exists",
				InfoDetails:     "error fetching history",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   HistoryExists,
				Info:            "concrete execution exists but got empty history",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp: &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: &persistence.ReadHistoryBranchResponse{
				HistoryEvents: []*types.HistoryEvent{
					{},
				},
			},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   HistoryExists,
			},
			expectedResourcePopulated: true,
		},
	}

	ctrl := gomock.NewController(s.T())
	domainCache := cache.NewMockDomainCache(ctrl)
	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		historyManager := &mocks.HistoryV2Manager{}
		execManager.On("GetWorkflowExecution", mock.Anything, mock.Anything).Return(tc.getExecResp, tc.getExecErr)
		historyManager.On("ReadHistoryBranch", mock.Anything, mock.Anything).Return(tc.getHistoryResp, tc.getHistoryErr)
		domainCache.EXPECT().GetDomainName(gomock.Any()).Return("test-domain-name", nil).AnyTimes()
		i := NewHistoryExists(persistence.NewPersistenceRetryer(execManager, historyManager, c2.CreatePersistenceRetryPolicy()), domainCache)
		result := i.Check(context.Background(), getOpenConcreteExecution())
		s.Equal(tc.expectedResult, result)

	}
}
