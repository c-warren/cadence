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
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/types"
)

type OpenCurrentExecutionSuite struct {
	*require.Assertions
	suite.Suite
}

func TestOpenCurrentExecutionSuite(t *testing.T) {
	suite.Run(t, new(OpenCurrentExecutionSuite))
}

func (s *OpenCurrentExecutionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *OpenCurrentExecutionSuite) TestCheck() {
	testCases := []struct {
		execution       *entity.ConcreteExecution
		getCurrentResp  *persistence.GetCurrentExecutionResponse
		getCurrentErr   error
		getConcreteResp *persistence.GetWorkflowExecutionResponse
		getConcreteErr  error
		expectedResult  CheckResult
	}{
		{
			execution: getClosedConcreteExecution(),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
		{
			execution:      getOpenConcreteExecution(),
			getConcreteErr: errors.New("got error checking if concrete is open"),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   OpenCurrentExecution,
				Info:            "failed to check if concrete execution is still open",
				InfoDetails:     "got error checking if concrete is open",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: closedState,
					},
				},
			},
			getConcreteErr: nil,
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  &types.EntityNotExistsError{},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   OpenCurrentExecution,
				Info:            "execution is open without having current execution",
				InfoDetails:     "",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  errors.New("error getting current execution"),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   OpenCurrentExecution,
				Info:            "failed to check if current execution exists",
				InfoDetails:     "error getting current execution",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  nil,
			getCurrentResp: &persistence.GetCurrentExecutionResponse{
				RunID: "not-equal",
			},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   OpenCurrentExecution,
				Info:            "execution is open but current points at a different execution",
				InfoDetails:     "current points at not-equal",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  nil,
			getCurrentResp: &persistence.GetCurrentExecutionResponse{
				RunID: runID,
			},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
	}
	ctrl := gomock.NewController(s.T())
	domainCache := cache.NewMockDomainCache(ctrl)
	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything, mock.Anything).Return(tc.getConcreteResp, tc.getConcreteErr)
		execManager.On("GetCurrentExecution", mock.Anything, mock.Anything).Return(tc.getCurrentResp, tc.getCurrentErr)
		domainCache.EXPECT().GetDomainName(gomock.Any()).Return("test-domain-name", nil).AnyTimes()
		o := NewOpenCurrentExecution(persistence.NewPersistenceRetryer(execManager, nil, c2.CreatePersistenceRetryPolicy()), domainCache)
		s.Equal(tc.expectedResult, o.Check(context.Background(), tc.execution))
	}
}
