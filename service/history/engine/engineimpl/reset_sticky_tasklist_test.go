package engineimpl

import (
	ctx "context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/engine/testdata"
	"github.com/uber/cadence/service/history/workflow"
)

func TestResetStickyTaskList(t *testing.T) {
	execution := &types.WorkflowExecution{
		WorkflowID: constants.TestWorkflowID,
		RunID:      constants.TestRunID,
	}
	cases := []struct {
		name        string
		request     *types.HistoryResetStickyTaskListRequest
		init        func(engine *testdata.EngineForTest)
		assertions  func(engine *testdata.EngineForTest)
		expectedErr error
	}{
		{
			name: "Invalid Domain",
			request: &types.HistoryResetStickyTaskListRequest{
				DomainUUID: "",
				Execution:  execution,
			},
			expectedErr: &types.BadRequestError{Message: "Missing domain UUID."},
		},
		{
			name: "Completed Workflow",
			request: &types.HistoryResetStickyTaskListRequest{
				DomainUUID: constants.TestDomainID,
				Execution:  execution,
			},
			init: func(engine *testdata.EngineForTest) {
				engine.ShardCtx.Resource.ExecutionMgr.On("GetWorkflowExecution", mock.Anything, mock.MatchedBy(func(req *persistence.GetWorkflowExecutionRequest) bool {
					return req.Execution == *execution
				})).Return(&persistence.GetWorkflowExecutionResponse{
					State: &persistence.WorkflowMutableState{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:   constants.TestDomainID,
							WorkflowID: execution.WorkflowID,
							RunID:      execution.RunID,
							State:      persistence.WorkflowStateCompleted,
						},
						ExecutionStats: &persistence.ExecutionStats{},
						Checksum:       checksum.Checksum{},
					},
				}, nil)
				engine.ShardCtx.Resource.ActiveClusterMgr.EXPECT().GetActiveClusterInfoByWorkflow(gomock.Any(), constants.TestDomainID, execution.WorkflowID, execution.RunID).
					Return(&types.ActiveClusterInfo{ActiveClusterName: "test-active-cluster"}, nil).Times(1)
			},
			expectedErr: workflow.ErrAlreadyCompleted,
		},
		{
			name: "Success",
			request: &types.HistoryResetStickyTaskListRequest{
				DomainUUID: constants.TestDomainID,
				Execution:  execution,
			},
			init: func(engine *testdata.EngineForTest) {
				engine.ShardCtx.Resource.ExecutionMgr.On("GetWorkflowExecution", mock.Anything, mock.MatchedBy(func(req *persistence.GetWorkflowExecutionRequest) bool {
					return req.Execution == *execution
				})).Return(&persistence.GetWorkflowExecutionResponse{
					State: &persistence.WorkflowMutableState{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:       constants.TestDomainID,
							WorkflowID:     execution.WorkflowID,
							RunID:          execution.RunID,
							StickyTaskList: "CLEAR ME PLEASE",
						},
						ExecutionStats: &persistence.ExecutionStats{},
						Checksum:       checksum.Checksum{},
					},
				}, nil)
				engine.ShardCtx.Resource.ActiveClusterMgr.EXPECT().GetActiveClusterInfoByWorkflow(gomock.Any(), constants.TestDomainID, execution.WorkflowID, execution.RunID).
					Return(&types.ActiveClusterInfo{ActiveClusterName: "test-active-cluster"}, nil).Times(1)
				engine.ShardCtx.Resource.ExecutionMgr.On("UpdateWorkflowExecution", mock.Anything, mock.MatchedBy(func(req *persistence.UpdateWorkflowExecutionRequest) bool {
					return req.UpdateWorkflowMutation.ExecutionInfo.WorkflowID == execution.WorkflowID &&
						req.UpdateWorkflowMutation.ExecutionInfo.RunID == execution.RunID &&
						req.UpdateWorkflowMutation.ExecutionInfo.StickyTaskList == ""
				})).Return(&persistence.UpdateWorkflowExecutionResponse{}, nil)
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			eft := testdata.NewEngineForTest(t, NewEngineWithShardContext)

			if testCase.init != nil {
				testCase.init(eft)
			}
			eft.Engine.Start()
			result, err := eft.Engine.ResetStickyTaskList(ctx.Background(), testCase.request)

			if testCase.assertions != nil {
				testCase.assertions(eft)
			}
			eft.Engine.Stop()

			if testCase.expectedErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, &types.HistoryResetStickyTaskListResponse{}, result)
			} else {
				assert.Equal(t, testCase.expectedErr, err)
				assert.Nil(t, result)
			}
		})
	}
}
