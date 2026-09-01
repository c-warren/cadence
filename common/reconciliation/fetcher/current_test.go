package fetcher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

func TestCurrentExecutionIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	retryer := persistence.NewMockRetryer(ctrl)
	retryer.EXPECT().ListCurrentExecutions(gomock.Any(), gomock.Any()).
		Return(&persistence.ListCurrentExecutionsResponse{}, nil).
		Times(1)

	iterator := CurrentExecutionIterator(
		context.Background(),
		retryer,
		10,
	)
	require.NotNil(t, iterator)
}

func TestCurrentExecution(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name       string
		setupMock  func(*persistence.MockRetryer)
		request    ExecutionRequest
		wantEntity entity.Entity
		wantErr    bool
	}{
		{
			name: "success",
			request: ExecutionRequest{
				DomainID:   "testDomainID",
				WorkflowID: "testWorkflowID",
				DomainName: "testDomainName",
			},
			setupMock: func(mockRetryer *persistence.MockRetryer) {
				mockRetryer.EXPECT().GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
					DomainID:   "testDomainID",
					WorkflowID: "testWorkflowID",
					DomainName: "testDomainName",
				}).Return(&persistence.GetCurrentExecutionResponse{
					RunID: "testRunID",
					State: persistence.WorkflowStateRunning,
				}, nil).Times(1)

				mockRetryer.EXPECT().GetShardID().Return(123).Times(1)
			},
			wantEntity: &entity.CurrentExecution{
				CurrentRunID: "testRunID",
				Execution: entity.Execution{
					ShardID:    123,
					DomainID:   "testDomainID",
					WorkflowID: "testWorkflowID",
					RunID:      "testRunID",
					State:      persistence.WorkflowStateRunning,
				},
			},
		},
		{
			name: "GetCurrentExecution failed",
			request: ExecutionRequest{
				DomainID:   "testDomainID",
				WorkflowID: "testWorkflowID",
				DomainName: "testDomainName",
			},
			setupMock: func(mockRetryer *persistence.MockRetryer) {
				mockRetryer.EXPECT().GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
					DomainID:   "testDomainID",
					WorkflowID: "testWorkflowID",
					DomainName: "testDomainName",
				}).Return(nil, fmt.Errorf("failed")).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockRetryer := persistence.NewMockRetryer(ctrl)

			tc.setupMock(mockRetryer)
			gotEntity, err := CurrentExecution(ctx, mockRetryer, tc.request)
			if (err != nil) != tc.wantErr {
				t.Errorf("CurrentExecution() error = %v, wantErr %v", err, tc.wantErr)
			}

			require.Equal(t, tc.wantEntity, gotEntity)
		})
	}
}

func TestGetCurrentExecution(t *testing.T) {
	ctx := context.Background()
	pageSize := 10

	testCases := []struct {
		name      string
		setupMock func(*persistence.MockRetryer)
		wantPage  pagination.Page
		wantErr   bool
	}{
		{
			name: "success",
			setupMock: func(mockRetryer *persistence.MockRetryer) {
				executions := []*persistence.CurrentWorkflowExecution{
					{
						DomainID:     "testDomainID",
						WorkflowID:   "testWorkflowID",
						RunID:        "testRunID",
						State:        persistence.WorkflowStateRunning,
						CurrentRunID: "testCurrentRunID", // Setting CurrentRunID
					},
				}

				mockRetryer.EXPECT().
					ListCurrentExecutions(ctx, &persistence.ListCurrentExecutionsRequest{
						PageSize: pageSize,
					}).
					Return(&persistence.ListCurrentExecutionsResponse{
						Executions: executions,
						PageToken:  nil,
					}, nil).Times(1)

				mockRetryer.EXPECT().GetShardID().Return(123).Times(1)
			},
			wantPage: pagination.Page{
				Entities: []pagination.Entity{
					&entity.CurrentExecution{
						CurrentRunID: "testCurrentRunID", // This should match with the mocked data
						Execution: entity.Execution{
							ShardID:    123,
							DomainID:   "testDomainID",
							WorkflowID: "testWorkflowID",
							RunID:      "testRunID",
							State:      persistence.WorkflowStateRunning,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "ListCurrentExecutions failed",
			setupMock: func(mockRetryer *persistence.MockRetryer) {
				mockRetryer.EXPECT().
					ListCurrentExecutions(ctx, &persistence.ListCurrentExecutionsRequest{
						PageSize: pageSize,
					}).
					Return(nil, fmt.Errorf("failed")).
					Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockRetryer := persistence.NewMockRetryer(ctrl)

			tc.setupMock(mockRetryer)
			fetchFn := getCurrentExecution(mockRetryer, pageSize)
			page, err := fetchFn(ctx, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("getCurrentExecution() error = %v, wantErr %v", err, tc.wantErr)
			}

			require.Equal(t, tc.wantPage, page)
		})
	}
}
