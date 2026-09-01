package fetcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

func TestTimerIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	retryer := persistence.NewMockRetryer(ctrl)
	retryer.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).
		Return(&persistence.GetHistoryTasksResponse{}, nil).
		Times(1)

	iterator := TimerIterator(
		context.Background(),
		retryer,
		time.Now(),
		time.Now(),
		10,
	)
	require.NotNil(t, iterator)
}

func TestGetUserTimers(t *testing.T) {
	fixedTimestamp, err := time.Parse(time.RFC3339, "2023-12-12T22:08:41Z")
	if err != nil {
		t.Fatalf("Failed to parse timestamp: %v", err)
	}

	pageSize := 10
	minTimestamp := fixedTimestamp.Add(-time.Hour)
	maxTimestamp := fixedTimestamp
	nonNilToken := []byte("non-nil-token")

	testCases := []struct {
		name          string
		setupMock     func(ctrl *gomock.Controller) *persistence.MockRetryer
		token         pagination.PageToken
		expectedPage  pagination.Page
		expectedError error
	}{
		{
			name: "Success",
			setupMock: func(ctrl *gomock.Controller) *persistence.MockRetryer {
				mockRetryer := persistence.NewMockRetryer(ctrl)
				timerTasks := []persistence.Task{
					&persistence.UserTimerTask{
						WorkflowIdentifier: persistence.WorkflowIdentifier{
							DomainID:   "testDomainID",
							WorkflowID: "testWorkflowID",
							RunID:      "testRunID",
						},
						TaskData: persistence.TaskData{
							VisibilityTimestamp: fixedTimestamp,
						},
					},
				}

				mockRetryer.EXPECT().
					GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
						TaskCategory:        persistence.HistoryTaskCategoryTimer,
						InclusiveMinTaskKey: persistence.NewHistoryTaskKey(minTimestamp, 0),
						ExclusiveMaxTaskKey: persistence.NewHistoryTaskKey(maxTimestamp, 0),
						PageSize:            pageSize,
						NextPageToken:       nil,
					}).
					Return(&persistence.GetHistoryTasksResponse{
						Tasks:         timerTasks,
						NextPageToken: nil,
					}, nil)

				mockRetryer.EXPECT().GetShardID().Return(123)

				return mockRetryer
			},
			token: nil,
			expectedPage: pagination.Page{
				Entities: []pagination.Entity{
					&entity.Timer{
						ShardID:             123,
						DomainID:            "testDomainID",
						WorkflowID:          "testWorkflowID",
						RunID:               "testRunID",
						TaskType:            persistence.TaskTypeUserTimer,
						VisibilityTimestamp: fixedTimestamp,
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "Non-nil Pagination Token Provided",
			setupMock: func(ctrl *gomock.Controller) *persistence.MockRetryer {
				mockRetryer := persistence.NewMockRetryer(ctrl)

				mockRetryer.EXPECT().
					GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
						TaskCategory:        persistence.HistoryTaskCategoryTimer,
						InclusiveMinTaskKey: persistence.NewHistoryTaskKey(minTimestamp, 0),
						ExclusiveMaxTaskKey: persistence.NewHistoryTaskKey(maxTimestamp, 0),
						PageSize:            pageSize,
						NextPageToken:       nonNilToken,
					}).
					Return(&persistence.GetHistoryTasksResponse{
						Tasks:         nil,
						NextPageToken: nonNilToken,
					}, nil)

				return mockRetryer
			},
			token: nonNilToken,
			expectedPage: pagination.Page{
				Entities:     nil,
				CurrentToken: nonNilToken,
				NextToken:    nonNilToken,
			},
			expectedError: nil,
		},
		{
			name: "Invalid Timer Causes Error",
			setupMock: func(ctrl *gomock.Controller) *persistence.MockRetryer {
				mockRetryer := persistence.NewMockRetryer(ctrl)

				invalidTimer := &persistence.UserTimerTask{
					WorkflowIdentifier: persistence.WorkflowIdentifier{
						DomainID:   "", // Invalid as it's empty
						WorkflowID: "testWorkflowID",
						RunID:      "testRunID",
					},
					TaskData: persistence.TaskData{
						VisibilityTimestamp: fixedTimestamp,
					},
				}

				mockRetryer.EXPECT().
					GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
						TaskCategory:        persistence.HistoryTaskCategoryTimer,
						InclusiveMinTaskKey: persistence.NewHistoryTaskKey(minTimestamp, 0),
						ExclusiveMaxTaskKey: persistence.NewHistoryTaskKey(maxTimestamp, 0),
						PageSize:            pageSize,
						NextPageToken:       nil,
					}).
					Return(&persistence.GetHistoryTasksResponse{
						Tasks:         []persistence.Task{invalidTimer},
						NextPageToken: nil,
					}, nil)

				mockRetryer.EXPECT().GetShardID().Return(123)

				return mockRetryer
			},
			token:         nil,
			expectedPage:  pagination.Page{},
			expectedError: fmt.Errorf("empty DomainID"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRetryer := tc.setupMock(ctrl)

			fetchFn := getUserTimers(mockRetryer, minTimestamp, maxTimestamp, pageSize)
			page, err := fetchFn(context.Background(), tc.token)

			if tc.expectedError != nil {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedError.Error(), "Error should match")
			} else {
				require.NoError(t, err, "No error is expected")
			}

			require.Equal(t, tc.expectedPage, page, "Page should match")
		})
	}
}
