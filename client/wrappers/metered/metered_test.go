package metered

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

func TestWrappers(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := frontend.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil).Times(1)

		retryableClient := NewFrontendClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.CountWorkflowExecutions(context.Background(), &types.CountWorkflowExecutionsRequest{})
		assert.NoError(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 1, "there should be a single counter registered")
		assert.Len(t, testScope.Snapshot().Timers(), 1, "there should be a single timer registered")
	})
	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := frontend.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("error"))

		retryableClient := NewFrontendClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.CountWorkflowExecutions(context.Background(), &types.CountWorkflowExecutionsRequest{})
		assert.Error(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 2, "there should be two counters registered, one for call and one for failure")
	})
}

// Matching has a special logic that should emit a metric if a request is forwarded.
func TestMatching(t *testing.T) {
	t.Run("forwarded", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := matching.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&types.AddActivityTaskResponse{}, nil).Times(1)

		retryableClient := NewMatchingClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.AddActivityTask(context.Background(), &types.AddActivityTaskRequest{
			ForwardedFrom: "test",
			TaskList:      &types.TaskList{Name: "test"},
		})
		assert.NoError(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 2, "there should be two counters registered, one for call and one for forwarded")
	})
	t.Run("forwarded poller", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := matching.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&types.MatchingPollForDecisionTaskResponse{}, nil).Times(1)

		retryableClient := NewMatchingClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.PollForDecisionTask(context.Background(), &types.MatchingPollForDecisionTaskRequest{
			ForwardedFrom: "test",
			PollRequest: &types.PollForDecisionTaskRequest{
				TaskList: &types.TaskList{Name: "test"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 2, "there should be two counters registered, one for call and one for forwarded")
	})
	t.Run("not forwarded", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := matching.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&types.AddActivityTaskResponse{}, nil).Times(1)

		retryableClient := NewMatchingClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.AddActivityTask(context.Background(), &types.AddActivityTaskRequest{
			ForwardedFrom: "",
			TaskList:      &types.TaskList{Name: "test"},
		})
		assert.NoError(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 1, "there should be one counters registered, one for call")
	})
	t.Run("invalid task list", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		clientMock := matching.NewMockClient(ctrl)

		testScope := tally.NewTestScope("", nil)
		metricsClient := metrics.NewClient(testScope, metrics.ServiceIdx(0), metrics.MigrationConfig{})

		clientMock.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&types.AddActivityTaskResponse{}, nil).Times(1)

		retryableClient := NewMatchingClient(
			clientMock,
			metricsClient)

		_, err := retryableClient.AddActivityTask(context.Background(), &types.AddActivityTaskRequest{
			ForwardedFrom: "",
			TaskList:      &types.TaskList{Name: constants.ReservedTaskListPrefix + "test"},
		})
		assert.NoError(t, err)
		assert.Len(t, testScope.Snapshot().Counters(), 2, "there should be two counters registered, one for call and one for invalid task list")
	})
}
