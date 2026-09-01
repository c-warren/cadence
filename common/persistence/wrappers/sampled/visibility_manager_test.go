package sampled

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/tokenbucket"
	"github.com/uber/cadence/common/types"
)

func TestVisibilityManagerSampledCalls(t *testing.T) {
	for _, tc := range []struct {
		name        string
		priority    int
		prepareMock func(*persistence.MockVisibilityManager)
		operation   func(context.Context, string, persistence.VisibilityManager) error
	}{
		{
			name: "RecordWorkflowExecutionStarted",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().RecordWorkflowExecutionStarted(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				return m.RecordWorkflowExecutionStarted(ctx, &persistence.RecordWorkflowExecutionStartedRequest{
					Domain: domain,
				})
			},
		},
		{
			name: "RecordWorkflowExecutionClosed",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				return m.RecordWorkflowExecutionClosed(ctx, &persistence.RecordWorkflowExecutionClosedRequest{
					Domain: domain,
					Status: types.WorkflowExecutionCloseStatusCanceled,
				})
			},
		},
		{
			name:     "RecordWorkflowExecutionClosed_Completed",
			priority: 1,
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				return m.RecordWorkflowExecutionClosed(ctx, &persistence.RecordWorkflowExecutionClosedRequest{
					Domain: domain,
					Status: types.WorkflowExecutionCloseStatusCompleted,
				})
			},
		},
		{
			name: "UpsertWorkflowExecution",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().UpsertWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				return m.UpsertWorkflowExecution(ctx, &persistence.UpsertWorkflowExecutionRequest{
					Domain: domain,
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockedManager := persistence.NewMockVisibilityManager(ctrl)

			testDomain := "domain1"

			m := NewVisibilityManager(mockedManager, Params{
				Config:       &Config{},
				MetricClient: metrics.NewNoopMetricsClient(),
				Logger:       testlogger.New(t),
				TimeSource:   clock.NewMockedTimeSource(),
				RateLimiterFactoryFunc: rateLimiterStubFunc(map[string]tokenbucket.PriorityTokenBucket{
					testDomain: &tokenBucketFactoryStub{tokens: map[int]int{tc.priority: 1}},
				}),
			})

			tc.prepareMock(mockedManager)

			err := tc.operation(context.Background(), testDomain, m)
			assert.NoError(t, err, "first call should succeed")

			err = tc.operation(context.Background(), testDomain, m)
			assert.NoError(t, err, "second call should not fail, but underlying call should be blocked by rate limiter")
		})
	}
}

func TestVisibilityManagerListOperations(t *testing.T) {
	for _, tc := range []struct {
		name        string
		priority    int
		prepareMock func(*persistence.MockVisibilityManager)
		operation   func(context.Context, string, persistence.VisibilityManager) error
	}{
		{
			name: "ListOpenWorkflowExecutions",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListOpenWorkflowExecutions(ctx, &persistence.ListWorkflowExecutionsRequest{
					Domain: domain,
				})
				return err
			},
		},
		{
			name: "ListClosedWorkflowExecutions",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListClosedWorkflowExecutions(ctx, &persistence.ListWorkflowExecutionsRequest{
					Domain: domain,
				})
				return err
			},
		},
		{
			name: "ListOpenWorkflowExecutionsByType",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListOpenWorkflowExecutionsByType(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListOpenWorkflowExecutionsByType(ctx, &persistence.ListWorkflowExecutionsByTypeRequest{
					ListWorkflowExecutionsRequest: persistence.ListWorkflowExecutionsRequest{
						Domain: domain,
					},
				})
				return err
			},
		},
		{
			name: "ListClosedWorkflowExecutionsByType",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListClosedWorkflowExecutionsByType(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListClosedWorkflowExecutionsByType(ctx, &persistence.ListWorkflowExecutionsByTypeRequest{
					ListWorkflowExecutionsRequest: persistence.ListWorkflowExecutionsRequest{
						Domain: domain,
					},
				})
				return err
			},
		},
		{
			name: "ListOpenWorkflowExecutionsByWorkflowID",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListOpenWorkflowExecutionsByWorkflowID(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListOpenWorkflowExecutionsByWorkflowID(ctx, &persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: persistence.ListWorkflowExecutionsRequest{
						Domain: domain,
					},
				})
				return err
			},
		},
		{
			name: "ListClosedWorkflowExecutionsByWorkflowID",
			prepareMock: func(mock *persistence.MockVisibilityManager) {
				mock.EXPECT().ListClosedWorkflowExecutionsByWorkflowID(gomock.Any(), gomock.Any()).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Times(1)
			},
			operation: func(ctx context.Context, domain string, m persistence.VisibilityManager) error {
				_, err := m.ListClosedWorkflowExecutionsByWorkflowID(ctx, &persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: persistence.ListWorkflowExecutionsRequest{
						Domain: domain,
					},
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockedManager := persistence.NewMockVisibilityManager(ctrl)

			testDomain := "domain1"

			m := NewVisibilityManager(mockedManager, Params{
				Config:       &Config{},
				MetricClient: metrics.NewNoopMetricsClient(),
				Logger:       testlogger.New(t),
				TimeSource:   clock.NewMockedTimeSource(),
				RateLimiterFactoryFunc: rateLimiterStubFunc(map[string]tokenbucket.PriorityTokenBucket{
					testDomain: &tokenBucketFactoryStub{tokens: map[int]int{tc.priority: 1}},
				}),
			})

			tc.prepareMock(mockedManager)

			err := tc.operation(context.Background(), testDomain, m)
			assert.NoError(t, err, "first call should succeed")

			err = tc.operation(context.Background(), testDomain, m)
			assert.Error(t, err, "second call should fail since underlying call should be blocked by rate limiter")
		})
	}
}

func rateLimiterStubFunc(domainData map[string]tokenbucket.PriorityTokenBucket) RateLimiterFactoryFunc {
	return func(timeSource clock.TimeSource, numOfPriority int, qpsConfig dynamicproperties.IntPropertyFnWithDomainFilter) RateLimiterFactory {
		return rateLimiterStub{domainData}
	}
}

type rateLimiterStub struct {
	data map[string]tokenbucket.PriorityTokenBucket
}

func (r rateLimiterStub) GetRateLimiter(domain string) tokenbucket.PriorityTokenBucket {
	return r.data[domain]
}

type tokenBucketFactoryStub struct {
	tokens map[int]int
}

func (t *tokenBucketFactoryStub) GetToken(priority, count int) (bool, time.Duration) {
	val := t.tokens[priority]
	if count > val {
		return false, time.Duration(0)
	}
	val -= count
	t.tokens[priority] = val
	return true, time.Duration(0)
}
