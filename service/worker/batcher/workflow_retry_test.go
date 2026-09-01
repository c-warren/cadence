package batcher

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/types"
)

type workflowRetrySuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	workflowEnv  *testsuite.TestWorkflowEnvironment
	activityEnv  *testsuite.TestActivityEnvironment
	mockResource *resource.Test
	metricsMock  *mmocks.Client
}

func TestWorkflowRetrySuite(t *testing.T) {
	suite.Run(t, new(workflowRetrySuite))
}

func (s *workflowRetrySuite) SetupTest() {
	s.workflowEnv = s.NewTestWorkflowEnvironment()
	s.workflowEnv.RegisterWorkflow(BatchWorkflow)

	s.activityEnv = s.NewTestActivityEnvironment()
	s.activityEnv.RegisterActivity(BatchActivity)

	batcher, mockResource := setuptest(s.T())
	s.mockResource = mockResource

	s.metricsMock = &mmocks.Client{}
	batcher.metricsClient = s.metricsMock

	mockResource.FrontendClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(&types.DescribeDomainResponse{}, nil).AnyTimes()

	mockResource.FrontendClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&types.CountWorkflowExecutionsResponse{Count: 1}, nil).AnyTimes()
	mockResource.FrontendClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockResource.FrontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(&types.DescribeWorkflowExecutionResponse{}, nil).AnyTimes()

	ctx := context.WithValue(context.Background(), BatcherContextKey, batcher)
	workerOpts := worker.Options{
		MetricsScope:              tally.TestScope(nil),
		BackgroundActivityContext: ctx,
		Tracer:                    opentracing.GlobalTracer(),
	}
	s.workflowEnv.SetWorkerOptions(workerOpts)
	s.activityEnv.SetWorkerOptions(workerOpts)

}

func (s *workflowRetrySuite) TestActivityRetries() {
	params := createParams(BatchTypeCancel)
	params.MaxActivityRetries = 4

	s.metricsMock.On("IncCounter", metrics.BatcherScope, metrics.BatcherProcessorSuccess).Times(2)
	s.metricsMock.On("IncCounter", metrics.BatcherScope, metrics.BatcherProcessorFailures).Times(5)

	// First call to scan workflow executions succeeds, but returns a bad page token
	s.mockResource.FrontendClient.EXPECT().ScanWorkflowExecutions(gomock.Any(), &types.ListWorkflowExecutionsRequest{
		Domain:        params.DomainName,
		PageSize:      int32(params.PageSize),
		NextPageToken: nil,
		Query:         params.Query,
	}).
		Return(&types.ListWorkflowExecutionsResponse{
			Executions:    []*types.WorkflowExecutionInfo{{Execution: &types.WorkflowExecution{WorkflowID: "wid", RunID: "rid"}}},
			NextPageToken: []byte("bad-page-token"),
		}, nil).Times(1)

	// Max attempt is set to 4, so we try 5 times, 0,1,2,3,4
	s.mockResource.FrontendClient.EXPECT().ScanWorkflowExecutions(gomock.Any(), &types.ListWorkflowExecutionsRequest{
		Domain:        params.DomainName,
		PageSize:      int32(params.PageSize),
		NextPageToken: []byte("bad-page-token"),
		Query:         params.Query,
	}).
		Return(&types.ListWorkflowExecutionsResponse{
			Executions:    []*types.WorkflowExecutionInfo{{Execution: &types.WorkflowExecution{WorkflowID: "wid", RunID: "rid"}}},
			NextPageToken: []byte("bad-page-token"),
		}, assert.AnError).Times(5)

	s.workflowEnv.ExecuteWorkflow(BatchWorkflow, params)
	s.True(s.workflowEnv.IsWorkflowCompleted())
	s.ErrorContains(s.workflowEnv.GetWorkflowError(), assert.AnError.Error())
}

func (s *workflowRetrySuite) TearDownTest() {
	s.workflowEnv.AssertExpectations(s.T())
}
