package archiver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
)

var (
	workflowTestMetrics *mmocks.Client
	workflowTestLogger  log.Logger
	workflowTestHandler *MockHandler
	workflowTestPump    *MockPump
	workflowTestConfig  *Config
)

type workflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func (s *workflowSuite) SetupSuite() {
	workflow.Register(archivalWorkflowTest)
}

func TestWorkflowSuite(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	ctrl := gomock.NewController(s.T())
	workflowTestMetrics = &mmocks.Client{}
	workflowTestLogger = testlogger.New(s.T())
	workflowTestHandler = NewMockHandler(ctrl)
	workflowTestPump = NewMockPump(ctrl)
	workflowTestConfig = &Config{
		ArchiverConcurrency:           dynamicproperties.GetIntPropertyFn(0),
		ArchivalsPerIteration:         dynamicproperties.GetIntPropertyFn(0),
		TimeLimitPerArchivalIteration: dynamicproperties.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}

	// archival workflow dual-emits CadenceLatency timer + CadenceLatencyHistogram via
	// metricsClient.Scope(ArchiverArchivalWorkflowScope).ExponentialHistogram(...). Wire a
	// permissive scope mock so we don't have to repeat the expectation in every test.
	archivalScopeMock := &mmocks.Scope{}
	archivalScopeMock.On("ExponentialHistogram", metrics.CadenceLatencyHistogram, mock.Anything).Maybe()
	workflowTestMetrics.On("Scope", metrics.ArchiverArchivalWorkflowScope).Return(archivalScopeMock).Maybe()
}

func (s *workflowSuite) TestArchivalWorkflow_Fail_HashesDoNotEqual() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(3)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(3)).Once()
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverPumpedNotEqualHandledCount).Once()
	workflowTestHandler.EXPECT().Start().Times(1)
	workflowTestHandler.EXPECT().Finished().Return([]uint64{9, 7, 0}).Times(1)
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes: []uint64{8, 7, 0},
	}).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	var continueAsNewError *workflow.ContinueAsNewError
	ok := errors.As(env.GetWorkflowError(), &continueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Exit_TimeoutWithoutSignals() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(0)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(0)).Once()
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStoppingCount).Once()
	workflowTestHandler.EXPECT().Start().Times(1)
	workflowTestHandler.EXPECT().Finished().Return([]uint64{}).Times(1)
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes:          []uint64{},
		TimeoutWithoutSignals: true,
	}).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Success() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(5)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(5)).Once()
	workflowTestHandler.EXPECT().Start().Times(1)
	workflowTestHandler.EXPECT().Finished().Return([]uint64{1, 2, 3, 4, 5}).Times(1)
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes: []uint64{1, 2, 3, 4, 5},
	}).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	var continueAsNewError *workflow.ContinueAsNewError
	ok := errors.As(env.GetWorkflowError(), &continueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestReplayArchiveHistoryWorkflow() {
	logger := testlogger.NewZap(s.T())
	globalLogger = workflowTestLogger
	globalMetricsClient = metrics.NewClient(tally.NewTestScope("replay", nil), metrics.Worker, metrics.MigrationConfig{})
	globalConfig = &Config{
		ArchiverConcurrency:           dynamicproperties.GetIntPropertyFn(50),
		ArchivalsPerIteration:         dynamicproperties.GetIntPropertyFn(1000),
		TimeLimitPerArchivalIteration: dynamicproperties.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
	err := worker.ReplayWorkflowHistoryFromJSONFile(logger, "testdata/archival_workflow_history_v1.json")
	s.NoError(err)
}

func archivalWorkflowTest(ctx workflow.Context) error {
	return archivalWorkflowHelper(ctx, workflowTestLogger, workflowTestMetrics, workflowTestConfig, workflowTestHandler, workflowTestPump, nil)
}
