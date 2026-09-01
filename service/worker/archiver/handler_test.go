package archiver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/workflow"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
)

var (
	handlerTestMetrics *mmocks.Client
	handlerTestLogger  *log.MockLogger
)

type handlerSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerSuite))
}

func (s *handlerSuite) SetupSuite() {
	workflow.Register(handleHistoryRequestWorkflow)
	workflow.Register(handleVisibilityRequestWorkflow)
	workflow.Register(startAndFinishArchiverWorkflow)
}

func (s *handlerSuite) SetupTest() {
	handlerTestMetrics = &mmocks.Client{}
	handlerTestMetrics.On("StartTimer", mock.Anything, mock.Anything).Return(metrics.NopStopwatch()).Maybe()
	handlerTestMetrics.On("Scope", mock.Anything, mock.Anything).Return(metrics.NoopScope).Maybe()
	handlerTestLogger = log.NewMockLogger(gomock.NewController(s.T()))
	handlerTestLogger.EXPECT().WithTags(gomock.Any()).Return(handlerTestLogger).AnyTimes()
}

func (s *handlerSuite) TearDownTest() {
	handlerTestMetrics.AssertExpectations(s.T())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_NonRetryableError() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_ExpireRetryTimeout() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	timeoutErr := workflow.NewTimeoutError(shared.TimeoutTypeStartToClose)
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(timeoutErr)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadSuccess() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFails_NonRetryableError() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount).Once()
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		return cadence.NewCustomError(errDeleteNonRetriable.Error())
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFailsThenSucceeds() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	firstRun := true
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		if firstRun {
			firstRun = false
			return errors.New("some retryable error")
		}
		return nil
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Fail() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilityFailedAllRetiresCount).Once()
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Success() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestRunArchiver() {
	numRequests := 1000
	concurrency := 10
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStartedCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount).Times(concurrency)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount).Times(concurrency)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStoppedCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(startAndFinishArchiverWorkflow, concurrency, numRequests)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func handleHistoryRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleHistoryRequest(ctx, &request)
	return nil
}

func handleVisibilityRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleVisibilityRequest(ctx, &request)
	return nil
}

func startAndFinishArchiverWorkflow(ctx workflow.Context, concurrency int, numRequests int) error {
	requestCh := workflow.NewBufferedChannel(ctx, numRequests)
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, concurrency, requestCh)
	handler.Start()
	sentHashes := make([]uint64, numRequests)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for i := 0; i < numRequests; i++ {
			ar, hash := randomArchiveRequest()
			requestCh.Send(ctx, ar)
			sentHashes[i] = hash
		}
		requestCh.Close()
	})
	handledHashes := handler.Finished()
	if !hashesEqual(handledHashes, sentHashes) {
		return errors.New("handled hashes does not equal sent hashes")
	}
	return nil
}

func randomArchiveRequest() (ArchiveRequest, uint64) {
	ar := ArchiveRequest{
		DomainID:   fmt.Sprintf("%v", rand.Intn(1000)),
		WorkflowID: fmt.Sprintf("%v", rand.Intn(1000)),
		RunID:      fmt.Sprintf("%v", rand.Intn(1000)),
		Targets:    []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
	}
	return ar, hash(ar)
}
