package task

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
)

type (
	fifoTaskSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		queueSize int

		scheduler *fifoTaskSchedulerImpl[PriorityTask]
	}
)

func TestFIFOTaskSchedulerSuite(t *testing.T) {
	s := new(fifoTaskSchedulerSuite)
	suite.Run(t, s)
}

func (s *fifoTaskSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockProcessor = NewMockProcessor(s.controller)

	s.queueSize = 2
	s.scheduler = NewFIFOTaskScheduler[PriorityTask](
		testlogger.New(s.Suite.T()),
		metrics.NewClient(tally.NoopScope, metrics.Common, metrics.MigrationConfig{}),
		&FIFOTaskSchedulerOptions{
			QueueSize:       s.queueSize,
			WorkerCount:     dynamicproperties.GetIntPropertyFn(1),
			DispatcherCount: 1,
			RetryPolicy:     backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	).(*fifoTaskSchedulerImpl[PriorityTask])
}

func (s *fifoTaskSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *fifoTaskSchedulerSuite) TestFIFO() {
	numTasks := 5
	tasks := []PriorityTask{}
	var taskWG sync.WaitGroup

	calls := []any{
		s.mockProcessor.EXPECT().Start(),
	}
	mockFn := func(_ Task) error {
		taskWG.Done()
		return nil
	}
	for i := 0; i != numTasks; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		tasks = append(tasks, mockTask)
		taskWG.Add(1)
		calls = append(calls, s.mockProcessor.EXPECT().Submit(newMockPriorityTaskMatcher(mockTask)).DoAndReturn(mockFn))
	}
	calls = append(calls, s.mockProcessor.EXPECT().Stop())
	gomock.InOrder(calls...)

	s.scheduler.processor = s.mockProcessor
	s.scheduler.Start()
	for _, task := range tasks {
		s.NoError(s.scheduler.Submit(task))
	}
	taskWG.Wait()
	s.scheduler.Stop()
}

func (s *fifoTaskSchedulerSuite) TestTrySubmit() {
	for i := 0; i != s.queueSize; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		submitted, err := s.scheduler.TrySubmit(mockTask)
		s.NoError(err)
		s.True(submitted)
	}

	// now the queue is full, submit one more task, should be non-blocking
	mockTask := NewMockPriorityTask(s.controller)
	submitted, err := s.scheduler.TrySubmit(mockTask)
	s.NoError(err)
	s.False(submitted)
}

func (s *fifoTaskSchedulerSuite) TestSchedulerContract() {
	testSchedulerContract(s.Assertions, s.controller, s.scheduler, nil)
}
