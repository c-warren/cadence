// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package host

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/zap"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	persistenceclient "github.com/uber/cadence/common/persistence/client"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
)

func init() {
	workflow.Register(testDataConverterWorkflow)
	activity.Register(testActivity)
	workflow.Register(testParentWorkflow)
	workflow.Register(testChildWorkflow)
	workflow.Register(asyncTimerWorkflow)
	workflow.Register(asyncLongTimerWorkflow)
	workflow.Register(asyncChildParentWorkflow)
	workflow.Register(asyncChildWorkflow)
	workflow.Register(asyncPriorityChildParentWorkflow)
	workflow.Register(asyncPriorityChildWorkflow)
}

func TestClientIntegrationSuite(t *testing.T) {

	flag.Parse()

	clusterConfig, err := GetTestClusterConfig("testdata/clientintegrationtestcluster.yaml")
	if err != nil {
		panic(err)
	}
	testCluster := NewPersistenceTestCluster(t, clusterConfig)

	s := new(ClientIntegrationSuite)
	params := IntegrationBaseParams{
		DefaultTestCluster:    testCluster,
		VisibilityTestCluster: testCluster,
		TestClusterConfig:     clusterConfig,
	}
	s.IntegrationBase = NewIntegrationBase(params)
	suite.Run(t, s)
}

func (s *ClientIntegrationSuite) SetupSuite() {
	s.setupSuite()

	var err error
	s.wfService, err = s.buildServiceClient()
	if err != nil {
		s.Logger.Fatal("Error when build service client", tag.Error(err))
	}
	s.wfClient = client.NewClient(s.wfService, s.DomainName, nil)

	s.taskList = "client-integration-test-tasklist"
	s.worker = worker.New(s.wfService, s.DomainName, s.taskList, worker.Options{})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker", tag.Error(err))
	} else {
		s.Logger.Info("Worker started")
	}
}

func (s *ClientIntegrationSuite) TearDownSuite() {
	s.worker.Stop()
	s.TearDownBaseSuite()
}

func (s *ClientIntegrationSuite) buildServiceClient() (workflowserviceclient.Interface, error) {
	cadenceClientName := "cadence-client"
	hostPort := "127.0.0.1:7114" // use grpc port
	if TestFlags.FrontendAddr != "" {
		hostPort = TestFlags.FrontendAddr
	}
	ch := grpc.NewTransport(
		grpc.ServerMaxRecvMsgSize(32*1024*1024),
		grpc.ClientMaxRecvMsgSize(32*1024*1024),
	)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: cadenceClientName,
		Outbounds: yarpc.Outbounds{
			service.Frontend: {Unary: ch.NewSingleOutbound(hostPort)},
		},
	})
	if dispatcher == nil {
		s.Logger.Fatal("No RPC dispatcher provided to create a connection to Cadence Service")
	}
	if err := dispatcher.Start(); err != nil {
		s.Logger.Fatal("Failed to create outbound transport channel", tag.Error(err))
	}
	cc := dispatcher.ClientConfig(service.Frontend)
	return compatibility.NewThrift2ProtoAdapter(
		compatibility.AdapterClients{
			Domain:     apiv1.NewDomainAPIYARPCClient(cc),
			Workflow:   apiv1.NewWorkflowAPIYARPCClient(cc),
			Worker:     apiv1.NewWorkerAPIYARPCClient(cc),
			Visibility: apiv1.NewVisibilityAPIYARPCClient(cc),
			Schedule:   apiv1.NewScheduleAPIYARPCClient(cc),
		},
	), nil
}

func (s *ClientIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// testDataConverter implements encoded.DataConverter using gob
type testDataConverter struct {
	NumOfCallToData   int // for testing to know testDataConverter is called as expected
	NumOfCallFromData int
}

func (tdc *testDataConverter) ToData(value ...interface{}) ([]byte, error) {
	tdc.NumOfCallToData++
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	for i, obj := range value {
		if err := enc.Encode(obj); err != nil {
			return nil, fmt.Errorf(
				"unable to encode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return buf.Bytes(), nil
}

func (tdc *testDataConverter) FromData(input []byte, valuePtr ...interface{}) error {
	tdc.NumOfCallFromData++
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	for i, obj := range valuePtr {
		if err := dec.Decode(obj); err != nil {
			return fmt.Errorf(
				"unable to decode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return nil
}

func newTestDataConverter() encoded.DataConverter {
	return &testDataConverter{}
}

func testActivity(ctx context.Context, msg string) (string, error) {
	return "hello_" + msg, nil
}

func testDataConverterWorkflow(ctx workflow.Context, tl string) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 20 * time.Second,
		StartToCloseTimeout:    40 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, testActivity, "world").Get(ctx, &result)
	if err != nil {
		return "", err
	}

	// use another converter to run activity,
	// with new taskList so that worker with same data converter can properly process tasks.
	var result1 string
	ctx1 := workflow.WithDataConverter(ctx, newTestDataConverter())
	ctx1 = workflow.WithTaskList(ctx1, tl)
	err1 := workflow.ExecuteActivity(ctx1, testActivity, "world1").Get(ctx1, &result1)
	if err1 != nil {
		return "", err1
	}
	return result + "," + result1, nil
}

func (s *ClientIntegrationSuite) startWorkerWithDataConverter(tl string, dataConverter encoded.DataConverter) worker.Worker {
	opts := worker.Options{}
	if dataConverter != nil {
		opts.DataConverter = dataConverter
	}
	worker := worker.New(s.wfService, s.DomainName, tl, opts)
	if err := worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker with data converter", tag.Error(err))
	}
	return worker
}

func (s *ClientIntegrationSuite) TestClientDataConverter() {
	tl := "client-integration-data-converter-activity-tasklist"
	dc := newTestDataConverter()
	worker := s.startWorkerWithDataConverter(tl, dc)
	defer worker.Stop()

	id := "client-integration-data-converter-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("hello_world,hello_world1", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(1, d.NumOfCallToData)
	s.Equal(1, d.NumOfCallFromData)
}

func (s *ClientIntegrationSuite) TestClientDataConverter_Failed() {
	tl := "client-integration-data-converter-activity-failed-tasklist"
	worker := s.startWorkerWithDataConverter(tl, nil) // mismatch of data converter
	defer worker.Stop()

	id := "client-integration-data-converter-failed-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.Error(err)

	// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
	iter := s.wfClient.GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
	completedAct := 0
	failedAct := 0
	for iter.HasNext() {
		event, err := iter.Next()
		s.Nil(err)
		if event.GetEventType() == shared.EventTypeActivityTaskCompleted {
			completedAct++
		}
		if event.GetEventType() == shared.EventTypeActivityTaskFailed {
			failedAct++
			attr := event.ActivityTaskFailedEventAttributes
			s.True(strings.HasPrefix(string(attr.Details), "unable to decode the activity function input bytes with error"))
		}
	}
	s.Equal(1, completedAct)
	s.Equal(1, failedAct)
}

var childTaskList = "client-integration-data-converter-child-tasklist"

func testParentWorkflow(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	execution := workflow.GetInfo(ctx).WorkflowExecution
	childID := fmt.Sprintf("child_workflow:%v", execution.RunID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:                   childID,
		ExecutionStartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, testChildWorkflow, 0, 3).Get(ctx, &result)
	if err != nil {
		logger.Error("Parent execution received child execution failure.", zap.Error(err))
		return "", err
	}

	childID1 := fmt.Sprintf("child_workflow1:%v", execution.RunID)
	cwo1 := workflow.ChildWorkflowOptions{
		WorkflowID:                   childID1,
		ExecutionStartToCloseTimeout: time.Minute,
		TaskList:                     childTaskList,
	}
	ctx1 := workflow.WithChildOptions(ctx, cwo1)
	ctx1 = workflow.WithDataConverter(ctx1, newTestDataConverter())
	var result1 string
	err1 := workflow.ExecuteChildWorkflow(ctx1, testChildWorkflow, 0, 2).Get(ctx1, &result1)
	if err1 != nil {
		logger.Error("Parent execution received child execution 1 failure.", zap.Error(err1))
		return "", err1
	}

	res := fmt.Sprintf("Complete child1 %s times, complete child2 %s times", result, result1)
	logger.Info("Parent execution completed.", zap.String("Result", res))
	return res, nil
}

func testChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow execution started.")
	if runCount <= 0 {
		logger.Error("Invalid valid for run count.", zap.Int("RunCount", runCount))
		return "", errors.New("invalid run count")
	}

	totalCount++
	runCount--
	if runCount == 0 {
		result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
		logger.Info("Child workflow completed.", zap.String("Result", result))
		return strconv.Itoa(totalCount), nil
	}

	logger.Info("Child workflow starting new run.", zap.Int("RunCount", runCount), zap.Int("TotalCount",
		totalCount))
	return "", workflow.NewContinueAsNewError(ctx, testChildWorkflow, totalCount, runCount)
}

func (s *ClientIntegrationSuite) TestClientDataConverter_WithChild() {
	dc := newTestDataConverter()
	worker := s.startWorkerWithDataConverter(childTaskList, dc)
	defer worker.Stop()

	id := "client-integration-data-converter-with-child-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("Complete child1 3 times, complete child2 2 times", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(3, d.NumOfCallToData)
	s.Equal(2, d.NumOfCallFromData)
}

func (s *ClientIntegrationSuite) Test_StickyWorkerRestartDecisionTask_SLOW() {
	testCases := []struct {
		name       string
		waitTime   time.Duration
		doQuery    bool
		doSignal   bool
		delayCheck func(duration time.Duration) bool
	}{
		{
			name:     "new_query_after_10s_no_delay",
			waitTime: 10 * time.Second,
			doQuery:  true,
			delayCheck: func(duration time.Duration) bool {
				return duration < 5*time.Second
			},
		},
		{
			name:     "new_query_immediately_expect_5s_delay",
			waitTime: 0,
			doQuery:  true,
			delayCheck: func(duration time.Duration) bool {
				return duration > 5*time.Second
			},
		},
		{
			name:     "new_workflow_task_after_10s_no_delay",
			waitTime: 10 * time.Second,
			doSignal: true,
			delayCheck: func(duration time.Duration) bool {
				return duration < 5*time.Second
			},
		},
		{
			name:     "new_workflow_task_immediately_expect_5s_delay",
			waitTime: 0,
			doSignal: true,
			delayCheck: func(duration time.Duration) bool {
				return duration > 5*time.Second
			},
		},
	}
	for _, tt := range testCases {
		s.Run(tt.name, func() {
			workflowFn := func(ctx workflow.Context) (string, error) {
				workflow.SetQueryHandler(ctx, "test", func() (string, error) {
					return "query works", nil
				})

				signalCh := workflow.GetSignalChannel(ctx, "test")
				var msg string
				signalCh.Receive(ctx, &msg)
				return msg, nil
			}

			taskList := "task-list-" + tt.name

			oldWorker := worker.New(s.wfService, s.DomainName, taskList, worker.Options{})
			oldWorker.RegisterWorkflow(workflowFn)
			if err := oldWorker.Start(); err != nil {
				s.Logger.Fatal("Error when start worker", tag.Error(err))
			}

			id := "test-sticky-delay" + tt.name
			workflowOptions := client.StartWorkflowOptions{
				ID:                           id,
				TaskList:                     taskList,
				ExecutionStartToCloseTimeout: 20 * time.Second,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
			defer cancel()
			workflowRun, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			if err != nil {
				s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
			}

			s.NotNil(workflowRun)
			s.True(workflowRun.GetRunID() != "")

			findFirstWorkflowTaskCompleted := false
		WaitForFirstWorkflowTaskComplete:
			for i := 0; i < 10; i++ {
				// wait until first workflow task completed (so we know sticky is set on workflow)
				iter := s.wfClient.GetWorkflowHistory(ctx, id, "", false, 0)
				for iter.HasNext() {
					evt, err := iter.Next()
					s.NoError(err)
					if evt.GetEventType() == shared.EventTypeDecisionTaskCompleted {
						findFirstWorkflowTaskCompleted = true
						break WaitForFirstWorkflowTaskComplete
					}
				}
				time.Sleep(time.Second)
			}
			s.True(findFirstWorkflowTaskCompleted)

			// stop old worker
			oldWorker.Stop()

			// maybe wait for 10s, which will make matching aware the old sticky worker is unavailable
			time.Sleep(tt.waitTime)

			// start a new worker
			newWorker := worker.New(s.wfService, s.DomainName, taskList, worker.Options{})
			newWorker.RegisterWorkflow(workflowFn)
			if err := newWorker.Start(); err != nil {
				s.Logger.Fatal("Error when start worker", tag.Error(err))
			}
			defer newWorker.Stop()

			startTime := time.Now()
			// send a signal, and workflow should complete immediately, there should not be 5s delay
			if tt.doSignal {
				err = s.wfClient.SignalWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				err = workflowRun.Get(ctx, nil)
				s.NoError(err)
			} else if tt.doQuery {
				// send a signal, and workflow should complete immediately, there should not be 5s delay
				queryResult, err := s.wfClient.QueryWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				var queryResultStr string
				err = queryResult.Get(&queryResultStr)
				s.NoError(err)
				s.Equal("query works", queryResultStr)
			}
			endTime := time.Now()
			duration := endTime.Sub(startTime)
			s.True(tt.delayCheck(duration), "delay check failed: %s", duration)
		})
	}
}

// Async (deprioritized) timer & child-workflow integration tests.
//
// These exercise the "async" priority end-to-end through the SDK client. For each of timers and
// child workflows we assert two things:
//   - correctness: the workflow completes successfully — deprioritization must never drop or stall
//     a task when the system is not backlogged;
//   - priority applied: the persisted TimerInfo / ChildExecutionInfo carries
//     persistence.TaskPriorityAsync.
//
// Backlog/ordering behavior under contention is intentionally not asserted here — it is
// non-deterministic in an integration test and is covered by unit tests on priorityAssignerImpl.

const (
	asyncChildTaskList         = "client-integration-async-child-tasklist"
	asyncPriorityChildTaskList = "client-integration-async-priority-child-tasklist"
)

// asyncTimerWorkflow sleeps briefly with async priority; used to prove an async timer fires and
// the workflow completes.
func asyncTimerWorkflow(ctx workflow.Context) error {
	return workflow.SleepWithOptions(ctx, time.Second, workflow.WithPriority(workflow.PriorityAsync))
}

// asyncLongTimerWorkflow sleeps for a long time with async priority so the pending TimerInfo is
// observable in mutable state before the timer fires.
func asyncLongTimerWorkflow(ctx workflow.Context) error {
	return workflow.SleepWithOptions(ctx, 10*time.Minute, workflow.WithPriority(workflow.PriorityAsync))
}

// asyncChildWorkflow returns immediately; child body for the async-child correctness test.
func asyncChildWorkflow(ctx workflow.Context) (string, error) {
	return "async-child-done", nil
}

// asyncChildParentWorkflow starts a child workflow with async priority and waits for it.
func asyncChildParentWorkflow(ctx workflow.Context) (string, error) {
	cwo := workflow.ChildWorkflowOptions{
		TaskList:                     asyncChildTaskList,
		Priority:                     workflow.PriorityAsync,
		ExecutionStartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var res string
	if err := workflow.ExecuteChildWorkflow(ctx, asyncChildWorkflow).Get(ctx, &res); err != nil {
		return "", err
	}
	return res, nil
}

// asyncPriorityChildWorkflow sleeps for a long time so the parent's ChildExecutionInfo entry
// remains pending long enough to inspect its persisted priority.
func asyncPriorityChildWorkflow(ctx workflow.Context) (string, error) {
	if err := workflow.Sleep(ctx, 10*time.Minute); err != nil {
		return "", err
	}
	return "async-child-done", nil
}

// asyncPriorityChildParentWorkflow starts a long-running child with async priority and waits.
func asyncPriorityChildParentWorkflow(ctx workflow.Context) (string, error) {
	cwo := workflow.ChildWorkflowOptions{
		TaskList:                     asyncPriorityChildTaskList,
		Priority:                     workflow.PriorityAsync,
		ExecutionStartToCloseTimeout: 15 * time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var res string
	if err := workflow.ExecuteChildWorkflow(ctx, asyncPriorityChildWorkflow).Get(ctx, &res); err != nil {
		return "", err
	}
	return res, nil
}

func (s *ClientIntegrationSuite) TestAsyncTimer_SLOW() {
	id := "client-integration-async-timer"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, asyncTimerWorkflow)
	s.NoError(err)
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	err = we.Get(ctx, nil)
	s.NoError(err)
}

func (s *ClientIntegrationSuite) TestAsyncChildWorkflow_SLOW() {
	worker := s.startWorkerWithDataConverter(asyncChildTaskList, nil)
	defer worker.Stop()

	id := "client-integration-async-child-workflow"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, asyncChildParentWorkflow)
	s.NoError(err)
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("async-child-done", res)
}

func (s *ClientIntegrationSuite) TestAsyncTimerPriorityPersisted_SLOW() {
	id := "client-integration-async-timer-priority"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 15 * time.Minute,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, asyncLongTimerWorkflow)
	s.NoError(err)
	s.NotNil(we)
	runID := we.GetRunID()
	s.True(runID != "")
	defer s.terminateWorkflow(id, runID)

	state := s.pollMutableStateUntil(id, runID, func(ms *persistence.WorkflowMutableState) bool {
		return len(ms.TimerInfos) > 0
	})
	s.NotEmpty(state.TimerInfos, "expected a pending timer in mutable state")
	for _, ti := range state.TimerInfos {
		s.Equal(persistence.TaskPriorityAsync, ti.Priority, "pending timer should carry async priority")
	}
}

func (s *ClientIntegrationSuite) TestAsyncChildWorkflowPriorityPersisted_SLOW() {
	worker := s.startWorkerWithDataConverter(asyncPriorityChildTaskList, nil)
	defer worker.Stop()

	id := "client-integration-async-child-priority"
	workflowOptions := client.StartWorkflowOptions{
		ID:                           id,
		TaskList:                     s.taskList,
		ExecutionStartToCloseTimeout: 15 * time.Minute,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	we, err := s.wfClient.ExecuteWorkflow(ctx, workflowOptions, asyncPriorityChildParentWorkflow)
	s.NoError(err)
	s.NotNil(we)
	runID := we.GetRunID()
	s.True(runID != "")
	defer s.terminateWorkflow(id, runID)

	state := s.pollMutableStateUntil(id, runID, func(ms *persistence.WorkflowMutableState) bool {
		return len(ms.ChildExecutionInfos) > 0
	})
	s.NotEmpty(state.ChildExecutionInfos, "expected a pending child execution in mutable state")
	for _, ci := range state.ChildExecutionInfos {
		s.Equal(persistence.TaskPriorityAsync, ci.Priority, "pending child execution should carry async priority")
	}
}

// terminateWorkflow best-effort terminates a long-running test workflow during cleanup.
func (s *ClientIntegrationSuite) terminateWorkflow(workflowID, runID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = s.wfClient.TerminateWorkflow(ctx, workflowID, runID, "test cleanup", nil)
}

// pollMutableStateUntil loads the workflow's mutable state directly from persistence and returns it
// once cond is satisfied, failing the test if the retry budget is exhausted. It observes an async
// timer / child task while it is still pending, before it fires or completes.
func (s *ClientIntegrationSuite) pollMutableStateUntil(
	workflowID, runID string,
	cond func(*persistence.WorkflowMutableState) bool,
) *persistence.WorkflowMutableState {
	domainCtx, domainCancel := context.WithTimeout(context.Background(), defaultTestPersistenceTimeout)
	domainResp, err := s.Engine.DescribeDomain(domainCtx, &types.DescribeDomainRequest{
		Name: common.StringPtr(s.DomainName),
	})
	domainCancel()
	s.NoError(err)
	domainID := domainResp.DomainInfo.GetUUID()

	// The ExecutionManager is shard-scoped, so build it for the shard that actually
	// owns this workflow (numHistoryShards > 1 means shard 0 usually isn't it).
	shardID := common.WorkflowIDToHistoryShard(workflowID, s.TestClusterConfig.HistoryConfig.NumHistoryShards)
	execMgr := s.newExecutionManager(shardID)
	defer execMgr.Close()

	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:   common.IntPtr(shardID),
		DomainID:  domainID,
		Execution: types.WorkflowExecution{WorkflowID: workflowID, RunID: runID},
	}
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTestPersistenceTimeout)
		resp, err := execMgr.GetWorkflowExecution(ctx, request)
		cancel()
		if err == nil && resp.State != nil && cond(resp.State) {
			return resp.State
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.Require().FailNow("timed out waiting for mutable state condition")
	return nil
}

// newExecutionManager builds a fresh ExecutionManager for the given shard against the same keyspace
// the history service uses, mirroring the helper in workflow_timer_task_cleanup_test.go.
func (s *ClientIntegrationSuite) newExecutionManager(shardID int) persistence.ExecutionManager {
	pConfig := s.TestCluster.testBase.DefaultTestCluster.Config()
	factory := persistenceclient.NewFactory(
		&pConfig,
		func() float64 { return 1000 },
		s.TestCluster.testBase.ClusterMetadata.GetCurrentClusterName(),
		metrics.NewNoopMetricsClient(),
		s.Logger,
		&s.TestCluster.testBase.DynamicConfiguration,
	)
	execMgr, err := factory.NewExecutionManager(shardID)
	s.Require().NoError(err)
	return execMgr
}
