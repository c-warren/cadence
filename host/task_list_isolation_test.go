package host

import (
	"flag"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/types"
)

const (
	isolationTl = "integration-task-list-isolation-tl"
)

func TestTaskListIsolationSuite(t *testing.T) {
	flag.Parse()

	var isolationGroups = []any{
		"a", "b", "c",
	}
	clusterConfig, err := GetTestClusterConfig("testdata/task_list_test_cluster.yaml")
	if err != nil {
		panic(err)
	}
	testCluster := NewPersistenceTestCluster(t, clusterConfig)
	clusterConfig.FrontendDynamicConfigOverrides = map[dynamicproperties.Key]interface{}{
		dynamicproperties.EnableTasklistIsolation:            true,
		dynamicproperties.AllIsolationGroups:                 isolationGroups,
		dynamicproperties.MatchingNumTasklistWritePartitions: 1,
		dynamicproperties.MatchingNumTasklistReadPartitions:  1,
	}
	clusterConfig.HistoryDynamicConfigOverrides = map[dynamicproperties.Key]interface{}{
		dynamicproperties.EnableTasklistIsolation:            true,
		dynamicproperties.AllIsolationGroups:                 isolationGroups,
		dynamicproperties.MatchingNumTasklistWritePartitions: 1,
		dynamicproperties.MatchingNumTasklistReadPartitions:  1,
	}
	clusterConfig.MatchingDynamicConfigOverrides = map[dynamicproperties.Key]interface{}{
		dynamicproperties.EnableTasklistIsolation:            true,
		dynamicproperties.AllIsolationGroups:                 isolationGroups,
		dynamicproperties.TaskIsolationDuration:              time.Second * 5,
		dynamicproperties.MatchingNumTasklistWritePartitions: 1,
		dynamicproperties.MatchingNumTasklistReadPartitions:  1,
	}

	s := new(TaskListIsolationIntegrationSuite)
	params := IntegrationBaseParams{
		PersistenceConfig: testCluster,
		TestClusterConfig: clusterConfig,
	}
	s.IntegrationBase = NewIntegrationBase(params)
	suite.Run(t, s)
}

func (s *TaskListIsolationIntegrationSuite) SetupSuite() {
	s.setupSuite()
}

func (s *TaskListIsolationIntegrationSuite) TearDownSuite() {
	s.TearDownBaseSuite()
}

func (s *TaskListIsolationIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *TaskListIsolationIntegrationSuite) TestTaskListIsolation() {
	aPoller := s.createPoller("a")
	bPoller := s.createPoller("b")

	cancelB := bPoller.PollAndProcessDecisions()
	defer cancelB()
	cancelA := aPoller.PollAndProcessDecisions()
	defer cancelA()

	// Give pollers time to start
	time.Sleep(time.Second)

	// Running a single workflow is a bit of a coinflip: if isolation didn't work, it would pass 50% of the time.
	// Run 10 workflows to demonstrate that we consistently isolate tasks to the correct poller
	for i := 0; i < 10; i++ {
		runID := s.startWorkflow("a").RunID
		result, err := s.GetWorkflowResult(runID)
		s.NoError(err)
		s.Equal("a", result)
	}
}

func (s *TaskListIsolationIntegrationSuite) TestTaskListIsolationLeak_SLOW() {
	runID := s.startWorkflow("a").RunID

	bPoller := s.createPoller("b")
	// B will get the task as there are no pollers from A
	cancelB := bPoller.PollAndProcessDecisions()
	defer cancelB()

	result, err := s.GetWorkflowResult(runID)
	s.NoError(err)
	s.Equal("b", result)
}

func (s *TaskListIsolationIntegrationSuite) createPoller(group string) *TaskPoller {
	return &TaskPoller{
		Engine:   s.Engine,
		Domain:   s.DomainName,
		TaskList: &types.TaskList{Name: isolationTl, Kind: types.TaskListKindNormal.Ptr()},
		Identity: group,
		DecisionHandler: func(execution *types.WorkflowExecution, wt *types.WorkflowType, previousStartedEventID, startedEventID int64, history *types.History) ([]byte, []*types.Decision, error) {
			// Complete the workflow with the group name
			return []byte(strconv.Itoa(0)), []*types.Decision{{
				DecisionType: types.DecisionTypeCompleteWorkflowExecution.Ptr(),
				CompleteWorkflowExecutionDecisionAttributes: &types.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte(group),
				},
			}}, nil
		},
		Logger:      s.Logger,
		T:           s.T(),
		CallOptions: []yarpc.CallOption{withIsolationGroup(group)},
	}
}

func (s *TaskListIsolationIntegrationSuite) startWorkflow(group string) *types.StartWorkflowExecutionResponse {
	identity := "test"

	request := &types.StartWorkflowExecutionRequest{
		RequestID:  uuid.New(),
		Domain:     s.DomainName,
		WorkflowID: s.T().Name(),
		WorkflowType: &types.WorkflowType{
			Name: "integration-task-list-isolation-type",
		},
		TaskList: &types.TaskList{
			Name: isolationTl,
			Kind: types.TaskListKindNormal.Ptr(),
		},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(10),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            identity,
		WorkflowIDReusePolicy:               types.WorkflowIDReusePolicyAllowDuplicate.Ptr(),
	}

	ctx, cancel := createContext()
	defer cancel()
	result, err := s.Engine.StartWorkflowExecution(ctx, request, withIsolationGroup(group))
	s.Nil(err)

	return result
}

func withIsolationGroup(group string) yarpc.CallOption {
	return yarpc.WithHeader(common.ClientIsolationGroupHeaderName, group)
}
