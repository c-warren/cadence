package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/service/worker/scanner/tasklist"
)

type scannerWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestScannerWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(scannerWorkflowTestSuite))
}

func (s *scannerWorkflowTestSuite) TestWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(taskListScavengerActivityName, mock.Anything).Return(nil)
	env.ExecuteWorkflow(tlScannerWFTypeName)
	s.True(env.IsWorkflowCompleted())
}

func (s *scannerWorkflowTestSuite) TestScavengerActivity() {
	env := s.NewTestActivityEnvironment()
	controller := gomock.NewController(s.T())
	mockResource := resource.NewTest(s.T(), controller, metrics.Worker)
	defer mockResource.Finish(s.T())

	mockResource.TaskMgr.On("ListTaskList", mock.Anything, mock.Anything).Return(&p.ListTaskListResponse{}, nil)
	mockResource.TaskMgr.On("GetOrphanTasks", mock.Anything, mock.Anything).Return(&p.GetOrphanTasksResponse{}, nil)
	ctx := scannerContext{
		resource: mockResource,
		cfg: Config{
			TaskListScannerOptions: tasklist.Options{
				GetOrphanTasksPageSizeFn: dynamicproperties.GetIntPropertyFn(dynamicproperties.ScannerGetOrphanTasksPageSize.DefaultInt()),
				EnableCleaning:           dynamicproperties.GetBoolPropertyFn(true),
				ExecutorPollInterval:     time.Millisecond * 50,
			},
		},
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: NewScannerContext(context.Background(), "default-test-workflow-type-name", ctx),
	})
	tlScavengerHBInterval = time.Millisecond * 10
	_, err := env.ExecuteActivity(taskListScavengerActivityName)
	s.NoError(err)
}
