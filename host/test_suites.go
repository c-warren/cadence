package host

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
)

// NOTE: the following definitions can't be defined in *_test.go
// since they need to be exported and used by our internal tests

type (
	IntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		*IntegrationBase
	}

	IntegrationQueueV2Suite struct {
		*IntegrationSuite
	}

	SizeLimitIntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		*IntegrationBase
	}

	ClientIntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		*IntegrationBase
		wfService workflowserviceclient.Interface
		wfClient  client.Client
		worker    worker.Worker
		taskList  string
	}

	AsyncWFIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	WorkflowIDRateLimitIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	WorkflowIDInternalRateLimitIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	TaskListIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase

		TaskListName string
	}

	TaskListIsolationIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	DecisionTimeoutMaxAttemptsIntegrationSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	WorkflowTimerTaskCleanupSuite struct {
		*require.Assertions
		*IntegrationBase
	}

	WorkflowTimerTaskCleanupDisabledSuite struct {
		*require.Assertions
		*IntegrationBase
	}
)
