package ndc

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/testing"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/host"
)

// NOTE: the following definitions can't be defined in *_test.go
// since they need to be exported and used by our internal tests

type (
	NDCIntegrationTestSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		active     *host.TestCluster
		generator  testing.Generator
		serializer persistence.PayloadSerializer
		logger     log.Logger

		domainName                  string
		domainID                    string
		version                     int64
		versionIncrement            int64
		mockAdminClient             map[string]admin.Client
		standByReplicationTasksChan chan *types.ReplicationTask
		standByTaskID               int64

		clusterConfigs    []*host.TestClusterConfig
		persistenceConfig config.Persistence
	}

	NDCIntegrationTestSuiteParams struct {
		ClusterConfigs    []*host.TestClusterConfig
		PersistenceConfig config.Persistence
	}
)

func NewNDCIntegrationTestSuite(params NDCIntegrationTestSuiteParams) *NDCIntegrationTestSuite {
	return &NDCIntegrationTestSuite{
		clusterConfigs:    params.ClusterConfigs,
		persistenceConfig: params.PersistenceConfig,
	}
}
