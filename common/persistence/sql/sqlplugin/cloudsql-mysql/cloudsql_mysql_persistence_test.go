package cloudsqlmysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/testflags"
)

// This is intentionally in a weird spot because it's part of a separate go module
// It also requires considerable manual configuration to actually run, such as provisioning the necessary cloud
// resources

func TestCloudSQLMySQLHistoryV2PersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.HistoryV2PersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLMatchingPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.MatchingPersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLMetadataPersistenceSuiteV2(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.MetadataPersistenceSuiteV2)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLShardPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ShardPersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

type ExecutionManagerSuite struct {
	pt.ExecutionManagerSuite
}

func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in mysql")
}

func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in mysql")
}

func TestCloudSQLMySQLExecutionManagerSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(ExecutionManagerSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLExecutionManagerWithEventsV2(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLVisibilityPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.DBVisibilityPersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLQueuePersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.QueuePersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLConfigPersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ConfigStorePersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCloudSQLMySQLDomainAuditPersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.DomainAuditPersistenceSuite)
	option, err := GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}
