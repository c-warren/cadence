package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"
	"github.com/uber/cadence/testflags"
)

func TestMySQLHistoryV2PersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.HistoryV2PersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLMatchingPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.MatchingPersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLMetadataPersistenceSuiteV2(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.MetadataPersistenceSuiteV2)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLShardPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ShardPersistenceSuite)
	option, err := mysql.GetTestClusterOption()
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

func TestMySQLExecutionManagerSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(ExecutionManagerSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLExecutionManagerWithEventsV2(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLVisibilityPersistenceSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.DBVisibilityPersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLQueuePersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.QueuePersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLConfigPersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.ConfigStorePersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestMySQLDomainAuditPersistence(t *testing.T) {
	testflags.RequireMySQL(t)
	s := new(pt.DomainAuditPersistenceSuite)
	option, err := mysql.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}
