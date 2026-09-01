//go:build !race

package sqlite

import (
	"testing"

	"github.com/stretchr/testify/suite"

	pt "github.com/uber/cadence/common/persistence/persistence-tests"

	_ "github.com/ncruces/go-sqlite3/driver" // register sqlite3 driver for tests
	_ "github.com/ncruces/go-sqlite3/embed"  // embed sqlite db for tests
)

func TestSQLiteHistoryV2PersistenceSuite(t *testing.T) {
	s := new(pt.HistoryV2PersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteMatchingPersistenceSuite(t *testing.T) {
	s := new(pt.MatchingPersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(pt.MetadataPersistenceSuiteV2)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteShardPersistenceSuite(t *testing.T) {
	s := new(pt.ShardPersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

type ExecutionManagerSuite struct {
	pt.ExecutionManagerSuite
}

func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in sqlite")
}

func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in sqlite")
}

func TestSQLiteExecutionManagerSuite(t *testing.T) {
	s := new(ExecutionManagerSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteExecutionManagerWithEventsV2(t *testing.T) {
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteVisibilityPersistenceSuite(t *testing.T) {
	s := new(pt.DBVisibilityPersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteQueuePersistence(t *testing.T) {
	s := new(pt.QueuePersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteConfigPersistence(t *testing.T) {
	s := new(pt.ConfigStorePersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLiteDomainAuditPersistence(t *testing.T) {
	s := new(pt.DomainAuditPersistenceSuite)
	option := GetTestClusterOption()
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}
