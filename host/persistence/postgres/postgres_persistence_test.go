package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin/postgres"
	"github.com/uber/cadence/testflags"
)

func TestPostgresSQLHistoryV2PersistenceSuite(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.HistoryV2PersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLMatchingPersistenceSuite(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.MatchingPersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLMetadataPersistenceSuiteV2(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.MetadataPersistenceSuiteV2)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLShardPersistenceSuite(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.ShardPersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

type ExecutionManagerSuite struct {
	pt.ExecutionManagerSuite
}

func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in postgres sql")
}

func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionWithWorkflowRequestsDedup() {
	s.T().Skip("skip the test until we store workflow_request in postgres sql")
}

func (s *ExecutionManagerSuite) TestGetActiveClusterSelectionPolicy() {
	s.T().Skip("skip the test until we support ActiveClusterSelectionPolicy in postgres sql")
}

func (s *ExecutionManagerSuite) TestDeleteActiveClusterSelectionPolicy() {
	s.T().Skip("skip the test until we support ActiveClusterSelectionPolicy in postgres sql")
}

func TestPostgresSQLExecutionManagerSuite(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(ExecutionManagerSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLExecutionManagerWithEventsV2(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	option, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, option)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLVisibilityPersistenceSuite(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.DBVisibilityPersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

// TODO flaky test
// https://github.com/uber/cadence/issues/2877
/*
FAIL: TestPostgresSQLQueuePersistence/TestDomainReplicationQueue (0.26s)
        queuePersistenceTest.go:102:
            	Error Trace:	queuePersistenceTest.go:102
            	Error:      	Not equal:
            	            	expected: 99
            	            	actual  : 98
            	Test:       	TestPostgresSQLQueuePersistence/TestDomainReplicationQueue
*/
// func TestPostgresSQLQueuePersistence(t *testing.T) {
//	s := new(pt.QueuePersistenceSuite)
//	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
//	s.TestBase.Setup()
//	suite.Run(t, s)
// }

func TestPostgresSQLConfigPersistence(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.ConfigStorePersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestPostgresSQLDomainAuditPersistence(t *testing.T) {
	testflags.RequirePostgres(t)
	s := new(pt.DomainAuditPersistenceSuite)
	options, err := postgres.GetTestClusterOption()
	assert.NoError(t, err)
	s.TestBase = pt.NewTestBaseWithSQL(t, options)
	s.TestBase.Setup()
	suite.Run(t, s)
}
