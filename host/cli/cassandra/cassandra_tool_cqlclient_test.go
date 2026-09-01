package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/testflags"
	"github.com/uber/cadence/tools/cassandra"
	"github.com/uber/cadence/tools/common/schema/test"

	_ "github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql/public" // needed to load the default gocql client
)

type (
	CQLClientTestSuite struct {
		test.DBTestBase
	}
)

var _ test.DB = (cassandra.CqlClient)(nil)

func TestCQLClientTestSuite(t *testing.T) {
	testflags.RequireCassandra(t)
	suite.Run(t, new(CQLClientTestSuite))
}

func (s *CQLClientTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *CQLClientTestSuite) SetupSuite() {
	client, err := NewTestCQLClient(cassandra.SystemKeyspace)
	if err != nil {
		s.Log.Fatal("error creating CQLClient, ", tag.Error(err))
	}
	s.SetupSuiteBase(client)
}

func (s *CQLClientTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *CQLClientTestSuite) TestParseCQLFile() {
	s.RunParseFileTest(CreateTestCQLFileContent())
}

func (s *CQLClientTestSuite) TestCQLClient() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	s.RunCreateTest(client)
	s.RunUpdateTest(client)
	s.RunDropTest(client)
	client.Close()
}
