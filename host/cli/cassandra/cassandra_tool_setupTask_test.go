package cassandra

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/testflags"
	"github.com/uber/cadence/tools/cassandra"
	"github.com/uber/cadence/tools/common/schema/test"
)

type (
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		client cassandra.CqlClient
	}
)

func TestSetupSchemaTestSuite(t *testing.T) {
	testflags.RequireCassandra(t)
	suite.Run(t, new(SetupSchemaTestSuite))
}

func (s *SetupSchemaTestSuite) SetupSuite() {
	os.Setenv("CASSANDRA_HOST", environment.GetCassandraAddress())
	client, err := NewTestCQLClient(cassandra.SystemKeyspace)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.client = client
	s.SetupSuiteBase(client)
}

func (s *SetupSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *SetupSchemaTestSuite) TestCreateKeyspace() {
	s.Nil(cassandra.RunTool([]string{"./tool", "create", "-k", "foobar123", "--rf", "1"}))
	err := s.client.DropKeyspace("foobar123")
	s.Nil(err)
}

func (s *SetupSchemaTestSuite) TestSetupSchema() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	s.RunSetupTest(cassandra.BuildCLIOptions(), client, "-k", CreateTestCQLFileContent(), []string{"tasks", "events"})
}
