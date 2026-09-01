package cassandra

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/schema/cassandra"
	"github.com/uber/cadence/testflags"
	cassandra2 "github.com/uber/cadence/tools/cassandra"
	"github.com/uber/cadence/tools/common/schema/test"
)

type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
}

func TestUpdateSchemaTestSuite(t *testing.T) {
	testflags.RequireCassandra(t)
	suite.Run(t, new(UpdateSchemaTestSuite))
}

func (s *UpdateSchemaTestSuite) SetupSuite() {
	client, err := NewTestCQLClient(cassandra2.SystemKeyspace)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(client)
}

func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	s.RunUpdateSchemaTest(cassandra2.BuildCLIOptions(), client, "-k", CreateTestCQLFileContent(), []string{"events", "tasks"})
}

func (s *UpdateSchemaTestSuite) TestDryrun() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := rootRelativePath + "schema/cassandra/cadence/versioned"
	s.RunDryrunTest(cassandra2.BuildCLIOptions(), client, "-k", dir, cassandra.Version)
}

func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := rootRelativePath + "schema/cassandra/visibility/versioned"
	s.RunDryrunTest(cassandra2.BuildCLIOptions(), client, "-k", dir, cassandra.VisibilityVersion)
}

func (s *UpdateSchemaTestSuite) TestShortcut() {
	client, err := NewTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := rootRelativePath + "schema/cassandra/cadence/versioned"

	cqlshArgs := []string{"--cqlversion=3.4.6", "-e", "DESC KEYSPACE %s;"}
	if cassandraHost := os.Getenv("CASSANDRA_HOST"); cassandraHost != "" {
		cqlshArgs = append(cqlshArgs, cassandraHost)
	}
	s.RunShortcutTest(cassandra2.BuildCLIOptions(), client, "-k", dir, "cqlsh", cqlshArgs...)
}
