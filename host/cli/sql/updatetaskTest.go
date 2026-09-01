package sql

import (
	"log"
	"os"

	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/schema/mysql"
	"github.com/uber/cadence/tools/common/schema/test"
	"github.com/uber/cadence/tools/sql"
)

// UpdateSchemaTestSuite defines a test suite
type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
	pluginName string
}

// NewUpdateSchemaTestSuite returns a test suite
func NewUpdateSchemaTestSuite(pluginName string) *UpdateSchemaTestSuite {
	return &UpdateSchemaTestSuite{
		pluginName: pluginName,
	}
}

// SetupSuite setups test suite
func (s *UpdateSchemaTestSuite) SetupSuite() {
	os.Setenv("SQL_HOST", environment.GetMySQLAddress())
	os.Setenv("SQL_USER", environment.GetMySQLUser())
	os.Setenv("SQL_PASSWORD", environment.GetMySQLPassword())
	conn, err := newTestConn("", s.pluginName)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestUpdateSchema test
func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	s.RunUpdateSchemaTest(sql.BuildCLIOptions(), conn, "--db", createTestSQLFileContent(), []string{"task_maps", "tasks"})
}

// TestDryrun test
func (s *UpdateSchemaTestSuite) TestDryrun() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../schema/mysql/v8/cadence/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.Version)
}

// TestVisibilityDryrun test
func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../schema/mysql/v8/visibility/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.VisibilityVersion)
}
