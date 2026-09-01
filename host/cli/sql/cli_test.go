package sql

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"
	"github.com/uber/cadence/testflags"
)

func TestMySQLConnTestSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	suite.Run(t, NewSQLConnTestSuite(mysql.PluginName))
}

func TestMySQLHandlerTestSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	suite.Run(t, NewHandlerTestSuite(mysql.PluginName))
}

func TestMySQLSetupSchemaTestSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	suite.Run(t, NewSetupSchemaTestSuite(mysql.PluginName))
}

func TestMySQLUpdateSchemaTestSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	suite.Run(t, NewUpdateSchemaTestSuite(mysql.PluginName))
}

func TestMySQLVersionTestSuite(t *testing.T) {
	testflags.RequireMySQL(t)
	suite.Run(t, NewVersionTestSuite(mysql.PluginName))
}
