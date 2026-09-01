package host

import "flag"

// TestFlags contains the feature flags for integration tests
var TestFlags struct {
	FrontendAddr          string
	PersistenceType       string
	SQLPluginName         string
	TestClusterConfigFile string
}

func init() {
	flag.StringVar(&TestFlags.FrontendAddr, "frontendAddress", "", "host:port for cadence frontend service")
	flag.StringVar(&TestFlags.PersistenceType, "persistenceType", "cassandra", "type of persistence store - [cassandra or sql]")
	flag.StringVar(&TestFlags.SQLPluginName, "sqlPluginName", "mysql", "type of sql store - [mysql or postgres]")
	flag.StringVar(&TestFlags.TestClusterConfigFile, "TestClusterConfigFile", "", "test cluster config file location")
}
