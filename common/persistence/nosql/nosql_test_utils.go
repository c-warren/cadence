package nosql

import (
	"testing"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

// TestClusterParams are params for test cluster initialization.
type TestClusterParams struct {
	PluginName    string
	KeySpace      string
	Username      string
	Password      string
	Host          string
	Port          int
	ProtoVersion  int
	SchemaBaseDir string
	// Replicas defaults to 1 if not set
	Replicas int
	// MaxConns defaults to 2 if not set
	MaxConns int
}

// NewTestCluster returns a new cassandra test cluster
func NewTestCluster(_ *testing.T, params TestClusterParams) config.Persistence {
	cfg := config.NoSQL{
		PluginName:   params.PluginName,
		User:         params.Username,
		Password:     params.Password,
		Hosts:        params.Host,
		Port:         params.Port,
		MaxConns:     maxConns(params.MaxConns),
		Keyspace:     params.KeySpace,
		ProtoVersion: params.ProtoVersion,
	}

	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {NoSQL: &cfg},
		},
		TransactionSizeLimit: dynamicproperties.GetIntPropertyFn(constants.DefaultTransactionSizeLimit),
		ErrorInjectionRate:   dynamicproperties.GetFloatPropertyFn(0),
	}
}

func maxConns(maxConns int) int {
	if maxConns == 0 {
		return 2
	}

	return maxConns
}
