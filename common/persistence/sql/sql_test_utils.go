package sql

import (
	"fmt"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

// NewTestCluster returns a new SQL test cluster
func NewTestCluster(pluginName, dbName, username, password, host string, port int) (config.Persistence, error) {
	var connectAddr string
	// CloudSQL doesn't need a port, don't add it
	if port > 0 {
		connectAddr = fmt.Sprintf("%s:%d", host, port)
	} else {
		connectAddr = host
	}

	cfg := config.SQL{
		User:            username,
		Password:        password,
		ConnectAddr:     connectAddr,
		ConnectProtocol: "tcp",
		PluginName:      pluginName,
		DatabaseName:    dbName,
		NumShards:       4,
		EncodingType:    "thriftrw",
		DecodingTypes:   []string{"thriftrw"},
	}

	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {SQL: &cfg},
		},
		TransactionSizeLimit: dynamicproperties.GetIntPropertyFn(constants.DefaultTransactionSizeLimit),
		ErrorInjectionRate:   dynamicproperties.GetFloatPropertyFn(0),
		NumHistoryShards:     cfg.NumShards,
	}, nil
}
