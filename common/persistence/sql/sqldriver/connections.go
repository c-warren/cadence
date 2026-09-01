package sqldriver

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

type CreateSingleDBConn func(cfg *config.SQL) (*sqlx.DB, error)

// CreateDBConnections returns references to logical connections to the underlying SQL databases.
// By default when UseMultipleDatabases == false, the returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on the tables in the database.
// If UseMultipleDatabases == true then return connections to all the databases
func CreateDBConnections(cfg *config.SQL, createConnFunc CreateSingleDBConn, closer CloseFunc) (Driver, error) {
	if !cfg.UseMultipleDatabases {
		xdb, err := createConnFunc(cfg)
		if err != nil {
			return nil, err
		}
		return newSingletonSQLDriver(xdb, nil, closer), nil
	}
	if cfg.NumShards <= 1 || len(cfg.MultipleDatabasesConfig) != cfg.NumShards {
		return nil, fmt.Errorf("invalid SQL config. NumShards should be > 1 and equal to the length of MultipleDatabasesConfig")
	}

	// recover from the original at the end
	defer func() {
		cfg.User = ""
		cfg.Password = ""
		cfg.DatabaseName = ""
		cfg.ConnectAddr = ""
	}()

	xdbs := make([]*sqlx.DB, cfg.NumShards)
	for idx, entry := range cfg.MultipleDatabasesConfig {
		cfg.User = entry.User
		cfg.Password = entry.Password
		cfg.DatabaseName = entry.DatabaseName
		cfg.ConnectAddr = entry.ConnectAddr
		xdb, err := createConnFunc(cfg)
		if err != nil {
			return nil, fmt.Errorf("got error of %v to connect to %v database with config %v", err, idx, cfg)
		}
		xdbs[idx] = xdb
	}
	return newShardedSQLDriver(xdbs, nil, sqlplugin.DbShardUndefined, closer), nil
}
