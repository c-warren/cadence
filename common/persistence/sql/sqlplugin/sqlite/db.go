package sqlite

import (
	"context"

	"github.com/uber/cadence/common/persistence/sql/sqldriver"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"
)

var (
	_ sqlplugin.AdminDB = (*DB)(nil)
	_ sqlplugin.DB      = (*DB)(nil)
	_ sqlplugin.Tx      = (*DB)(nil)
)

// DB contains methods for managing objects in a sqlite database
// It inherits methods from the mysql.DB to reuse the implementation of the methods
// sqlplugin.ErrorChecker is customized for sqlite
type DB struct {
	*mysql.DB

	converter   mysql.DataConverter
	driver      sqldriver.Driver
	numDBShards int
	dsn         string
}

// NewDB returns an instance of DB, which contains a new created mysql.DB with sqlite specific methods
func NewDB(driver sqldriver.Driver, numDBShards int, dataConverter mysql.DataConverter, dsn string) (*DB, error) {
	return &DB{
		DB:          mysql.NewDB(driver, numDBShards, dataConverter),
		driver:      driver,
		numDBShards: numDBShards,
		converter:   dataConverter,
		dsn:         dsn,
	}, nil
}

// PluginName returns the name of the plugin
func (mdb *DB) PluginName() string {
	return PluginName
}

// BeginTx starts a new transaction and returns a new Tx
func (mdb *DB) BeginTx(ctx context.Context, dbShardID int) (sqlplugin.Tx, error) {
	driver, err := mdb.driver.BeginTransaction(ctx, dbShardID, nil)
	if err != nil {
		return nil, err
	}

	return NewDB(driver, mdb.numDBShards, mdb.converter, mdb.dsn)
}

func (mdb *DB) Close() error {
	return closeSharedDBConn(mdb.dsn, mdb.DB.Close)
}
