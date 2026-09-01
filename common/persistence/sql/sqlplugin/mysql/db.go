package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"

	"github.com/uber/cadence/common/persistence/sql/sqldriver"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

type (
	DB struct {
		converter   DataConverter
		driver      sqldriver.Driver
		numDBShards int
	}
)

// NewDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
// dbShardID is needed when tx is not nil
func NewDB(driver sqldriver.Driver, numDBShards int, converter DataConverter) *DB {
	return &DB{
		converter:   converter,
		driver:      driver,
		numDBShards: numDBShards,
	}
}

func (mdb *DB) GetTotalNumDBShards() int {
	return mdb.numDBShards
}

var _ sqlplugin.AdminDB = (*DB)(nil)
var _ sqlplugin.DB = (*DB)(nil)
var _ sqlplugin.Tx = (*DB)(nil)

func (mdb *DB) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	// ErrDupEntry MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
	// so we don't do the insert and return a ConditionalUpdate error.
	return ok && sqlErr.Number == mysqlerr.ER_DUP_ENTRY
}

func (mdb *DB) IsNotFoundError(err error) bool {
	return err == sql.ErrNoRows
}

func (mdb *DB) IsTimeoutError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	sqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		if sqlErr.Number == mysqlerr.ER_NET_READ_INTERRUPTED ||
			sqlErr.Number == mysqlerr.ER_NET_WRITE_INTERRUPTED ||
			sqlErr.Number == mysqlerr.ER_LOCK_WAIT_TIMEOUT ||
			sqlErr.Number == mysqlerr.ER_XA_RBTIMEOUT ||
			sqlErr.Number == mysqlerr.ER_QUERY_TIMEOUT ||
			sqlErr.Number == mysqlerr.ER_LOCKING_SERVICE_TIMEOUT ||
			sqlErr.Number == mysqlerr.ER_REGEXP_TIME_OUT {
			return true
		}
	}
	return false
}

func (mdb *DB) IsThrottlingError(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		if sqlErr.Number == mysqlerr.ER_CON_COUNT_ERROR ||
			sqlErr.Number == mysqlerr.ER_TOO_MANY_USER_CONNECTIONS ||
			sqlErr.Number == mysqlerr.ER_TOO_MANY_CONCURRENT_TRXS ||
			sqlErr.Number == mysqlerr.ER_CLONE_TOO_MANY_CONCURRENT_CLONES {
			return true
		}
	}
	return false
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *DB) BeginTx(ctx context.Context, dbShardID int) (sqlplugin.Tx, error) {
	driver, err := mdb.driver.BeginTransaction(ctx, dbShardID, nil)
	if err != nil {
		return nil, err
	}

	return NewDB(driver, mdb.numDBShards, mdb.converter), nil
}

// Commit commits a previously started transaction
func (mdb *DB) Commit() error {
	return mdb.driver.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (mdb *DB) Rollback() error {
	return mdb.driver.Rollback()
}

// Close closes the connection to the mysql db
func (mdb *DB) Close() error {
	return mdb.driver.Close()
}

// PluginName returns the name of the mysql plugin
func (mdb *DB) PluginName() string {
	return PluginName
}

// SupportsTTL returns weather MySQL supports TTL
func (mdb *DB) SupportsTTL() bool {
	return false
}

// MaxAllowedTTL returns the max allowed ttl MySQL supports
func (mdb *DB) MaxAllowedTTL() (*time.Duration, error) {
	return nil, sqlplugin.ErrTTLNotSupported
}

// SupportsTTL returns weather MySQL supports Asynchronous transaction
func (mdb *DB) SupportsAsyncTransaction() bool {
	return false
}
