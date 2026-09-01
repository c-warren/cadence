package sqlite

import (
	"errors"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	listTablesQuery = "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	hasTableQuery   = "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name = ?"

	schemaVersionTableName       = "schema_version"
	schemaUpdateHistoryTableName = "schema_update_history"
)

// HasSchemaVersionTables checks if schema metadata tables exist.
func (mdb *DB) HasSchemaVersionTables() (bool, error) {
	hasVersionTable, err := mdb.hasTable(schemaVersionTableName)
	if err != nil || !hasVersionTable {
		return false, err
	}

	hasHistoryTable, err := mdb.hasTable(schemaUpdateHistoryTableName)
	if err != nil || !hasHistoryTable {
		return false, err
	}

	return true, nil
}

func (mdb *DB) hasTable(name string) (bool, error) {
	var count int
	err := mdb.driver.GetForSchemaQuery(sqlplugin.DbShardUndefined, &count, hasTableQuery, name)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// CreateDatabase is not supported by sqlite
// each sqlite file is a database
func (mdb *DB) CreateDatabase(_ string) error {
	return errors.New("sqlite doesn't support creating database")
}

// DatabaseExists is not supported by sqlite
// each sqlite file is a database
func (mdb *DB) DatabaseExists(_ string) (bool, error) {
	return true, nil
}

// DropDatabase is not supported by sqlite
// each sqlite file is a database
func (mdb *DB) DropDatabase(_ string) error {
	return nil
}

// ListTables returns a list of tables in this database
func (mdb *DB) ListTables(_ string) ([]string, error) {
	var tables []string
	err := mdb.driver.SelectForSchemaQuery(sqlplugin.DbShardUndefined, &tables, listTablesQuery)
	return tables, err
}
