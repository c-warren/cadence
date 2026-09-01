package sql

import (
	"context"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/tools/common/schema"
)

type (
	// Connection is the connection to database
	Connection struct {
		dbName  string
		adminDb sqlplugin.AdminDB
	}
)

var _ schema.SchemaClient = (*Connection)(nil)

// NewConnection creates a new connection to database
func NewConnection(cfg *config.SQL) (*Connection, error) {
	db, err := sql.NewSQLAdminDB(cfg)
	if err != nil {
		return nil, err
	}

	return &Connection{
		adminDb: db,
		dbName:  cfg.DatabaseName,
	}, nil
}

// CreateSchemaVersionTables sets up the schema version tables
func (c *Connection) CreateSchemaVersionTables() error {
	return c.adminDb.CreateSchemaVersionTables()
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (c *Connection) ReadSchemaVersion() (string, error) {
	return c.adminDb.ReadSchemaVersion(c.dbName)
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (c *Connection) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	return c.adminDb.UpdateSchemaVersion(c.dbName, newVersion, minCompatibleVersion)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (c *Connection) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	return c.adminDb.WriteSchemaUpdateLog(oldVersion, newVersion, manifestMD5, desc)
}

// ExecDDLQuery executes a sql statement
func (c *Connection) ExecDDLQuery(stmt string, args ...interface{}) error {
	// TODO pass in context timeout value from command line param
	err := c.adminDb.ExecSchemaOperationQuery(context.Background(), stmt, args...)
	return err
}

// ListTables returns a list of tables in this database
func (c *Connection) ListTables() ([]string, error) {
	return c.adminDb.ListTables(c.dbName)
}

// DropTable drops a given table from the database
func (c *Connection) DropTable(name string) error {
	return c.adminDb.DropTable(name)
}

// DropAllTables drops all tables from this database
func (c *Connection) DropAllTables() error {
	tables, err := c.ListTables()
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := c.DropTable(tab); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
func (c *Connection) CreateDatabase(name string) error {
	return c.adminDb.CreateDatabase(name)
}

// DropDatabase drops a database
func (c *Connection) DropDatabase(name string) error {
	return c.adminDb.DropDatabase(name)
}

// Close closes the sql client
func (c *Connection) Close() {
	if c.adminDb != nil {
		err := c.adminDb.Close()
		if err != nil {
			panic("cannot close connection")
		}
	}
}
