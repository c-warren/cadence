package sqlite

import (
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/persistence"
	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqldriver"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/schema/sqlite"
)

const (
	PluginName = "sqlite"
)

// SQLite plugin provides an sql persistence storage implementation for sqlite database
// Mostly the implementation reuses the mysql implementation
// If DatabaseName is not provided, then sqlite will use in-memory database,
// otherwise it will use the file as the database
type plugin struct{}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

// CreateDB wraps createDB to return an instance of sqlplugin.DB
func (p *plugin) CreateDB(cfg *config.SQL) (sqlplugin.DB, error) {
	return p.createDB(cfg)
}

// CreateAdminDB wraps createDB to return an instance of sqlplugin.AdminDB
func (p *plugin) CreateAdminDB(cfg *config.SQL) (sqlplugin.AdminDB, error) {
	return p.createDB(cfg)
}

func (p *plugin) GetSchema(dbType persistence.DBType) (persistence.Schema, error) {
	switch dbType {
	case persistence.DBTypeDefault:
		return sqlite.DefaultSchema, nil
	case persistence.DBTypeVisibility:
		return sqlite.VisibilitySchema, nil
	default:
		return nil, fmt.Errorf("unknown db type: %v", dbType)
	}
}

// createDB create a new instance of DB
func (p *plugin) createDB(cfg *config.SQL) (*DB, error) {
	driver, err := sqldriver.CreateDBConnections(cfg, p.createSingleDBConn, sqldriver.NoopClose)
	if err != nil {
		return nil, err
	}
	return NewDB(driver, cfg.NumShards, newConverter(), buildDSN(cfg))
}

// createSingleDBConn creates a single database connection for sqlite
// Plugin respects the following arguments MaxConns, MaxIdleConns, MaxConnLifetime
// Other arguments are used and described in buildDSN function
func (p *plugin) createSingleDBConn(cfg *config.SQL) (*sqlx.DB, error) {
	// Always use the shared pool, keyed by the full DSN.
	// This ensures that multiple services running in the same process (e.g. single-binary mode)
	// share one *sqlx.DB, which is required for both file-based SQLite (to avoid "database is
	// locked" errors) and in-memory SQLite (so all services operate on the same database).
	dsn := buildDSN(cfg)
	return createSharedDBConn(dsn, func() (*sqlx.DB, error) {
		return p.createDBConn(cfg)
	})
}

func (p *plugin) createDBConn(cfg *config.SQL) (*sqlx.DB, error) {
	db, err := sqlx.Connect("sqlite3", buildDSN(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %v", err)
	}

	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	} else {
		db.SetMaxOpenConns(1)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(1)
	}
	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}
	// For in-memory databases, the database is deleted when the last connection
	// closes. Set ConnMaxIdleTime to 0 (infinite) to prevent idle connections
	// from being reaped, which would destroy the database.
	if cfg.DatabaseName == "" {
		db.SetConnMaxIdleTime(0)
	}

	// Maps struct names in CamelCase to snake without need for DB struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

// GetTestClusterOption returns a test cluster option for sqlite plugin
// It uses a temporary directory for the database name
func GetTestClusterOption() *pt.TestBaseOptions {
	return &pt.TestBaseOptions{
		DBPluginName: PluginName,
		DBName:       path.Join(os.TempDir(), uuid.New().String()),
	}
}
