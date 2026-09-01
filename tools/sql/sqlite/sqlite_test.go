package sqlite

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/config"
	sqliteplugin "github.com/uber/cadence/common/persistence/sql/sqlplugin/sqlite"
	"github.com/uber/cadence/schema/sqlite"
	"github.com/uber/cadence/tools/common/schema"
	"github.com/uber/cadence/tools/sql"

	_ "github.com/ncruces/go-sqlite3/driver" // register sqlite3 driver for tests
	_ "github.com/ncruces/go-sqlite3/embed"  // embed sqlite db for tests
)

// Test_SetupSchema test that setup schema works for all database sqlite schemas
// in-memory sqlite database is used for testing
func Test_SetupSchema(t *testing.T) {
	for _, dbName := range listDatabaseNames(t) {
		t.Run(dbName, func(t *testing.T) {
			conn := newTempFileDB(t)

			err := schema.SetupFromConfig(&schema.SetupConfig{
				SchemaFilePath:    fmt.Sprintf("../../../schema/sqlite/%s/schema.sql", dbName),
				InitialVersion:    "0.1",
				Overwrite:         false,
				DisableVersioning: false,
			}, conn)

			assert.NoError(t, err)
		})
	}
}

// newTempFileDB returns a new isolated sqlite connection backed by a unique temp file.
// Each call produces a distinct database so sub-tests don't share schema state.
func newTempFileDB(t *testing.T) *sql.Connection {
	t.Helper()

	dbPath := path.Join(os.TempDir(), uuid.New().String())
	t.Cleanup(func() { _ = os.Remove(dbPath) })

	conn, err := sql.NewConnection(&config.SQL{
		PluginName:   sqliteplugin.PluginName,
		DatabaseName: dbPath,
	})
	require.NoError(t, err)
	return conn
}

// listDatabaseSchemaFilePaths returns a list of database schema file paths
func listDatabaseNames(t *testing.T) []string {
	t.Helper()

	dirs, err := sqlite.SchemaFS.ReadDir(".")
	require.NoError(t, err)

	var databaseNames = make([]string, len(dirs))
	for i, dir := range dirs {
		databaseNames[i] = dir.Name()
	}

	return databaseNames
}
