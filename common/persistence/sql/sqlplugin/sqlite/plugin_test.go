package sqlite

import (
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/config"

	_ "github.com/ncruces/go-sqlite3/driver" // register sqlite3 driver for tests
	_ "github.com/ncruces/go-sqlite3/embed"  // embed sqlite db for tests
)

func TestPlugin_CreateDB(t *testing.T) {
	for name, cfg := range map[string]*config.SQL{
		"in-memory": {},
		"temp file": {DatabaseName: path.Join(os.TempDir(), uuid.New().String())},
	} {
		t.Run(name, func(t *testing.T) {
			p := &plugin{}
			db, err := p.CreateDB(cfg)

			assert.NoError(t, err)
			assert.NotNil(t, db)
		})
	}
}

func TestPlugin_CreateAdminDB(t *testing.T) {
	for name, cfg := range map[string]*config.SQL{
		"in-memory": {},
		"temp file": {DatabaseName: path.Join(os.TempDir(), uuid.New().String())},
	} {
		t.Run(name, func(t *testing.T) {
			p := &plugin{}
			db, err := p.CreateAdminDB(cfg)

			assert.NoError(t, err)
			assert.NotNil(t, db)
		})
	}
}
