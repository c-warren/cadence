package nosql

import (
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// a shared struct for all stores in this package
type nosqlStore struct {
	logger log.Logger
	db     nosqlplugin.DB
	dc     *persistence.DynamicConfiguration
}

func (nm *nosqlStore) GetName() string {
	return nm.db.PluginName()
}

// Close releases the underlying resources held by this object
func (nm *nosqlStore) Close() {
	nm.db.Close()
}
