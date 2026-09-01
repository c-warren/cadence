package sql

import (
	"fmt"
	"sort"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

var supportedPlugins = map[string]sqlplugin.Plugin{}

// RegisterPlugin will register a SQL plugin
func RegisterPlugin(pluginName string, plugin sqlplugin.Plugin) {
	if _, ok := supportedPlugins[pluginName]; ok {
		panic("plugin " + pluginName + " already registered")
	}
	supportedPlugins[pluginName] = plugin
}

// RegisterPluginIfNotExists will register a SQL plugin only if a plugin with same name has not already been registered
func RegisterPluginIfNotExists(pluginName string, plugin sqlplugin.Plugin) {
	if _, ok := supportedPlugins[pluginName]; !ok {
		supportedPlugins[pluginName] = plugin
	}
}

// PluginRegistered returns true if plugin with given name has been registered, false otherwise
func PluginRegistered(pluginName string) bool {
	_, ok := supportedPlugins[pluginName]
	return ok
}

// GetRegisteredPluginNames returns the list of registered plugin names
func GetRegisteredPluginNames() []string {
	var plugins []string
	for k := range supportedPlugins {
		plugins = append(plugins, k)
	}
	sort.Strings(plugins)
	return plugins
}

// NewSQLDB creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func NewSQLDB(cfg *config.SQL) (sqlplugin.DB, error) {
	plugin, ok := supportedPlugins[cfg.PluginName]

	if !ok {
		return nil, fmt.Errorf("not supported plugin %v, only supported: %v", cfg.PluginName, supportedPlugins)
	}

	return plugin.CreateDB(cfg)
}

// NewSQLAdminDB returns a AdminDB
func NewSQLAdminDB(cfg *config.SQL) (sqlplugin.AdminDB, error) {
	plugin, ok := supportedPlugins[cfg.PluginName]

	if !ok {
		return nil, fmt.Errorf("not supported plugin %v, only supported: %v", cfg.PluginName, supportedPlugins)
	}

	return plugin.CreateAdminDB(cfg)
}
