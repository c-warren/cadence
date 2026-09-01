package nosql

import (
	"fmt"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
)

type (
	// Factory vends datastore implementations backed by cassandra
	Factory struct {
		cfg            config.ShardedNoSQL
		clusterName    string
		logger         log.Logger
		metricsClient  metrics.Client
		dc             *persistence.DynamicConfiguration
		parser         serialization.Parser
		taskSerializer serialization.TaskSerializer
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// datastores that are backed by cassandra
func NewFactory(cfg config.ShardedNoSQL, clusterName string, logger log.Logger, metricsClient metrics.Client, taskSerializer serialization.TaskSerializer, parser serialization.Parser, dc *persistence.DynamicConfiguration) *Factory {
	return &Factory{
		cfg:            cfg,
		clusterName:    clusterName,
		logger:         logger,
		metricsClient:  metricsClient,
		taskSerializer: taskSerializer,
		dc:             dc,
		parser:         parser,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (persistence.TaskStore, error) {
	return newNoSQLTaskStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (persistence.ShardStore, error) {
	return newNoSQLShardStore(f.cfg, f.clusterName, f.logger, f.metricsClient, f.dc, f.parser)
}

// NewHistoryStore returns a new history store
func (f *Factory) NewHistoryStore() (persistence.HistoryStore, error) {
	return newNoSQLHistoryStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewDomainStore returns a metadata store that understands only v2
func (f *Factory) NewDomainStore() (persistence.DomainStore, error) {
	return newNoSQLDomainStore(f.cfg, f.clusterName, f.logger, f.metricsClient, f.dc)
}

// NewDomainAuditStore returns a domain audit store
func (f *Factory) NewDomainAuditStore() (persistence.DomainAuditStore, error) {
	return newNoSQLDomainAuditStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewSemaphoreMetadataStore returns a semaphore metadata store
func (f *Factory) NewSemaphoreMetadataStore() (persistence.SemaphoreMetadataStore, error) {
	return newNoSQLSemaphoreMetadataStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewHistoryDLQTaskStore returns a history DLQ task store
func (f *Factory) NewHistoryDLQTaskStore() (persistence.HistoryDLQTaskStore, error) {
	return newNoSQLHistoryDLQTaskStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewExecutionStore returns an ExecutionStore
func (f *Factory) NewExecutionStore() (persistence.ExecutionStore, error) {
	return newNoSQLExecutionStore(f.cfg, f.logger, f.metricsClient, f.taskSerializer, f.dc)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore(sortByCloseTime bool) (persistence.VisibilityStore, error) {
	return newNoSQLVisibilityStore(sortByCloseTime, f.cfg, f.logger, f.metricsClient, f.dc)
}

// NewQueue returns a new queue backed by cassandra
func (f *Factory) NewQueue(queueType persistence.QueueType) (persistence.QueueStore, error) {
	return newNoSQLQueueStore(f.cfg, f.logger, f.metricsClient, queueType, f.dc)
}

// NewConfigStore returns a new config store
func (f *Factory) NewConfigStore() (persistence.ConfigStore, error) {
	return NewNoSQLConfigStore(f.cfg, f.logger, f.metricsClient, f.dc)
}

func (f *Factory) NewAdminDBs(dbType persistence.DBType) ([]persistence.AdminDB, error) {
	var result []persistence.AdminDB
	for connectionID, conn := range f.cfg.Connections {
		plugin, ok := supportedPlugins[conn.NoSQLPlugin.PluginName]
		if !ok {
			return nil, fmt.Errorf("unsupported plugin: %v", conn.NoSQLPlugin.PluginName)
		}
		result = append(result, &nosqlAdmin{
			logger:     f.logger,
			plugin:     plugin,
			dbType:     dbType,
			identifier: connectionID,
			cfg:        conn.NoSQLPlugin,
		})
	}
	return result, nil
}

// Close closes the factory. Store Close methods own connection lifecycle
// (matching HistoryStore), so this is intentionally a no-op.
func (f *Factory) Close() {}
