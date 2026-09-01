package nosql

import (
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/persistence/serialization"
)

const testExecutionShardID = 1

// fakeShardedNosqlStore is a test double that always returns the same nosqlStore.
type fakeShardedNosqlStore struct {
	store  nosqlStore
	logger log.Logger
}

func (f *fakeShardedNosqlStore) GetStoreShardByHistoryShard(shardID int) (*nosqlStore, error) {
	return &f.store, nil
}

func (f *fakeShardedNosqlStore) GetStoreShardByTaskList(domainID string, taskListName string, taskType int) (*nosqlStore, error) {
	return &f.store, nil
}

func (f *fakeShardedNosqlStore) GetDefaultShard() nosqlStore {
	return f.store
}

func (f *fakeShardedNosqlStore) Close() {}

func (f *fakeShardedNosqlStore) GetName() string {
	if f.store.db != nil {
		return f.store.db.PluginName()
	}
	return "fake"
}

func (f *fakeShardedNosqlStore) GetShardingPolicy() shardingPolicy {
	return shardingPolicy{}
}

func (f *fakeShardedNosqlStore) GetLogger() log.Logger {
	if f.logger != nil {
		return f.logger
	}
	return f.store.logger
}

func (f *fakeShardedNosqlStore) GetMetricsClient() metrics.Client {
	return metrics.NewNoopMetricsClient()
}

func newTestNosqlExecutionStoreWithOptions(
	db nosqlplugin.DB,
	logger log.Logger,
	taskSerializer serialization.TaskSerializer,
	dc *persistence.DynamicConfiguration,
) *nosqlExecutionStore {
	if dc == nil {
		dc = &persistence.DynamicConfiguration{}
	}
	store := nosqlStore{logger: logger, db: db, dc: dc}
	return &nosqlExecutionStore{
		shardedNosqlStore: &fakeShardedNosqlStore{store: store, logger: logger},
		taskSerializer:    taskSerializer,
	}
}
