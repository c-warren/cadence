package nosql

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

type nosqlConfigStore struct {
	nosqlStore
}

func NewNoSQLConfigStore(
	cfg config.ShardedNoSQL,
	logger log.Logger,
	metricsClient metrics.Client,
	dc *persistence.DynamicConfiguration,
) (persistence.ConfigStore, error) {
	shardedStore, err := newShardedNosqlStore(cfg, logger, metricsClient, dc, false)
	if err != nil {
		return nil, err
	}
	return &nosqlConfigStore{
		nosqlStore: shardedStore.GetDefaultShard(),
	}, nil
}

func (m *nosqlConfigStore) FetchConfig(ctx context.Context, configType persistence.ConfigType) (*persistence.InternalConfigStoreEntry, error) {
	entry, err := m.db.SelectLatestConfig(ctx, int(configType))
	if err != nil {
		if m.db.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, convertCommonErrors(m.db, "FetchConfig", err)
	}
	return entry, nil
}

func (m *nosqlConfigStore) UpdateConfig(ctx context.Context, value *persistence.InternalConfigStoreEntry) error {
	err := m.db.InsertConfig(ctx, value)
	if err != nil {
		if _, ok := err.(*nosqlplugin.ConditionFailure); ok {
			return &persistence.ConditionFailedError{Msg: fmt.Sprintf("Version %v already exists. Condition Failed", value.Version)}
		}
		return convertCommonErrors(m.db, "UpdateConfig", err)
	}
	return nil
}
