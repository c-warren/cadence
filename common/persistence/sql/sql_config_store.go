package sql

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

type (
	sqlConfigStore struct {
		sqlStore
	}
)

// NewSQLConfigStore creates a config store for SQL
func NewSQLConfigStore(
	db sqlplugin.DB,
	logger log.Logger,
	parser serialization.Parser,
) (persistence.ConfigStore, error) {
	return &sqlConfigStore{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
			parser: parser,
		},
	}, nil
}

func (m *sqlConfigStore) FetchConfig(ctx context.Context, configType persistence.ConfigType) (*persistence.InternalConfigStoreEntry, error) {
	entry, err := m.db.SelectLatestConfig(ctx, int(configType))
	if m.db.IsNotFoundError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, convertCommonErrors(m.db, "FetchConfig", "", err)
	}
	return entry, nil
}

func (m *sqlConfigStore) UpdateConfig(ctx context.Context, value *persistence.InternalConfigStoreEntry) error {
	err := m.db.InsertConfig(ctx, value)
	if err != nil {
		if m.db.IsDupEntryError(err) {
			return &persistence.ConditionFailedError{Msg: fmt.Sprintf("Version %v already exists. Condition Failed", value.Version)}
		}
		return convertCommonErrors(m.db, "UpdateConfig", "", err)
	}
	return nil
}
