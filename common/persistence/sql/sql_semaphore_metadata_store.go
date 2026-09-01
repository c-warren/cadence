package sql

import (
	"context"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/types"
)

// sqlSemaphoreMetadataStore is a stub. The distributed semaphore feature is only
// supported on the NoSQL (Cassandra) persistence backend. These methods exist to
// satisfy the SemaphoreMetadataStore interface and always return a not-supported error.
type sqlSemaphoreMetadataStore struct {
	sqlStore
}

// newSQLSemaphoreMetadataStore creates an instance of sqlSemaphoreMetadataStore
func newSQLSemaphoreMetadataStore(
	db sqlplugin.DB,
	logger log.Logger,
	parser serialization.Parser,
) (persistence.SemaphoreMetadataStore, error) {
	return &sqlSemaphoreMetadataStore{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
			parser: parser,
		},
	}, nil
}

func errSemaphoreNotSupportedOnSQL() error {
	return &types.BadRequestError{
		Message: "distributed semaphore is not supported on the SQL persistence backend",
	}
}

func (m *sqlSemaphoreMetadataStore) CreateSemaphore(
	ctx context.Context,
	semaphore *persistence.SemaphoreMetadata,
) error {
	return errSemaphoreNotSupportedOnSQL()
}

func (m *sqlSemaphoreMetadataStore) GetSemaphore(
	ctx context.Context,
	request *persistence.GetSemaphoreRequest,
) (*persistence.SemaphoreMetadata, error) {
	return nil, errSemaphoreNotSupportedOnSQL()
}

func (m *sqlSemaphoreMetadataStore) ListSemaphores(
	ctx context.Context,
	request *persistence.ListSemaphoresRequest,
) (*persistence.ListSemaphoresResponse, error) {
	return nil, errSemaphoreNotSupportedOnSQL()
}
