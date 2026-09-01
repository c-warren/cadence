package dynamodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *ddb) InsertSemaphoreMetadata(ctx context.Context, row *nosqlplugin.SemaphoreMetadataRow) error {
	panic("TODO: InsertSemaphoreMetadata is not implemented")
}

func (db *ddb) SelectSemaphoreMetadata(ctx context.Context, domainID, semaphoreName string) (*nosqlplugin.SemaphoreMetadataRow, error) {
	panic("TODO: SelectSemaphoreMetadata is not implemented")
}

func (db *ddb) SelectSemaphoreMetadataByDomain(ctx context.Context, filter *nosqlplugin.SemaphoreMetadataFilter) ([]*nosqlplugin.SemaphoreMetadataRow, []byte, error) {
	panic("TODO: SelectSemaphoreMetadataByDomain is not implemented")
}
