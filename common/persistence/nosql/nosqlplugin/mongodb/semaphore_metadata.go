package mongodb

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *mdb) InsertSemaphoreMetadata(ctx context.Context, row *nosqlplugin.SemaphoreMetadataRow) error {
	return fmt.Errorf("InsertSemaphoreMetadata is not implemented")
}

func (db *mdb) SelectSemaphoreMetadata(ctx context.Context, domainID, semaphoreName string) (*nosqlplugin.SemaphoreMetadataRow, error) {
	return nil, fmt.Errorf("SelectSemaphoreMetadata is not implemented")
}

func (db *mdb) SelectSemaphoreMetadataByDomain(ctx context.Context, filter *nosqlplugin.SemaphoreMetadataFilter) ([]*nosqlplugin.SemaphoreMetadataRow, []byte, error) {
	return nil, nil, fmt.Errorf("SelectSemaphoreMetadataByDomain is not implemented")
}
