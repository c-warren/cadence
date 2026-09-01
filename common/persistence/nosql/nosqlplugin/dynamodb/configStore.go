package dynamodb

import (
	"context"
	"errors"

	"github.com/uber/cadence/common/persistence"
)

func (db *ddb) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	return errors.New("TODO")
}

func (db *ddb) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	return nil, errors.New("TODO")
}
