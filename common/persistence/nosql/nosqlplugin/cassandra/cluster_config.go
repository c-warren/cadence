package cassandra

import (
	"context"
	"time"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *CDB) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	query := db.session.Query(templateInsertConfig, row.RowType, row.Version, row.Timestamp, row.Values.Data, row.Values.Encoding).WithContext(ctx)
	applied, err := query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return err
	}
	if !applied {
		return nosqlplugin.NewConditionFailure("InsertConfig operation failed because of version collision")
	}
	return nil
}

func (db *CDB) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	var version int64
	var timestamp time.Time
	var data []byte
	var encoding constants.EncodingType

	query := db.session.Query(templateSelectLatestConfig, rowType).WithContext(ctx)
	err := query.Scan(&rowType, &version, &timestamp, &data, &encoding)
	if err != nil {
		return nil, err
	}

	return &persistence.InternalConfigStoreEntry{
		RowType:   rowType,
		Version:   version,
		Timestamp: timestamp,
		Values: &persistence.DataBlob{
			Data:     data,
			Encoding: encoding,
		},
	}, err
}
