package mysql

import (
	"context"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

func (mdb *DB) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	_, err := mdb.driver.ExecContext(ctx, sqlplugin.DbDefaultShard, _insertConfigQuery, row.RowType, -1*row.Version, mdb.converter.ToDateTime(row.Timestamp), row.Values.Data, row.Values.Encoding)
	return err
}

func (mdb *DB) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	var row sqlplugin.ClusterConfigRow
	err := mdb.driver.GetContext(ctx, sqlplugin.DbDefaultShard, &row, _selectLatestConfigQuery, rowType)
	if err != nil {
		return nil, err
	}
	row.Version *= -1
	return &persistence.InternalConfigStoreEntry{
		RowType:   row.RowType,
		Version:   row.Version,
		Timestamp: mdb.converter.FromDateTime(row.Timestamp),
		Values: &persistence.DataBlob{
			Data:     row.Data,
			Encoding: constants.EncodingType(row.DataEncoding),
		},
	}, nil
}
