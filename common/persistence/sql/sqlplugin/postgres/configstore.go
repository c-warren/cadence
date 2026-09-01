package postgres

import (
	"context"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	_selectLatestConfigQuery = "SELECT row_type, version, timestamp, data, data_encoding FROM cluster_config WHERE row_type = $1 ORDER BY version LIMIT 1;"

	_insertConfigQuery = "INSERT INTO cluster_config (row_type, version, timestamp, data, data_encoding) VALUES($1, $2, $3, $4, $5)"
)

func (pdb *db) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	_, err := pdb.driver.ExecContext(ctx, sqlplugin.DbDefaultShard, _insertConfigQuery, row.RowType, -1*row.Version, pdb.converter.ToPostgresDateTime(row.Timestamp), row.Values.Data, row.Values.Encoding)
	return err
}

func (pdb *db) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	var row sqlplugin.ClusterConfigRow
	err := pdb.driver.GetContext(ctx, sqlplugin.DbDefaultShard, &row, _selectLatestConfigQuery, rowType)
	if err != nil {
		return nil, err
	}
	row.Version *= -1
	return &persistence.InternalConfigStoreEntry{
		RowType:   row.RowType,
		Version:   row.Version,
		Timestamp: pdb.converter.FromPostgresDateTime(row.Timestamp),
		Values: &persistence.DataBlob{
			Data:     row.Data,
			Encoding: constants.EncodingType(row.DataEncoding),
		},
	}, nil
}
