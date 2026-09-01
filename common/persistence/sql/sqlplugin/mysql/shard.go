package mysql

import (
	"context"
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	createShardQry = `INSERT INTO
 shards (shard_id, range_id, data, data_encoding) VALUES (?, ?, ?, ?)`

	getShardQry = `SELECT
 shard_id, range_id, data, data_encoding
 FROM shards WHERE shard_id = ?`

	updateShardQry = `UPDATE shards 
 SET range_id = ?, data = ?, data_encoding = ? 
 WHERE shard_id = ?`

	lockShardQry     = `SELECT range_id FROM shards WHERE shard_id = ? FOR UPDATE`
	readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = ? LOCK IN SHARE MODE`
)

// InsertIntoShards inserts one or more rows into shards table
func (mdb *DB) InsertIntoShards(ctx context.Context, row *sqlplugin.ShardsRow) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(int(row.ShardID), mdb.GetTotalNumDBShards())
	return mdb.driver.ExecContext(ctx, dbShardID, createShardQry, row.ShardID, row.RangeID, row.Data, row.DataEncoding)
}

// UpdateShards updates one or more rows into shards table
func (mdb *DB) UpdateShards(ctx context.Context, row *sqlplugin.ShardsRow) (sql.Result, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(int(row.ShardID), mdb.GetTotalNumDBShards())
	return mdb.driver.ExecContext(ctx, dbShardID, updateShardQry, row.RangeID, row.Data, row.DataEncoding, row.ShardID)
}

// SelectFromShards reads one or more rows from shards table
func (mdb *DB) SelectFromShards(ctx context.Context, filter *sqlplugin.ShardsFilter) (*sqlplugin.ShardsRow, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(int(filter.ShardID), mdb.GetTotalNumDBShards())
	var row sqlplugin.ShardsRow
	err := mdb.driver.GetContext(ctx, dbShardID, &row, getShardQry, filter.ShardID)
	if err != nil {
		return nil, err
	}
	return &row, err
}

// ReadLockShards acquires a read lock on a single row in shards table
func (mdb *DB) ReadLockShards(ctx context.Context, filter *sqlplugin.ShardsFilter) (int, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(int(filter.ShardID), mdb.GetTotalNumDBShards())
	var rangeID int
	err := mdb.driver.GetContext(ctx, dbShardID, &rangeID, readLockShardQry, filter.ShardID)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
func (mdb *DB) WriteLockShards(ctx context.Context, filter *sqlplugin.ShardsFilter) (int, error) {
	dbShardID := sqlplugin.GetDBShardIDFromHistoryShardID(int(filter.ShardID), mdb.GetTotalNumDBShards())
	var rangeID int
	err := mdb.driver.GetContext(ctx, dbShardID, &rangeID, lockShardQry, filter.ShardID)
	return rangeID, err
}
