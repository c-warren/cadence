package dynamodb

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertShard creates a new shard, return error is there is any.
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *ddb) InsertShard(ctx context.Context, row *nosqlplugin.ShardRow) error {
	panic("TODO")
}

// SelectShard gets a shard
func (db *ddb) SelectShard(ctx context.Context, shardID int, currentClusterName string) (int64, *nosqlplugin.ShardRow, error) {
	panic("TODO")
}

// UpdateRangeID updates the rangeID, return error is there is any
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *ddb) UpdateRangeID(ctx context.Context, shardID int, rangeID int64, previousRangeID int64) error {
	panic("TODO")
}

// UpdateShard updates a shard, return error is there is any.
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *ddb) UpdateShard(ctx context.Context, row *nosqlplugin.ShardRow, previousRangeID int64) error {
	panic("TODO")
}
