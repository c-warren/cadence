package mongodb

import (
	"context"
	"log"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// InsertShard creates a new shard, return error is there is any.
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *mdb) InsertShard(ctx context.Context, row *nosqlplugin.ShardRow) error {
	log.Println("not implemented...ignore the error for testing...")
	return nil
}

// SelectShard gets a shard
func (db *mdb) SelectShard(ctx context.Context, shardID int, currentClusterName string) (int64, *nosqlplugin.ShardRow, error) {
	panic("TODO")
}

// UpdateRangeID updates the rangeID, return error is there is any
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *mdb) UpdateRangeID(ctx context.Context, shardID int, rangeID int64, previousRangeID int64) error {
	panic("TODO")
}

// UpdateShard updates a shard, return error is there is any.
// Return ShardOperationConditionFailure if the condition doesn't meet
func (db *mdb) UpdateShard(ctx context.Context, row *nosqlplugin.ShardRow, previousRangeID int64) error {
	panic("TODO")
}
