package mongodb

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *mdb) InsertAsyncWorkflowDLQMessage(ctx context.Context, row *nosqlplugin.AsyncWorkflowDLQMessageRow) error {
	return fmt.Errorf("InsertAsyncWorkflowDLQMessage not implemented for MongoDB")
}

func (db *mdb) SelectAsyncWorkflowDLQMessages(ctx context.Context, shardID int, sourceCluster string, exclusiveMinMessageID int64, pageSize int) ([]*nosqlplugin.AsyncWorkflowDLQMessageRow, error) {
	return nil, fmt.Errorf("SelectAsyncWorkflowDLQMessages not implemented for MongoDB")
}

func (db *mdb) RangeDeleteAsyncWorkflowDLQMessages(ctx context.Context, shardID int, sourceCluster string, inclusiveMaxMessageID int64) error {
	return fmt.Errorf("RangeDeleteAsyncWorkflowDLQMessages not implemented for MongoDB")
}
