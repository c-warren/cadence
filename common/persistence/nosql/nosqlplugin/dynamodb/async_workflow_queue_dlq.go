package dynamodb

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func (db *ddb) InsertAsyncWorkflowDLQMessage(ctx context.Context, row *nosqlplugin.AsyncWorkflowDLQMessageRow) error {
	return fmt.Errorf("InsertAsyncWorkflowDLQMessage not implemented for DynamoDB")
}

func (db *ddb) SelectAsyncWorkflowDLQMessages(ctx context.Context, shardID int, sourceCluster string, exclusiveMinMessageID int64, pageSize int) ([]*nosqlplugin.AsyncWorkflowDLQMessageRow, error) {
	return nil, fmt.Errorf("SelectAsyncWorkflowDLQMessages not implemented for DynamoDB")
}

func (db *ddb) RangeDeleteAsyncWorkflowDLQMessages(ctx context.Context, shardID int, sourceCluster string, inclusiveMaxMessageID int64) error {
	return fmt.Errorf("RangeDeleteAsyncWorkflowDLQMessages not implemented for DynamoDB")
}
