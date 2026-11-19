package cassandra

import (
	"context"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

// InsertAsyncWorkflowDLQMessage inserts or overwrites a DLQ row without an LWT: the key is the
// source message's key, so a retried write replaces the row instead of duplicating it.
func (db *CDB) InsertAsyncWorkflowDLQMessage(ctx context.Context, row *nosqlplugin.AsyncWorkflowDLQMessageRow) error {
	return db.session.Query(templateInsertAsyncWorkflowDLQMessageQuery,
		row.ShardID,
		row.SourceCluster,
		row.MessageID,
		row.Payload,
		row.PayloadEncoding,
		row.DomainName,
		row.WorkflowID,
		row.RequestID,
		row.RequestType,
		row.Reason,
		row.CreatedTime,
	).WithContext(ctx).Exec()
}

// SelectAsyncWorkflowDLQMessages returns up to pageSize DLQ rows after the exclusive lower bound,
// in clustering (message_id ascending) order.
func (db *CDB) SelectAsyncWorkflowDLQMessages(
	ctx context.Context,
	shardID int,
	sourceCluster string,
	exclusiveMinMessageID int64,
	pageSize int,
) ([]*nosqlplugin.AsyncWorkflowDLQMessageRow, error) {
	query := db.session.Query(templateSelectAsyncWorkflowDLQMessagesQuery,
		shardID,
		sourceCluster,
		exclusiveMinMessageID,
	).PageSize(pageSize).WithContext(ctx)

	iter := query.Iter()
	if iter == nil {
		return nil, &types.InternalServiceError{
			Message: "SelectAsyncWorkflowDLQMessages operation failed. Not able to create query iterator.",
		}
	}

	var rows []*nosqlplugin.AsyncWorkflowDLQMessageRow
	for len(rows) < pageSize {
		row := &nosqlplugin.AsyncWorkflowDLQMessageRow{
			AsyncWorkflowMessageRow: nosqlplugin.AsyncWorkflowMessageRow{
				ShardID:       shardID,
				SourceCluster: sourceCluster,
			},
		}
		if !iter.Scan(
			&row.MessageID,
			&row.Payload,
			&row.PayloadEncoding,
			&row.DomainName,
			&row.WorkflowID,
			&row.RequestID,
			&row.RequestType,
			&row.Reason,
			&row.CreatedTime,
		) {
			break
		}
		rows = append(rows, row)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}
	return rows, nil
}

// RangeDeleteAsyncWorkflowDLQMessages deletes DLQ rows through the inclusive bound for one
// (shard, source cluster). Like the other queue tables this range delete is deliberately unfenced.
func (db *CDB) RangeDeleteAsyncWorkflowDLQMessages(ctx context.Context, shardID int, sourceCluster string, inclusiveMaxMessageID int64) error {
	query := db.session.Query(templateRangeDeleteAsyncWorkflowDLQMessagesQuery,
		shardID,
		sourceCluster,
		inclusiveMaxMessageID,
	).WithContext(ctx)
	return db.executeWithConsistencyAll(query)
}
