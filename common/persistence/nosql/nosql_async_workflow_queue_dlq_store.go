package nosql

import (
	"context"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

// EnqueueAsyncWorkflowMessageToDLQ writes the message to async_workflow_queue_dlq under the source
// message's key. A retry overwrites the row.
func (sh *nosqlAsyncWorkflowQueueStore) EnqueueAsyncWorkflowMessageToDLQ(ctx context.Context, request *persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest) error {
	storeShard, err := sh.GetStoreShardByHistoryShard(request.Message.ShardID)
	if err != nil {
		return err
	}
	if err := storeShard.db.InsertAsyncWorkflowDLQMessage(ctx, asyncWorkflowDLQRowFromRequest(request)); err != nil {
		return convertCommonErrors(storeShard.db, "EnqueueAsyncWorkflowMessageToDLQ", err)
	}
	return nil
}

func (sh *nosqlAsyncWorkflowQueueStore) ReadAsyncWorkflowMessagesFromDLQ(ctx context.Context, request *persistence.ReadAsyncWorkflowMessagesFromDLQRequest) (*persistence.ReadAsyncWorkflowMessagesFromDLQResponse, error) {
	storeShard, err := sh.GetStoreShardByHistoryShard(request.ShardID)
	if err != nil {
		return nil, err
	}
	rows, err := storeShard.db.SelectAsyncWorkflowDLQMessages(ctx, request.ShardID, request.SourceCluster, request.ExclusiveMinMessageID, request.PageSize)
	if err != nil {
		return nil, convertCommonErrors(storeShard.db, "ReadAsyncWorkflowMessagesFromDLQ", err)
	}
	messages := make([]*persistence.AsyncWorkflowDLQMessage, 0, len(rows))
	for _, row := range rows {
		messages = append(messages, asyncWorkflowDLQMessageFromRow(row))
	}
	return &persistence.ReadAsyncWorkflowMessagesFromDLQResponse{Messages: messages}, nil
}

func (sh *nosqlAsyncWorkflowQueueStore) RangeDeleteAsyncWorkflowMessagesFromDLQ(ctx context.Context, request *persistence.RangeDeleteAsyncWorkflowMessagesFromDLQRequest) error {
	storeShard, err := sh.GetStoreShardByHistoryShard(request.ShardID)
	if err != nil {
		return err
	}
	if err := storeShard.db.RangeDeleteAsyncWorkflowDLQMessages(ctx, request.ShardID, request.SourceCluster, request.InclusiveMaxMessageID); err != nil {
		return convertCommonErrors(storeShard.db, "RangeDeleteAsyncWorkflowMessagesFromDLQ", err)
	}
	return nil
}

// asyncWorkflowDLQRowFromRequest builds the DLQ row. CreatedTime is the DLQ write time
// (request.CurrentTimeStamp), not the source message's CreatedTime.
func asyncWorkflowDLQRowFromRequest(request *persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest) *nosqlplugin.AsyncWorkflowDLQMessageRow {
	message := request.Message
	return &nosqlplugin.AsyncWorkflowDLQMessageRow{
		AsyncWorkflowMessageRow: nosqlplugin.AsyncWorkflowMessageRow{
			ShardID:         message.ShardID,
			SourceCluster:   message.SourceCluster,
			MessageID:       message.MessageID,
			DomainName:      message.DomainName,
			WorkflowID:      message.WorkflowID,
			RequestID:       message.RequestID,
			RequestType:     int(message.RequestType),
			Payload:         message.Payload,
			PayloadEncoding: string(message.PayloadEncoding),
			CreatedTime:     request.CurrentTimeStamp,
		},
		Reason: request.Reason,
	}
}

func asyncWorkflowDLQMessageFromRow(row *nosqlplugin.AsyncWorkflowDLQMessageRow) *persistence.AsyncWorkflowDLQMessage {
	return &persistence.AsyncWorkflowDLQMessage{
		AsyncWorkflowMessage: *asyncWorkflowMessageFromRow(&row.AsyncWorkflowMessageRow),
		Reason:               row.Reason,
	}
}
