package cassandra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
)

const asyncWorkflowDLQTestTimeout = 30 * time.Second

func TestCassandraAsyncWorkflowQueueDLQPersistence(t *testing.T) {
	base := CassandraTestBase(t)
	t.Cleanup(base.TearDownWorkflowStore)

	manager, err := base.PersistenceFactory.NewAsyncWorkflowQueueManager()
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	ctx, cancel := context.WithTimeout(context.Background(), asyncWorkflowDLQTestTimeout)
	t.Cleanup(cancel)

	const shardID = 7001
	const otherShardID = 7002
	sourceCluster := "cluster-" + uuid.New()
	otherSourceCluster := "cluster-" + uuid.New()
	before := time.Now().UTC().Add(-time.Minute)

	t.Run("write overwrite and page", func(t *testing.T) {
		for messageID := int64(20); messageID <= 22; messageID++ {
			require.NoError(t, manager.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{
				Message: asyncWorkflowDLQTestMessage(shardID, sourceCluster, messageID),
				Reason:  fmt.Sprintf("reason-%d", messageID),
			}))
		}
		// Same key again: the row is replaced, not duplicated.
		require.NoError(t, manager.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{
			Message: asyncWorkflowDLQTestMessage(shardID, sourceCluster, 21),
			Reason:  "replacement-reason",
		}))

		first, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, PageSize: 2,
		})
		require.NoError(t, err)
		require.Equal(t, []int64{20, 21}, asyncWorkflowDLQMessageIDs(first.Messages))
		assert.Equal(t, "reason-20", first.Messages[0].Reason)
		assert.Equal(t, "replacement-reason", first.Messages[1].Reason)

		second, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, ExclusiveMinMessageID: 21, PageSize: 2,
		})
		require.NoError(t, err)
		require.Equal(t, []int64{22}, asyncWorkflowDLQMessageIDs(second.Messages))

		// Every descriptive column round-trips; CreatedTime is stamped by the manager at write time.
		got := second.Messages[0]
		want := asyncWorkflowDLQTestMessage(shardID, sourceCluster, 22)
		assert.Equal(t, want.ShardID, got.ShardID)
		assert.Equal(t, want.SourceCluster, got.SourceCluster)
		assert.Equal(t, want.MessageID, got.MessageID)
		assert.Equal(t, want.DomainName, got.DomainName)
		assert.Equal(t, want.WorkflowID, got.WorkflowID)
		assert.Equal(t, want.RequestID, got.RequestID)
		assert.Equal(t, want.RequestType, got.RequestType)
		assert.Equal(t, want.Payload, got.Payload)
		assert.Equal(t, want.PayloadEncoding, got.PayloadEncoding)
		assert.Equal(t, "reason-22", got.Reason)
		assert.True(t, got.CreatedTime.After(before), "CreatedTime %v should be after %v", got.CreatedTime, before)
		assert.Equal(t, time.UTC, got.CreatedTime.Location())
	})

	t.Run("other source cluster and other shard are isolated", func(t *testing.T) {
		require.NoError(t, manager.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{
			Message: asyncWorkflowDLQTestMessage(shardID, otherSourceCluster, 5),
			Reason:  "other-source",
		}))

		otherSource, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: otherSourceCluster, PageSize: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{5}, asyncWorkflowDLQMessageIDs(otherSource.Messages))

		otherShard, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: otherShardID, SourceCluster: sourceCluster, PageSize: 10,
		})
		require.NoError(t, err)
		assert.Empty(t, otherShard.Messages)

		same, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, PageSize: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{20, 21, 22}, asyncWorkflowDLQMessageIDs(same.Messages))
	})

	t.Run("range delete is inclusive and scoped to one source cluster", func(t *testing.T) {
		require.NoError(t, manager.RangeDeleteAsyncWorkflowMessagesFromDLQ(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, InclusiveMaxMessageID: 21,
		}))

		remaining, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, PageSize: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{22}, asyncWorkflowDLQMessageIDs(remaining.Messages))

		otherSource, err := manager.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: otherSourceCluster, PageSize: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{5}, asyncWorkflowDLQMessageIDs(otherSource.Messages))

		// Deleting an already-empty range is a no-op, not an error.
		require.NoError(t, manager.RangeDeleteAsyncWorkflowMessagesFromDLQ(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesFromDLQRequest{
			ShardID: shardID, SourceCluster: sourceCluster, InclusiveMaxMessageID: 21,
		}))
	})
}

func asyncWorkflowDLQTestMessage(shardID int, sourceCluster string, messageID int64) *persistence.AsyncWorkflowMessage {
	return &persistence.AsyncWorkflowMessage{
		ShardID:         shardID,
		SourceCluster:   sourceCluster,
		MessageID:       messageID,
		DomainName:      "dlq-domain",
		WorkflowID:      fmt.Sprintf("workflow-%d", messageID),
		RequestID:       fmt.Sprintf("request-%d", messageID),
		RequestType:     persistence.AsyncWorkflowRequestType(messageID % 2),
		Payload:         []byte{byte(messageID), 1, 2},
		PayloadEncoding: constants.EncodingTypeThriftRW,
	}
}

func asyncWorkflowDLQMessageIDs(messages []*persistence.AsyncWorkflowDLQMessage) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.MessageID)
	}
	return ids
}
