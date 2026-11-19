package nosql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

func newTestAsyncWorkflowQueueDLQStore(t *testing.T) (*nosqlAsyncWorkflowQueueStore, *nosqlplugin.MockDB, *MockshardedNosqlStore, *nosqlStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	db := nosqlplugin.NewMockDB(ctrl)
	sharded := NewMockshardedNosqlStore(ctrl)
	storeShard := &nosqlStore{db: db, logger: testlogger.New(t)}
	return &nosqlAsyncWorkflowQueueStore{shardedNosqlStore: sharded}, db, sharded, storeShard
}

var _ persistence.AsyncWorkflowQueueStore = (*nosqlAsyncWorkflowQueueStore)(nil)

func TestNoSQLAsyncWorkflowQueueDLQStore(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2025, 1, 6, 15, 0, 0, 0, time.UTC)
	sourceCreated := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)

	message := &persistence.AsyncWorkflowMessage{
		ShardID:         3,
		SourceCluster:   "c1",
		MessageID:       11,
		DomainName:      "domain",
		WorkflowID:      "workflow",
		RequestID:       "request",
		RequestType:     persistence.AsyncWorkflowRequestTypeSignalWithStartWorkflow,
		Payload:         []byte("payload"),
		PayloadEncoding: constants.EncodingTypeThriftRW,
		CreatedTime:     sourceCreated,
	}
	row := &nosqlplugin.AsyncWorkflowDLQMessageRow{
		AsyncWorkflowMessageRow: nosqlplugin.AsyncWorkflowMessageRow{
			ShardID:         3,
			SourceCluster:   "c1",
			MessageID:       11,
			DomainName:      "domain",
			WorkflowID:      "workflow",
			RequestID:       "request",
			RequestType:     1,
			Payload:         []byte("payload"),
			PayloadEncoding: "thriftrw",
			CreatedTime:     now,
		},
		Reason: "boom",
	}

	t.Run("enqueue stamps the dlq write time, not the source created time", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().InsertAsyncWorkflowDLQMessage(ctx, row).Return(nil)

		err := store.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest{
			EnqueueAsyncWorkflowMessageToDLQRequest: &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{Message: message, Reason: "boom"},
			CurrentTimeStamp:                        now,
		})
		require.NoError(t, err)
	})

	t.Run("enqueue converts plugin errors", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().InsertAsyncWorkflowDLQMessage(ctx, gomock.Any()).Return(errors.New("plugin failed"))
		db.EXPECT().IsNotFoundError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsTimeoutError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsThrottlingError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsDBUnavailableError(gomock.Any()).Return(false).AnyTimes()

		err := store.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest{
			EnqueueAsyncWorkflowMessageToDLQRequest: &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{Message: message, Reason: "boom"},
			CurrentTimeStamp:                        now,
		})
		var internalErr *types.InternalServiceError
		require.ErrorAs(t, err, &internalErr)
		assert.Contains(t, internalErr.Message, "EnqueueAsyncWorkflowMessageToDLQ")
	})

	t.Run("enqueue returns the shard lookup error", func(t *testing.T) {
		store, _, sharded, _ := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(nil, errors.New("no shard"))

		err := store.EnqueueAsyncWorkflowMessageToDLQ(ctx, &persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest{
			EnqueueAsyncWorkflowMessageToDLQRequest: &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{Message: message, Reason: "boom"},
			CurrentTimeStamp:                        now,
		})
		require.EqualError(t, err, "no shard")
	})

	t.Run("read maps rows", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().SelectAsyncWorkflowDLQMessages(ctx, 3, "c1", int64(10), 5).Return([]*nosqlplugin.AsyncWorkflowDLQMessageRow{row}, nil)

		response, err := store.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: 3, SourceCluster: "c1", ExclusiveMinMessageID: 10, PageSize: 5,
		})
		require.NoError(t, err)
		require.Len(t, response.Messages, 1)
		assert.Equal(t, &persistence.AsyncWorkflowDLQMessage{
			AsyncWorkflowMessage: persistence.AsyncWorkflowMessage{
				ShardID: 3, SourceCluster: "c1", MessageID: 11, DomainName: "domain",
				WorkflowID: "workflow", RequestID: "request", RequestType: persistence.AsyncWorkflowRequestTypeSignalWithStartWorkflow,
				Payload: []byte("payload"), PayloadEncoding: constants.EncodingTypeThriftRW, CreatedTime: now,
			},
			Reason: "boom",
		}, response.Messages[0])
	})

	t.Run("read returns an empty non nil slice", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().SelectAsyncWorkflowDLQMessages(ctx, 3, "c1", int64(0), 5).Return(nil, nil)

		response, err := store.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: 3, SourceCluster: "c1", PageSize: 5,
		})
		require.NoError(t, err)
		require.NotNil(t, response.Messages)
		assert.Empty(t, response.Messages)
	})

	t.Run("read converts plugin errors", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().SelectAsyncWorkflowDLQMessages(ctx, 3, "c1", int64(10), 5).Return(nil, errors.New("plugin failed"))
		db.EXPECT().IsNotFoundError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsTimeoutError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsThrottlingError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsDBUnavailableError(gomock.Any()).Return(false).AnyTimes()

		_, err := store.ReadAsyncWorkflowMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesFromDLQRequest{
			ShardID: 3, SourceCluster: "c1", ExclusiveMinMessageID: 10, PageSize: 5,
		})
		var internalErr *types.InternalServiceError
		require.ErrorAs(t, err, &internalErr)
		assert.Contains(t, internalErr.Message, "ReadAsyncWorkflowMessagesFromDLQ")
	})

	t.Run("range delete passes the inclusive bound", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().RangeDeleteAsyncWorkflowDLQMessages(ctx, 3, "c1", int64(20)).Return(nil)

		err := store.RangeDeleteAsyncWorkflowMessagesFromDLQ(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesFromDLQRequest{
			ShardID: 3, SourceCluster: "c1", InclusiveMaxMessageID: 20,
		})
		require.NoError(t, err)
	})

	t.Run("range delete converts plugin errors", func(t *testing.T) {
		store, db, sharded, storeShard := newTestAsyncWorkflowQueueDLQStore(t)
		sharded.EXPECT().GetStoreShardByHistoryShard(3).Return(storeShard, nil)
		db.EXPECT().RangeDeleteAsyncWorkflowDLQMessages(ctx, 3, "c1", int64(20)).Return(errors.New("plugin failed"))
		db.EXPECT().IsNotFoundError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsTimeoutError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsThrottlingError(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().IsDBUnavailableError(gomock.Any()).Return(false).AnyTimes()

		err := store.RangeDeleteAsyncWorkflowMessagesFromDLQ(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesFromDLQRequest{
			ShardID: 3, SourceCluster: "c1", InclusiveMaxMessageID: 20,
		})
		var internalErr *types.InternalServiceError
		require.ErrorAs(t, err, &internalErr)
		assert.Contains(t, internalErr.Message, "RangeDeleteAsyncWorkflowMessagesFromDLQ")
	})
}

func TestAsyncWorkflowDLQMappersRoundTrip(t *testing.T) {
	now := time.Date(2025, 1, 6, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		message *persistence.AsyncWorkflowMessage
		reason  string
	}{
		{
			name: "start workflow",
			message: &persistence.AsyncWorkflowMessage{
				ShardID: 0, SourceCluster: "local", MessageID: 1, DomainName: "d", WorkflowID: "w", RequestID: "r",
				RequestType: persistence.AsyncWorkflowRequestTypeStartWorkflow, Payload: []byte{0}, PayloadEncoding: constants.EncodingTypeThriftRW,
			},
			reason: "validation error",
		},
		{
			name: "signal with start, large ids, empty payload bytes preserved",
			message: &persistence.AsyncWorkflowMessage{
				ShardID: 16383, SourceCluster: "cluster-b", MessageID: 1 << 40, DomainName: "domain-name", WorkflowID: "wf-id", RequestID: "req-id",
				RequestType: persistence.AsyncWorkflowRequestTypeSignalWithStartWorkflow, Payload: []byte{}, PayloadEncoding: constants.EncodingTypeJSON,
			},
			reason: "decode error: unexpected EOF",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &persistence.InternalEnqueueAsyncWorkflowMessageToDLQRequest{
				EnqueueAsyncWorkflowMessageToDLQRequest: &persistence.EnqueueAsyncWorkflowMessageToDLQRequest{Message: tc.message, Reason: tc.reason},
				CurrentTimeStamp:                        now,
			}
			got := asyncWorkflowDLQMessageFromRow(asyncWorkflowDLQRowFromRequest(request))

			want := &persistence.AsyncWorkflowDLQMessage{AsyncWorkflowMessage: *tc.message, Reason: tc.reason}
			want.CreatedTime = now
			assert.Equal(t, want, got)
		})
	}
}
