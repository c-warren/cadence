package cassandra

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"github.com/uber/cadence/common/types"
)

func TestInsertAsyncWorkflowDLQMessage(t *testing.T) {
	created := time.Date(2025, 1, 6, 15, 0, 0, 0, time.UTC)
	row := &nosqlplugin.AsyncWorkflowDLQMessageRow{
		AsyncWorkflowMessageRow: nosqlplugin.AsyncWorkflowMessageRow{
			ShardID:         3,
			SourceCluster:   "c1",
			MessageID:       11,
			DomainName:      "domain",
			WorkflowID:      "wf",
			RequestID:       "req",
			RequestType:     1,
			Payload:         []byte{1, 2, 3},
			PayloadEncoding: "thriftrw",
			CreatedTime:     created,
		},
		Reason: "decode error",
	}

	// No IF NOT EXISTS: the DLQ key is the source message's key and a retry must overwrite.
	const wantQuery = `INSERT INTO async_workflow_queue_dlq ` +
		`(shard_id, source_cluster, message_id, message_payload, message_payload_encoding, domain_name, workflow_id, request_id, request_type, reason, created_time) ` +
		`VALUES (3, c1, 11, [1 2 3], thriftrw, domain, wf, req, 1, decode error, 2025-01-06T15:00:00Z)`

	tests := []struct {
		name        string
		queryMockFn func(query *gocql.MockQuery)
		wantErr     bool
	}{
		{
			name: "success",
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().Exec().Return(nil).Times(1)
			},
		},
		{
			name: "exec failure is propagated",
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().Exec().Return(errors.New("exec failed")).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)
			session := &fakeSession{query: query}
			db := newTestCDB(t, session)

			err := db.InsertAsyncWorkflowDLQMessage(context.Background(), row)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{wantQuery}, session.queries)
		})
	}
}

func TestSelectAsyncWorkflowDLQMessages(t *testing.T) {
	created := time.Date(2025, 1, 6, 15, 0, 0, 0, time.UTC)
	const wantQuery = `SELECT message_id, message_payload, message_payload_encoding, domain_name, workflow_id, request_id, request_type, reason, created_time ` +
		`FROM async_workflow_queue_dlq ` +
		`WHERE shard_id = 3 AND source_cluster = c1 AND message_id > 10`

	scan := func(id int64) []interface{} {
		return []interface{}{id, []byte{byte(id)}, "thriftrw", "domain", "wf", "req", int(1), "reason", created}
	}

	tests := []struct {
		name      string
		pageSize  int
		iter      *fakeIter
		nilIter   bool
		wantIDs   []int64
		wantErr   bool
		wantFirst bool
	}{
		{
			name:      "returns rows in scan order with the key stamped on each row",
			pageSize:  10,
			iter:      &fakeIter{scanInputs: [][]interface{}{scan(11), scan(12)}},
			wantIDs:   []int64{11, 12},
			wantFirst: true,
		},
		{
			name:     "stops at pageSize even if the iterator has more",
			pageSize: 2,
			iter:     &fakeIter{scanInputs: [][]interface{}{scan(11), scan(12), scan(13)}},
			wantIDs:  []int64{11, 12},
		},
		{
			name:     "empty dlq",
			pageSize: 5,
			iter:     &fakeIter{},
			wantIDs:  nil,
		},
		{
			name:     "iterator close error is returned",
			pageSize: 5,
			iter:     &fakeIter{scanInputs: [][]interface{}{scan(11)}, closeErr: errors.New("boom")},
			wantErr:  true,
		},
		{
			name:     "nil iterator is an internal service error",
			pageSize: 5,
			nilIter:  true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			query.EXPECT().PageSize(tc.pageSize).Return(query).Times(1)
			query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
			if tc.nilIter {
				query.EXPECT().Iter().Return(nil).Times(1)
			} else {
				query.EXPECT().Iter().Return(tc.iter).Times(1)
			}
			session := &fakeSession{query: query}
			db := newTestCDB(t, session)

			rows, err := db.SelectAsyncWorkflowDLQMessages(context.Background(), 3, "c1", 10, tc.pageSize)

			assert.Equal(t, []string{wantQuery}, session.queries)
			if tc.wantErr {
				require.Error(t, err)
				if tc.nilIter {
					var internalErr *types.InternalServiceError
					assert.ErrorAs(t, err, &internalErr)
				}
				return
			}
			require.NoError(t, err)
			var gotIDs []int64
			for _, r := range rows {
				gotIDs = append(gotIDs, r.MessageID)
				assert.Equal(t, 3, r.ShardID)
				assert.Equal(t, "c1", r.SourceCluster)
				assert.Equal(t, "reason", r.Reason)
			}
			assert.Equal(t, tc.wantIDs, gotIDs)
			if tc.wantFirst {
				assert.Equal(t, "thriftrw", rows[0].PayloadEncoding)
				assert.Equal(t, []byte{11}, rows[0].Payload)
				assert.Equal(t, "domain", rows[0].DomainName)
				assert.Equal(t, "wf", rows[0].WorkflowID)
				assert.Equal(t, "req", rows[0].RequestID)
				assert.Equal(t, 1, rows[0].RequestType)
				assert.Equal(t, created, rows[0].CreatedTime)
			}
			assert.True(t, tc.iter.closed)
		})
	}
}

func TestRangeDeleteAsyncWorkflowDLQMessages(t *testing.T) {
	const wantQuery = `DELETE FROM async_workflow_queue_dlq ` +
		`WHERE shard_id = 3 AND source_cluster = c1 AND message_id <= 20`

	tests := []struct {
		name           string
		allConsistency bool
		queryMockFn    func(query *gocql.MockQuery)
		wantErr        bool
	}{
		{
			name:           "with the ALL-consistency flag on the delete runs at consistency ALL",
			allConsistency: true,
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().Consistency(cassandraAllConslevel).Return(query).Times(1)
				query.EXPECT().Exec().Return(nil).Times(1)
			},
		},
		{
			name: "with the flag off the delete runs at the default consistency",
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().Exec().Return(nil).Times(1)
			},
		},
		{
			name: "exec failure is propagated",
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().Exec().Return(errors.New("exec failed")).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)
			session := &fakeSession{query: query}
			client := gocql.NewMockClient(ctrl)
			dc := persistence.NewDefaultDynamicConfiguration()
			dc.EnableCassandraAllConsistencyLevelDelete = dynamicproperties.GetBoolPropertyFn(tc.allConsistency)
			db := NewCassandraDBFromSession(&config.NoSQL{}, session, testlogger.New(t), dc, DbWithClient(client))

			err := db.RangeDeleteAsyncWorkflowDLQMessages(context.Background(), 3, "c1", 20)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{wantQuery}, session.queries)
		})
	}
}
