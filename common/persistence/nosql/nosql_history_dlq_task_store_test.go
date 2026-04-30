// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nosql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func setUpMocksForHistoryDLQTaskStore(t *testing.T) (*nosqlHistoryDLQTaskStore, *nosqlplugin.MockDB) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dbMock := nosqlplugin.NewMockDB(ctrl)
	return &nosqlHistoryDLQTaskStore{
		nosqlStore: nosqlStore{db: dbMock, logger: testlogger.New(t)},
	}, dbMock
}

func TestNoSQLHistoryDLQTaskStore_CreateHistoryDLQTask(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	createdAt := time.Date(2025, 6, 1, 12, 0, 1, 0, time.UTC)

	baseRequest := persistence.InternalCreateHistoryDLQTaskRequest{
		ShardID:               5,
		DomainID:              "domain-abc",
		ClusterAttributeScope: "scope-1",
		ClusterAttributeName:  "cluster-west",
		TaskType:              3,
		TaskID:                99,
		VisibilityTimestamp:   now,
		WorkflowID:            "wf-1",
		RunID:                 "run-1",
		Version:               7,
		CreatedAt:             createdAt,
		TaskBlob: &persistence.DataBlob{
			Data:     []byte("task-payload"),
			Encoding: constants.EncodingTypeThriftRW,
		},
	}

	expectedTask := &nosqlplugin.HistoryDLQTaskRow{
		ShardID:               5,
		DomainID:              "domain-abc",
		ClusterAttributeScope: "scope-1",
		ClusterAttributeName:  "cluster-west",
		TaskType:              3,
		TaskID:                99,
		VisibilityTimestamp:   now,
		WorkflowID:            "wf-1",
		RunID:                 "run-1",
		Data:                  []byte("task-payload"),
		DataEncoding:          string(constants.EncodingTypeThriftRW),
		Version:               7,
		CreatedAt:             createdAt,
	}

	tests := map[string]struct {
		setupMock      func(*nosqlplugin.MockDB)
		request        persistence.InternalCreateHistoryDLQTaskRequest
		expectError    bool
		errorValidator func(t *testing.T, err error)
	}{
		"when insert succeeds then no error is returned": {
			setupMock: func(dbMock *nosqlplugin.MockDB) {
				dbMock.EXPECT().
					InsertHistoryDLQTaskRow(ctx, expectedTask).
					Return(nil).
					Times(1)
			},
			request: baseRequest,
		},
		"when the database returns an error then the error is propagated": {
			setupMock: func(dbMock *nosqlplugin.MockDB) {
				dbMock.EXPECT().
					InsertHistoryDLQTaskRow(ctx, gomock.Any()).
					Return(errors.New("connection refused"))
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(false).AnyTimes()
				dbMock.EXPECT().IsTimeoutError(gomock.Any()).Return(false).AnyTimes()
				dbMock.EXPECT().IsThrottlingError(gomock.Any()).Return(false).AnyTimes()
				dbMock.EXPECT().IsDBUnavailableError(gomock.Any()).Return(false).AnyTimes()
			},
			request:     baseRequest,
			expectError: true,
		},
		"when the database returns a throttling error then the error is propagated": {
			setupMock: func(dbMock *nosqlplugin.MockDB) {
				dbMock.EXPECT().
					InsertHistoryDLQTaskRow(ctx, gomock.Any()).
					Return(errors.New("rate exceeded"))
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(false).AnyTimes()
				dbMock.EXPECT().IsTimeoutError(gomock.Any()).Return(false).AnyTimes()
				dbMock.EXPECT().IsThrottlingError(gomock.Any()).Return(true).AnyTimes()
				dbMock.EXPECT().IsDBUnavailableError(gomock.Any()).Return(false).AnyTimes()
			},
			request:     baseRequest,
			expectError: true,
		},
		"when task blob is nil then InvalidPersistenceRequestError is returned": {
			setupMock: func(dbMock *nosqlplugin.MockDB) {
				// no DB calls expected — nil check fires before any DB interaction
			},
			request: func() persistence.InternalCreateHistoryDLQTaskRequest {
				r := baseRequest
				r.TaskBlob = nil
				return r
			}(),
			expectError: true,
			errorValidator: func(t *testing.T, err error) {
				var invalidReqErr *persistence.InvalidPersistenceRequestError
				assert.ErrorAs(t, err, &invalidReqErr)
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store, dbMock := setUpMocksForHistoryDLQTaskStore(t)
			tc.setupMock(dbMock)

			err := store.CreateHistoryDLQTask(ctx, tc.request)

			if tc.expectError {
				assert.Error(t, err)
				if tc.errorValidator != nil {
					tc.errorValidator(t, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNoSQLHistoryDLQTaskStore_GetName(t *testing.T) {
	store, dbMock := setUpMocksForHistoryDLQTaskStore(t)
	dbMock.EXPECT().PluginName().Return("cassandra")
	assert.Equal(t, "cassandra", store.GetName())
}

func TestNoSQLHistoryDLQTaskStore_Close(t *testing.T) {
	// Close is a no-op on the nosqlHistoryDLQTaskStore itself; the db is not closed here.
	store, _ := setUpMocksForHistoryDLQTaskStore(t)
	store.Close() // must not panic
}
