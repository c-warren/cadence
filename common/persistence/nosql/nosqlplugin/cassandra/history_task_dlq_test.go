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

package cassandra

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestPointDeleteDLQ_EnqueueStandbyTask(t *testing.T) {
	tests := []struct {
		name        string
		request     *persistence.EnqueueStandbyTaskRequest
		queryMockFn func(query *gocql.MockQuery)
		wantErr     bool
	}{
		{
			name: "successfully enqueue task",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return true, nil
				}).Times(1)
			},
			wantErr: false,
		},
		{
			name: "task already exists",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return false, nil
				}).Times(1)
			},
			wantErr: true,
		},
		{
			name: "query execution error",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return false, errors.New("query failed")
				}).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)

			session := &fakeSession{
				query: query,
			}

			logger := testlogger.New(t)
			dlq := NewCassandraPointDeleteDLQ(session, logger)

			err := dlq.EnqueueStandbyTask(context.Background(), tc.request)

			if tc.wantErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestRangeDeleteDLQ_EnqueueStandbyTask(t *testing.T) {
	tests := []struct {
		name        string
		request     *persistence.EnqueueStandbyTaskRequest
		queryMockFn func(query *gocql.MockQuery)
		wantErr     bool
	}{
		{
			name: "successfully enqueue task",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return true, nil
				}).Times(1)
			},
			wantErr: false,
		},
		{
			name: "task already exists",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(1)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return false, nil
				}).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)

			session := &fakeSession{
				query: query,
			}

			logger := testlogger.New(t)
			dlq := NewCassandraRangeDeleteDLQ(session, logger)

			err := dlq.EnqueueStandbyTask(context.Background(), tc.request)

			if tc.wantErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestPointDeleteDLQ_DeleteStandbyTask(t *testing.T) {
	tests := []struct {
		name        string
		request     *persistence.DeleteStandbyTaskRequest
		queryMockFn func(query *gocql.MockQuery)
		wantErr     bool
	}{
		{
			name: "successfully delete task",
			request: &persistence.DeleteStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
			},
			queryMockFn: func(query *gocql.MockQuery) {
				// Multiple calls for different task types
				query.EXPECT().WithContext(gomock.Any()).Return(query).AnyTimes()
				query.EXPECT().Exec().Return(nil).AnyTimes()
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)

			session := &fakeSession{
				query: query,
			}

			logger := testlogger.New(t)
			dlq := NewCassandraPointDeleteDLQ(session, logger)

			err := dlq.DeleteStandbyTask(context.Background(), tc.request)

			if tc.wantErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestComparisonDLQ_EnqueueStandbyTask(t *testing.T) {
	tests := []struct {
		name          string
		request       *persistence.EnqueueStandbyTaskRequest
		queryMockFn   func(query *gocql.MockQuery)
		wantErr       bool
		pointFails    bool
		rangeFails    bool
		expectSuccess bool
	}{
		{
			name: "both implementations succeed",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				// Two calls: one for point-delete, one for range-delete
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(2)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return true, nil
				}).Times(2)
			},
			expectSuccess: true,
			wantErr:       false,
		},
		{
			name: "both implementations fail",
			request: &persistence.EnqueueStandbyTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "cluster",
				ClusterAttributeName:  "test-cluster",
				WorkflowID:            "wf-1",
				RunID:                 "run-1",
				TaskID:                100,
				VisibilityTimestamp:   time.Now().UnixNano(),
				TaskType:              0,
				TaskPayload:           []byte("test-payload"),
				Version:               1,
			},
			queryMockFn: func(query *gocql.MockQuery) {
				query.EXPECT().WithContext(gomock.Any()).Return(query).Times(2)
				query.EXPECT().MapScanCAS(gomock.Any()).DoAndReturn(func(m map[string]interface{}) (bool, error) {
					return false, errors.New("both failed")
				}).Times(2)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			query := gocql.NewMockQuery(ctrl)
			tc.queryMockFn(query)

			session := &fakeSession{
				query: query,
			}

			logger := testlogger.New(t)
			dlq := NewCassandraComparisonDLQ(session, logger)

			err := dlq.EnqueueStandbyTask(context.Background(), tc.request)

			if tc.wantErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestPageTokenSerialization(t *testing.T) {
	logger := testlogger.New(t)
	dlq := NewCassandraPointDeleteDLQ(nil, logger)

	tests := []struct {
		name  string
		token *pageToken
	}{
		{
			name: "valid token",
			token: &pageToken{
				VisibilityTimestamp: 123456789,
				TaskID:              999,
			},
		},
		{
			name: "zero values",
			token: &pageToken{
				VisibilityTimestamp: 0,
				TaskID:              0,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serialized := dlq.serializePageToken(tc.token)
			deserialized, err := dlq.deserializePageToken(serialized)

			if err != nil {
				t.Errorf("failed to deserialize token: %v", err)
			}

			if deserialized.VisibilityTimestamp != tc.token.VisibilityTimestamp {
				t.Errorf("VisibilityTimestamp mismatch: got %d, want %d",
					deserialized.VisibilityTimestamp, tc.token.VisibilityTimestamp)
			}

			if deserialized.TaskID != tc.token.TaskID {
				t.Errorf("TaskID mismatch: got %d, want %d",
					deserialized.TaskID, tc.token.TaskID)
			}
		})
	}
}
