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

package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
)

func TestHistoryTaskDLQManager_CreateHistoryDLQTask(t *testing.T) {
	testTask := &ActivityTask{
		WorkflowIdentifier: WorkflowIdentifier{
			DomainID:   "test-domain",
			WorkflowID: "test-workflow",
			RunID:      "test-run",
		},
		TaskData: TaskData{
			Version:             1,
			TaskID:              42,
			VisibilityTimestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		TargetDomainID: "target-domain",
		TaskList:       "test-tasklist",
	}

	serializedBlob := DataBlob{
		Data:     []byte("serialized-task"),
		Encoding: constants.EncodingTypeThriftRW,
	}

	tests := []struct {
		name      string
		mockSetup func(*MockHistoryDLQTaskStore, *MockHistoryTaskSerializer)
		wantErr   string
	}{
		{
			name: "successful write",
			mockSetup: func(store *MockHistoryDLQTaskStore, ser *MockHistoryTaskSerializer) {
				ser.EXPECT().
					SerializeTask(HistoryTaskCategoryTransfer, testTask).
					Return(serializedBlob, nil)
				store.EXPECT().
					CreateHistoryDLQTask(gomock.Any(), gomock.AssignableToTypeOf(InternalCreateHistoryDLQTaskRequest{})).
					DoAndReturn(func(_ context.Context, req InternalCreateHistoryDLQTaskRequest) error {
						assert.Equal(t, 1, req.ShardID)
						assert.Equal(t, "test-domain", req.DomainID)
						assert.Equal(t, "scope", req.ClusterAttributeScope)
						assert.Equal(t, "cluster-a", req.ClusterAttributeName)
						assert.Equal(t, int64(42), req.TaskID)
						assert.Equal(t, "test-workflow", req.WorkflowID)
						assert.Equal(t, "test-run", req.RunID)
						assert.Equal(t, serializedBlob.Data, req.TaskBlob.Data)
						return nil
					})
			},
		},
		{
			name: "serialization failure",
			mockSetup: func(store *MockHistoryDLQTaskStore, ser *MockHistoryTaskSerializer) {
				ser.EXPECT().
					SerializeTask(HistoryTaskCategoryTransfer, testTask).
					Return(DataBlob{}, errors.New("codec error"))
				// store must NOT be called
			},
			wantErr: "failed to serialize history DLQ task: codec error",
		},
		{
			name: "store error propagation",
			mockSetup: func(store *MockHistoryDLQTaskStore, ser *MockHistoryTaskSerializer) {
				ser.EXPECT().
					SerializeTask(HistoryTaskCategoryTransfer, testTask).
					Return(serializedBlob, nil)
				store.EXPECT().
					CreateHistoryDLQTask(gomock.Any(), gomock.Any()).
					Return(errors.New("cassandra unavailable"))
			},
			wantErr: "cassandra unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStore := NewMockHistoryDLQTaskStore(ctrl)
			mockSerializer := NewMockHistoryTaskSerializer(ctrl)
			tc.mockSetup(mockStore, mockSerializer)

			mgr := NewHistoryTaskDLQManager(mockStore, mockSerializer, log.NewNoop())
			err := mgr.CreateHistoryDLQTask(context.Background(), CreateHistoryDLQTaskRequest{
				ShardID:               1,
				DomainID:              "test-domain",
				ClusterAttributeScope: "scope",
				ClusterAttributeName:  "cluster-a",
				Task:                  testTask,
			})

			if tc.wantErr != "" {
				assert.EqualError(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHistoryTaskDLQManager_GetName(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockHistoryDLQTaskStore(ctrl)
	mockStore.EXPECT().GetName().Return("cassandra")

	mgr := NewHistoryTaskDLQManager(mockStore, NewMockHistoryTaskSerializer(ctrl), log.NewNoop())
	assert.Equal(t, "cassandra", mgr.GetName())
}

func TestHistoryTaskDLQManager_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockHistoryDLQTaskStore(ctrl)
	mockStore.EXPECT().Close()

	mgr := NewHistoryTaskDLQManager(mockStore, NewMockHistoryTaskSerializer(ctrl), log.NewNoop())
	mgr.Close()
}
