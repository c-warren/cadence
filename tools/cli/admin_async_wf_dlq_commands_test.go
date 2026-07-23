// The MIT License (MIT)
//
// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/tools/cli/clitest"
)

func TestAdminReadAsyncWorkflowDLQMessages(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	tests := []struct {
		name          string
		setupMocks    func(*testing.T, *admin.MockClient)
		cmdline       string
		expectedError string
		expectedStrs  []string
	}{
		{
			name: "pagination stops on short page",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				gomock.InOrder(
					client.EXPECT().
						ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *types.ReadAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.ReadAsyncWorkflowMessagesFromDLQResponse, error) {
							// First page starts at the exclusive empty cursor.
							assert.Equal(t, int64(constants.EmptyMessageID), req.LastMessageID)
							assert.Equal(t, "my-queue", req.QueueName)
							assert.Equal(t, int32(0), req.ShardID)
							return &types.ReadAsyncWorkflowMessagesFromDLQResponse{
								Messages: []*types.AsyncWorkflowMessage{
									{MessageID: 10, PartitionKey: "pk-10", Encoding: "thriftrw", Payload: []byte("abc")},
									{MessageID: 11, PartitionKey: "pk-11", Encoding: "thriftrw", Payload: []byte("def")},
								},
								LastMessageID: 11,
							}, nil
						}),
					client.EXPECT().
						ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *types.ReadAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.ReadAsyncWorkflowMessagesFromDLQResponse, error) {
							// Cursor advanced to the previous response's LastMessageID.
							assert.Equal(t, int64(11), req.LastMessageID)
							return &types.ReadAsyncWorkflowMessagesFromDLQResponse{
								Messages: []*types.AsyncWorkflowMessage{
									{MessageID: 12, PartitionKey: "pk-12", Encoding: "thriftrw", Payload: []byte("ghi")},
								},
								LastMessageID: 12,
							}, nil
						}),
				)
			},
			cmdline:      "cadence admin async-wf-queue dlq read --queue_name my-queue --shard_id 0 --pagesize 2",
			expectedStrs: []string{"pk-10", "pk-11", "pk-12"},
		},
		{
			name: "max message count caps output and stops early",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				client.EXPECT().
					ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
					Return(&types.ReadAsyncWorkflowMessagesFromDLQResponse{
						Messages: []*types.AsyncWorkflowMessage{
							{MessageID: 1, PartitionKey: "pk-1", Encoding: "thriftrw", Payload: []byte("a")},
							{MessageID: 2, PartitionKey: "pk-2", Encoding: "thriftrw", Payload: []byte("b")},
							{MessageID: 3, PartitionKey: "pk-3", Encoding: "thriftrw", Payload: []byte("c")},
						},
						LastMessageID: 3,
					}, nil).
					Times(1)
			},
			cmdline:      "cadence admin async-wf-queue dlq read --queue_name my-queue --shard_id 0 --pagesize 10 --max_message_count 1",
			expectedStrs: []string{"pk-1"},
		},
		{
			name: "queue_name required",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				// No admin call expected.
			},
			cmdline:       "cadence admin async-wf-queue dlq read --shard_id 0",
			expectedError: "queue_name",
		},
		{
			name: "read error surfaces",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				client.EXPECT().
					ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
					Return(nil, assert.AnError).
					Times(1)
			},
			cmdline:       "cadence admin async-wf-queue dlq read --queue_name my-queue --shard_id 0",
			expectedError: "Failed to read async workflow DLQ messages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adminClient := admin.NewMockClient(mockCtrl)
			tt.setupMocks(t, adminClient)
			ioHandler := &testIOHandler{}

			app := NewCliApp(&clientFactoryMock{
				serverAdminClient: adminClient,
			}, WithIOHandler(ioHandler))

			err := clitest.RunCommandLine(t, app, tt.cmdline)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				for _, s := range tt.expectedStrs {
					assert.Contains(t, ioHandler.outputBytes.String(), s)
				}
			}
		})
	}
}

func TestAdminMergeAsyncWorkflowDLQMessages(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	tests := []struct {
		name          string
		setupMocks    func(*testing.T, *admin.MockClient)
		cmdline       string
		expectedError string
	}{
		{
			name: "loop stops when MessagesCount is zero",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				gomock.InOrder(
					client.EXPECT().
						MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *types.MergeAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.MergeAsyncWorkflowMessagesFromDLQResponse, error) {
							assert.Equal(t, "my-queue", req.QueueName)
							assert.Equal(t, int64(42), req.InclusiveEndMessageID)
							return &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 5}, nil
						}),
					client.EXPECT().
						MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
						Return(&types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 3}, nil),
					client.EXPECT().
						MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
						Return(&types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 0}, nil),
				)
			},
			cmdline: "cadence admin async-wf-queue dlq merge --queue_name my-queue --shard_id 0 --last_message_id 42",
		},
		{
			name: "defaults to inclusive end (all messages)",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				client.EXPECT().
					MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *types.MergeAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.MergeAsyncWorkflowMessagesFromDLQResponse, error) {
						assert.Equal(t, constants.InclusiveEndMessageID, req.InclusiveEndMessageID)
						return &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 0}, nil
					}).
					Times(1)
			},
			cmdline: "cadence admin async-wf-queue dlq merge --queue_name my-queue --shard_id 0",
		},
		{
			name: "queue_name required",
			setupMocks: func(t *testing.T, client *admin.MockClient) {
				// No admin call expected.
			},
			cmdline:       "cadence admin async-wf-queue dlq merge --shard_id 0",
			expectedError: "queue_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adminClient := admin.NewMockClient(mockCtrl)
			tt.setupMocks(t, adminClient)

			app := NewCliApp(&clientFactoryMock{
				serverAdminClient: adminClient,
			})

			err := clitest.RunCommandLine(t, app, tt.cmdline)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAdminPurgeAsyncWorkflowDLQMessages(t *testing.T) {
	mockCtrl := gomock.NewController(t)

	t.Run("one call per shard with inclusive end bound", func(t *testing.T) {
		adminClient := admin.NewMockClient(mockCtrl)
		gotShards := map[int32]struct{}{}
		adminClient.EXPECT().
			PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *types.PurgeAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.PurgeAsyncWorkflowMessagesFromDLQResponse, error) {
				assert.Equal(t, "my-queue", req.QueueName)
				assert.Equal(t, int64(99), req.InclusiveEndMessageID)
				gotShards[req.ShardID] = struct{}{}
				return &types.PurgeAsyncWorkflowMessagesFromDLQResponse{}, nil
			}).
			Times(2)

		app := NewCliApp(&clientFactoryMock{
			serverAdminClient: adminClient,
		})

		err := clitest.RunCommandLine(t, app, "cadence admin async-wf-queue dlq purge --queue_name my-queue --shards 1,2 --last_message_id 99")
		assert.NoError(t, err)
		assert.Equal(t, map[int32]struct{}{1: {}, 2: {}}, gotShards)
	})

	t.Run("defaults to inclusive end (all messages)", func(t *testing.T) {
		adminClient := admin.NewMockClient(mockCtrl)
		adminClient.EXPECT().
			PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *types.PurgeAsyncWorkflowMessagesFromDLQRequest, _ ...yarpc.CallOption) (*types.PurgeAsyncWorkflowMessagesFromDLQResponse, error) {
				assert.Equal(t, constants.InclusiveEndMessageID, req.InclusiveEndMessageID)
				return &types.PurgeAsyncWorkflowMessagesFromDLQResponse{}, nil
			}).
			Times(1)

		app := NewCliApp(&clientFactoryMock{
			serverAdminClient: adminClient,
		})

		err := clitest.RunCommandLine(t, app, "cadence admin async-wf-queue dlq purge --queue_name my-queue --shard_id 7")
		assert.NoError(t, err)
	})

	t.Run("queue_name required", func(t *testing.T) {
		adminClient := admin.NewMockClient(mockCtrl)

		app := NewCliApp(&clientFactoryMock{
			serverAdminClient: adminClient,
		})

		err := clitest.RunCommandLine(t, app, "cadence admin async-wf-queue dlq purge --shard_id 0")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue_name")
	})
}
