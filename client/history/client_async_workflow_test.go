// Copyright (c) 2026 Uber Technologies Inc.
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

package history

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
)

func TestClient_asyncWorkflowQueue(t *testing.T) {
	tests := []struct {
		name      string
		op        func(Client) (any, error)
		mock      func(*MockPeerResolver, *MockClient)
		want      any
		wantError bool
	}{
		{
			name: "EnqueueAsyncWorkflowMessage success",
			op: func(c Client) (any, error) {
				return c.EnqueueAsyncWorkflowMessage(context.Background(), &types.EnqueueAsyncWorkflowMessageRequest{
					ShardID:   123,
					QueueName: "q1",
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().EnqueueAsyncWorkflowMessage(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.EnqueueAsyncWorkflowMessageResponse{MessageID: 7}, nil).Times(1)
			},
			want: &types.EnqueueAsyncWorkflowMessageResponse{MessageID: 7},
		},
		{
			name: "EnqueueAsyncWorkflowMessage peer resolve error",
			op: func(c Client) (any, error) {
				return c.EnqueueAsyncWorkflowMessage(context.Background(), &types.EnqueueAsyncWorkflowMessageRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("", errors.New("no peer")).Times(1)
			},
			wantError: true,
		},
		{
			name: "EnqueueAsyncWorkflowMessage call error",
			op: func(c Client) (any, error) {
				return c.EnqueueAsyncWorkflowMessage(context.Background(), &types.EnqueueAsyncWorkflowMessageRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().EnqueueAsyncWorkflowMessage(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(nil, errors.New("boom")).Times(1)
			},
			wantError: true,
		},
		{
			name: "EnqueueAsyncWorkflowMessage redirected success on ShardOwnershipLost",
			op: func(c Client) (any, error) {
				return c.EnqueueAsyncWorkflowMessage(context.Background(), &types.EnqueueAsyncWorkflowMessageRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer-1", nil).Times(1)
				p.EXPECT().FromHostAddress("host-test-peer-2").Return("test-peer-2", nil).Times(1)
				c.EXPECT().EnqueueAsyncWorkflowMessage(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer-1")}).
					Return(nil, &types.ShardOwnershipLostError{
						Message: "test-peer-1 lost the shard",
						Owner:   "host-test-peer-2",
					}).Times(1)
				c.EXPECT().EnqueueAsyncWorkflowMessage(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer-2")}).
					Return(&types.EnqueueAsyncWorkflowMessageResponse{MessageID: 9}, nil).Times(1)
			},
			want: &types.EnqueueAsyncWorkflowMessageResponse{MessageID: 9},
		},
		{
			name: "GetAsyncWorkflowMessages success",
			op: func(c Client) (any, error) {
				return c.GetAsyncWorkflowMessages(context.Background(), &types.GetAsyncWorkflowMessagesRequest{
					ShardID:   123,
					QueueName: "q1",
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().GetAsyncWorkflowMessages(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.GetAsyncWorkflowMessagesResponse{
						Messages: []*types.AsyncWorkflowMessage{{MessageID: 1}},
					}, nil).Times(1)
			},
			want: &types.GetAsyncWorkflowMessagesResponse{
				Messages: []*types.AsyncWorkflowMessage{{MessageID: 1}},
			},
		},
		{
			name: "UpdateAsyncWorkflowAckLevel success",
			op: func(c Client) (any, error) {
				return c.UpdateAsyncWorkflowAckLevel(context.Background(), &types.UpdateAsyncWorkflowAckLevelRequest{
					ShardID:  123,
					AckLevel: 5,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().UpdateAsyncWorkflowAckLevel(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.UpdateAsyncWorkflowAckLevelResponse{}, nil).Times(1)
			},
			want: &types.UpdateAsyncWorkflowAckLevelResponse{},
		},
		{
			name: "EnqueueAsyncWorkflowMessageToDLQ success",
			op: func(c Client) (any, error) {
				return c.EnqueueAsyncWorkflowMessageToDLQ(context.Background(), &types.EnqueueAsyncWorkflowMessageToDLQRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().EnqueueAsyncWorkflowMessageToDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.EnqueueAsyncWorkflowMessageToDLQResponse{MessageID: 3}, nil).Times(1)
			},
			want: &types.EnqueueAsyncWorkflowMessageToDLQResponse{MessageID: 3},
		},
		{
			name: "ReadAsyncWorkflowMessagesFromDLQ success",
			op: func(c Client) (any, error) {
				return c.ReadAsyncWorkflowMessagesFromDLQ(context.Background(), &types.ReadAsyncWorkflowMessagesFromDLQRequest{
					ShardID:   123,
					QueueName: "q1",
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.ReadAsyncWorkflowMessagesFromDLQResponse{
						Messages:      []*types.AsyncWorkflowMessage{{MessageID: 2}},
						LastMessageID: 2,
					}, nil).Times(1)
			},
			want: &types.ReadAsyncWorkflowMessagesFromDLQResponse{
				Messages:      []*types.AsyncWorkflowMessage{{MessageID: 2}},
				LastMessageID: 2,
			},
		},
		{
			name: "ReadAsyncWorkflowMessagesFromDLQ peer resolve error",
			op: func(c Client) (any, error) {
				return c.ReadAsyncWorkflowMessagesFromDLQ(context.Background(), &types.ReadAsyncWorkflowMessagesFromDLQRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("", errors.New("no peer")).Times(1)
			},
			wantError: true,
		},
		{
			name: "MergeAsyncWorkflowMessagesFromDLQ success",
			op: func(c Client) (any, error) {
				return c.MergeAsyncWorkflowMessagesFromDLQ(context.Background(), &types.MergeAsyncWorkflowMessagesFromDLQRequest{
					ShardID:               123,
					QueueName:             "q1",
					InclusiveEndMessageID: 50,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 4, LastMessageID: 42}, nil).Times(1)
			},
			want: &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 4, LastMessageID: 42},
		},
		{
			name: "MergeAsyncWorkflowMessagesFromDLQ redirected success on ShardOwnershipLost",
			op: func(c Client) (any, error) {
				return c.MergeAsyncWorkflowMessagesFromDLQ(context.Background(), &types.MergeAsyncWorkflowMessagesFromDLQRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer-1", nil).Times(1)
				p.EXPECT().FromHostAddress("host-test-peer-2").Return("test-peer-2", nil).Times(1)
				c.EXPECT().MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer-1")}).
					Return(nil, &types.ShardOwnershipLostError{
						Message: "test-peer-1 lost the shard",
						Owner:   "host-test-peer-2",
					}).Times(1)
				c.EXPECT().MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer-2")}).
					Return(&types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 1, LastMessageID: 9}, nil).Times(1)
			},
			want: &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 1, LastMessageID: 9},
		},
		{
			name: "PurgeAsyncWorkflowMessagesFromDLQ success",
			op: func(c Client) (any, error) {
				return c.PurgeAsyncWorkflowMessagesFromDLQ(context.Background(), &types.PurgeAsyncWorkflowMessagesFromDLQRequest{
					ShardID:               123,
					QueueName:             "q1",
					InclusiveEndMessageID: 50,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(&types.PurgeAsyncWorkflowMessagesFromDLQResponse{}, nil).Times(1)
			},
			want: &types.PurgeAsyncWorkflowMessagesFromDLQResponse{},
		},
		{
			name: "PurgeAsyncWorkflowMessagesFromDLQ call error",
			op: func(c Client) (any, error) {
				return c.PurgeAsyncWorkflowMessagesFromDLQ(context.Background(), &types.PurgeAsyncWorkflowMessagesFromDLQRequest{
					ShardID: 123,
				})
			},
			mock: func(p *MockPeerResolver, c *MockClient) {
				p.EXPECT().FromShardID(123).Return("test-peer", nil).Times(1)
				c.EXPECT().PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any(), []yarpc.CallOption{yarpc.WithShardKey("test-peer")}).
					Return(nil, errors.New("boom")).Times(1)
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockClient := NewMockClient(ctrl)
			mockPeerResolver := NewMockPeerResolver(ctrl)
			tt.mock(mockPeerResolver, mockClient)

			c := NewClient(10, 1024, mockClient, mockPeerResolver, log.NewNoop())

			got, err := tt.op(c)
			if tt.wantError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
