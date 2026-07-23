// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/yarpc"

	historyclient "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

func host(identity string) membership.HostInfo {
	return membership.NewDetailedHostInfo(identity, identity, nil)
}

func newTestConsumer(
	t *testing.T,
	queueName string,
	numHistoryShards int,
	historyClient historyclient.Client,
	resolver membership.Resolver,
) *consumerImpl {
	t.Helper()
	return newConsumer(
		queueName,
		historyClient,
		numHistoryShards,
		resolver,
		clock.NewRealTimeSource(),
		log.NewNoop(),
		metrics.NewNoopMetricsClient(),
	)
}

func newTestShardWorker(c *consumerImpl, shardID int32) *shardWorker {
	ctx, cancel := context.WithCancel(c.ctx)
	return &shardWorker{
		consumer:      c,
		shardID:       shardID,
		ackMgr:        messaging.NewContinuousAckManager(c.logger),
		ctx:           ctx,
		cancel:        cancel,
		lastCommitted: -1,
	}
}

func TestShardOwnershipKey(t *testing.T) {
	assert.Equal(t, "q1/0", shardOwnershipKey("q1", 0))
	assert.Equal(t, "q1/7", shardOwnershipKey("q1", 7))
}

func TestComputeOwnedShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)

	self := host("self")
	other := host("other")

	c := newTestConsumer(t, "q1", 4, historyclient.NewMockClient(ctrl), resolver)
	c.self = self

	// shards 0 and 2 owned by self, 1 and 3 by other
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(self, nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/1").Return(other, nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/2").Return(self, nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/3").Return(other, nil)

	owned, err := c.computeOwnedShards()
	require.NoError(t, err)
	assert.Equal(t, map[int32]struct{}{0: {}, 2: {}}, owned)
}

func TestConsumerPullEmitAndResume(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	historyClient := historyclient.NewMockClient(ctrl)

	self := host("self")
	resolver.EXPECT().WhoAmI().Return(self, nil)
	resolver.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	resolver.EXPECT().Unsubscribe(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// single shard, owned by self
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(self, nil).AnyTimes()

	var mu sync.Mutex
	var seenLastMessageIDs []int64
	callCount := 0
	historyClient.EXPECT().GetAsyncWorkflowMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *types.GetAsyncWorkflowMessagesRequest, _ ...yarpc.CallOption) (*types.GetAsyncWorkflowMessagesResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			seenLastMessageIDs = append(seenLastMessageIDs, req.LastMessageID)
			callCount++
			if callCount == 1 {
				return &types.GetAsyncWorkflowMessagesResponse{
					AckLevel: 5,
					Messages: []*types.AsyncWorkflowMessage{
						{MessageID: 6, Payload: []byte("m6"), Encoding: "thriftrw", PartitionKey: "wf6"},
						{MessageID: 7, Payload: []byte("m7"), Encoding: "thriftrw", PartitionKey: "wf7"},
					},
				}, nil
			}
			return &types.GetAsyncWorkflowMessagesResponse{AckLevel: 7}, nil
		}).MinTimes(2)
	historyClient.EXPECT().UpdateAsyncWorkflowAckLevel(gomock.Any(), gomock.Any()).Return(&types.UpdateAsyncWorkflowAckLevelResponse{}, nil).AnyTimes()

	c := newTestConsumer(t, "q1", 1, historyClient, resolver)
	c.pollInterval = 10 * time.Millisecond
	require.NoError(t, c.Start())
	defer c.Stop()

	msg1 := readMessage(t, c.Messages())
	msg2 := readMessage(t, c.Messages())

	assert.Equal(t, []byte("m6"), msg1.Value())
	assert.Equal(t, int32(0), msg1.Partition())
	assert.Equal(t, int64(6), msg1.Offset())
	assert.Equal(t, []byte("m7"), msg2.Value())
	assert.Equal(t, int64(7), msg2.Offset())

	// Wait for at least a second poll to observe cursor resumption.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seenLastMessageIDs) >= 2
	}, time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// First poll starts below message id 0 (EmptyMessageID) so the server's
	// exclusive read (message_id > cursor) includes message 0; subsequent polls
	// resume past the last emitted message.
	assert.Equal(t, int64(-1), seenLastMessageIDs[0])
	assert.Equal(t, int64(7), seenLastMessageIDs[1])
}

func TestConsumerResumesFromAckLevelOnEmptyPage(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	historyClient := historyclient.NewMockClient(ctrl)

	self := host("self")
	resolver.EXPECT().WhoAmI().Return(self, nil)
	resolver.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	resolver.EXPECT().Unsubscribe(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(self, nil).AnyTimes()

	var mu sync.Mutex
	var seenLastMessageIDs []int64
	historyClient.EXPECT().GetAsyncWorkflowMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *types.GetAsyncWorkflowMessagesRequest, _ ...yarpc.CallOption) (*types.GetAsyncWorkflowMessagesResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			seenLastMessageIDs = append(seenLastMessageIDs, req.LastMessageID)
			// Always empty, but report a committed ack level of 42.
			return &types.GetAsyncWorkflowMessagesResponse{AckLevel: 42}, nil
		}).MinTimes(2)

	c := newTestConsumer(t, "q1", 1, historyClient, resolver)
	c.pollInterval = 5 * time.Millisecond
	require.NoError(t, c.Start())
	defer c.Stop()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seenLastMessageIDs) >= 2
	}, time.Second, 5*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// First poll starts below message id 0 (EmptyMessageID); after the first
	// (empty) response the cursor resumes from the server ack level.
	assert.Equal(t, int64(-1), seenLastMessageIDs[0])
	assert.Equal(t, int64(42), seenLastMessageIDs[1])
}

func TestMessageAckAdvancesAndCommits(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyclient.NewMockClient(ctrl)
	c := newTestConsumer(t, "q1", 1, historyClient, membership.NewMockResolver(ctrl))
	sw := newTestShardWorker(c, 3)

	require.NoError(t, sw.ackMgr.ReadItem(1))
	require.NoError(t, sw.ackMgr.ReadItem(2))

	m1 := &messageImpl{sw: sw, shardID: 3, messageID: 1}
	m2 := &messageImpl{sw: sw, shardID: 3, messageID: 2}

	require.NoError(t, m1.Ack())
	require.NoError(t, m2.Ack())
	assert.Equal(t, int64(2), sw.ackMgr.GetAckLevel())

	historyClient.EXPECT().UpdateAsyncWorkflowAckLevel(gomock.Any(), &types.UpdateAsyncWorkflowAckLevelRequest{
		ShardID:   3,
		QueueName: "q1",
		AckLevel:  2,
	}).Return(&types.UpdateAsyncWorkflowAckLevelResponse{}, nil)

	sw.commit()

	// A second commit with no new acks should not issue another RPC.
	sw.commit()
}

func TestMessageNackDLQsThenAdvances(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyclient.NewMockClient(ctrl)
	c := newTestConsumer(t, "q1", 1, historyClient, membership.NewMockResolver(ctrl))
	sw := newTestShardWorker(c, 2)

	require.NoError(t, sw.ackMgr.ReadItem(1))
	m := &messageImpl{sw: sw, shardID: 2, messageID: 1, payload: []byte("bad"), encoding: "thriftrw", partitionKey: "wf1"}

	historyClient.EXPECT().EnqueueAsyncWorkflowMessageToDLQ(gomock.Any(), &types.EnqueueAsyncWorkflowMessageToDLQRequest{
		ShardID:      2,
		QueueName:    "q1",
		Payload:      []byte("bad"),
		Encoding:     "thriftrw",
		PartitionKey: "wf1",
	}).Return(&types.EnqueueAsyncWorkflowMessageToDLQResponse{}, nil)

	require.NoError(t, m.Nack())
	assert.Equal(t, int64(1), sw.ackMgr.GetAckLevel())
}

func TestMessageNackDLQFailureDoesNotAdvance(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyclient.NewMockClient(ctrl)
	c := newTestConsumer(t, "q1", 1, historyClient, membership.NewMockResolver(ctrl))
	sw := newTestShardWorker(c, 2)

	require.NoError(t, sw.ackMgr.ReadItem(1))
	m := &messageImpl{sw: sw, shardID: 2, messageID: 1, payload: []byte("bad")}

	historyClient.EXPECT().EnqueueAsyncWorkflowMessageToDLQ(gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	require.Error(t, m.Nack())
	// Ack level must not advance past the failed message so it is redelivered.
	assert.Equal(t, int64(0), sw.ackMgr.GetAckLevel())
}

func TestReassignShardsStartsAndStops(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	historyClient := historyclient.NewMockClient(ctrl)

	self := host("self")
	other := host("other")

	// Shard poll + final ack commit are called by started workers; allow any.
	historyClient.EXPECT().GetAsyncWorkflowMessages(gomock.Any(), gomock.Any()).
		Return(&types.GetAsyncWorkflowMessagesResponse{AckLevel: -1}, nil).AnyTimes()
	historyClient.EXPECT().UpdateAsyncWorkflowAckLevel(gomock.Any(), gomock.Any()).
		Return(&types.UpdateAsyncWorkflowAckLevelResponse{}, nil).AnyTimes()

	c := newTestConsumer(t, "q1", 2, historyClient, resolver)
	c.self = self
	c.pollInterval = 10 * time.Millisecond
	defer func() {
		c.cancel()
		c.wg.Wait()
	}()

	// Round 1: self owns shard 0 only.
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(self, nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/1").Return(other, nil)
	c.reassignShards()
	assert.Equal(t, []int32{0}, sortedShardIDs(c.shardWorkers))

	// Round 2: ownership flips to shard 1.
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(other, nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/1").Return(self, nil)
	c.reassignShards()
	assert.Equal(t, []int32{1}, sortedShardIDs(c.shardWorkers))
}

func TestStopClosesMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockResolver(ctrl)
	historyClient := historyclient.NewMockClient(ctrl)

	self := host("self")
	resolver.EXPECT().WhoAmI().Return(self, nil)
	resolver.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	resolver.EXPECT().Unsubscribe(gomock.Any(), gomock.Any()).Return(nil)
	resolver.EXPECT().Lookup(gomock.Any(), "q1/0").Return(self, nil).AnyTimes()
	historyClient.EXPECT().GetAsyncWorkflowMessages(gomock.Any(), gomock.Any()).
		Return(&types.GetAsyncWorkflowMessagesResponse{AckLevel: -1}, nil).AnyTimes()
	historyClient.EXPECT().UpdateAsyncWorkflowAckLevel(gomock.Any(), gomock.Any()).
		Return(&types.UpdateAsyncWorkflowAckLevelResponse{}, nil).AnyTimes()

	c := newTestConsumer(t, "q1", 1, historyClient, resolver)
	c.pollInterval = 10 * time.Millisecond
	require.NoError(t, c.Start())

	msgCh := c.Messages()
	c.Stop()

	// After Stop, ranging over the channel must terminate (channel closed).
	done := make(chan struct{})
	go func() {
		for range msgCh {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Messages channel was not closed after Stop")
	}
}

func readMessage(t *testing.T, ch <-chan messaging.Message) messaging.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func sortedShardIDs(workers map[int32]*shardWorker) []int32 {
	ids := make([]int32, 0, len(workers))
	for id := range workers {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}
