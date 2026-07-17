// Copyright (c) 2026 Uber Technologies, Inc.
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

package persistencetests

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/persistence"
)

type (
	// AsyncWorkflowQueuePersistenceSuite contains async workflow queue persistence tests.
	AsyncWorkflowQueuePersistenceSuite struct {
		*TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *AsyncWorkflowQueuePersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *AsyncWorkflowQueuePersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.Require().NotNil(s.AsyncWorkflowQueueMgr, "AsyncWorkflowQueueManager is not supported by this datastore")
}

// TearDownSuite implementation
func (s *AsyncWorkflowQueuePersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// uniqueQueueName returns a queue name unique to a test so the shared test database does not leak
// state across tests within the suite.
func (s *AsyncWorkflowQueuePersistenceSuite) uniqueQueueName() string {
	return "async-queue-" + uuid.New()
}

func (s *AsyncWorkflowQueuePersistenceSuite) enqueue(ctx context.Context, queueName string, shardID int, payload string) int64 {
	resp, err := s.AsyncWorkflowQueueMgr.Enqueue(ctx, &persistence.EnqueueAsyncWorkflowMessageRequest{
		QueueName:    queueName,
		ShardID:      shardID,
		Payload:      []byte(payload),
		Encoding:     "thriftrw",
		PartitionKey: payload,
	})
	s.Require().NoError(err)
	return resp.MessageID
}

// TestEnqueueAssignsMonotonicIDs verifies that messages within a (queue, shard) partition receive a
// zero-based monotonic message id from the single writer.
func (s *AsyncWorkflowQueuePersistenceSuite) TestEnqueueAssignsMonotonicIDs() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueName := s.uniqueQueueName()
	shardID := 1

	for want := int64(0); want < 5; want++ {
		got := s.enqueue(ctx, queueName, shardID, "msg")
		s.Equal(want, got, "expected monotonic message id")
	}
}

// TestReadMessages verifies that ReadMessages returns messages ordered by id, honours the exclusive
// LastMessageID cursor, and respects PageSize.
func (s *AsyncWorkflowQueuePersistenceSuite) TestReadMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueName := s.uniqueQueueName()
	shardID := 2

	const numMessages = 10
	for i := 0; i < numMessages; i++ {
		s.enqueue(ctx, queueName, shardID, "payload")
	}

	// Read from the beginning with a page smaller than the backlog.
	resp, err := s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName:     queueName,
		ShardID:       shardID,
		LastMessageID: -1,
		PageSize:      4,
	})
	s.Require().NoError(err)
	s.Len(resp.Messages, 4)
	for i, msg := range resp.Messages {
		s.Equal(int64(i), msg.MessageID, "messages must be returned in id order")
		s.Equal(queueName, msg.QueueName)
		s.Equal(shardID, msg.ShardID)
		s.Equal([]byte("payload"), msg.Payload)
		s.Equal("thriftrw", msg.Encoding)
	}

	// Resume after the last returned id; the cursor is exclusive.
	last := resp.Messages[len(resp.Messages)-1].MessageID
	resp, err = s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName:     queueName,
		ShardID:       shardID,
		LastMessageID: last,
		PageSize:      100,
	})
	s.Require().NoError(err)
	s.Len(resp.Messages, numMessages-4)
	s.Equal(last+1, resp.Messages[0].MessageID)
}

// TestQueueAndShardIsolation verifies that different logical queues and different shards maintain
// independent id sequences and message sets.
func (s *AsyncWorkflowQueuePersistenceSuite) TestQueueAndShardIsolation() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueA := s.uniqueQueueName()
	queueB := s.uniqueQueueName()

	// Same shard id across two logical queues: each starts its own sequence at 0.
	s.Equal(int64(0), s.enqueue(ctx, queueA, 5, "a0"))
	s.Equal(int64(1), s.enqueue(ctx, queueA, 5, "a1"))
	s.Equal(int64(0), s.enqueue(ctx, queueB, 5, "b0"))

	// Same logical queue across two shards: each shard is independent.
	s.Equal(int64(0), s.enqueue(ctx, queueA, 6, "a-shard6"))

	readAll := func(queueName string, shardID int) persistence.AsyncWorkflowMessageList {
		resp, err := s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
			QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
		})
		s.Require().NoError(err)
		return resp.Messages
	}

	s.Len(readAll(queueA, 5), 2)
	s.Len(readAll(queueB, 5), 1)
	s.Len(readAll(queueA, 6), 1)
	s.Equal([]byte("b0"), readAll(queueB, 5)[0].Payload)
}

// TestAckLevel verifies ack-level initialization, monotonic advance, and that it never moves backwards.
func (s *AsyncWorkflowQueuePersistenceSuite) TestAckLevel() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueName := s.uniqueQueueName()
	shardID := 3

	// A shard with no ack level yet reports the empty sentinel.
	getResp, err := s.AsyncWorkflowQueueMgr.GetAckLevel(ctx, &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID,
	})
	s.Require().NoError(err)
	s.Equal(int64(-1), getResp.AckLevel)

	// Advance the ack level.
	s.Require().NoError(s.AsyncWorkflowQueueMgr.UpdateAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID, AckLevel: 10,
	}))
	getResp, err = s.AsyncWorkflowQueueMgr.GetAckLevel(ctx, &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID,
	})
	s.Require().NoError(err)
	s.Equal(int64(10), getResp.AckLevel)

	// A lower ack level must not move the cursor backwards.
	s.Require().NoError(s.AsyncWorkflowQueueMgr.UpdateAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID, AckLevel: 5,
	}))
	getResp, err = s.AsyncWorkflowQueueMgr.GetAckLevel(ctx, &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID,
	})
	s.Require().NoError(err)
	s.Equal(int64(10), getResp.AckLevel)
}

// TestRangeDeleteAndMonotonicity verifies that range-delete removes acked messages and that new
// enqueues keep increasing ids afterwards (the ack level, not the surviving rows, anchors the sequence).
func (s *AsyncWorkflowQueuePersistenceSuite) TestRangeDeleteAndMonotonicity() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueName := s.uniqueQueueName()
	shardID := 4

	for i := 0; i < 3; i++ {
		s.enqueue(ctx, queueName, shardID, "m") // ids 0,1,2
	}

	// Consume through id 2, then range-delete behind the ack cursor.
	s.Require().NoError(s.AsyncWorkflowQueueMgr.UpdateAckLevel(ctx, &persistence.UpdateAsyncWorkflowAckLevelRequest{
		QueueName: queueName, ShardID: shardID, AckLevel: 2,
	}))
	s.Require().NoError(s.AsyncWorkflowQueueMgr.RangeDeleteMessages(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, InclusiveEndMessageID: 2,
	}))

	resp, err := s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
	})
	s.Require().NoError(err)
	s.Empty(resp.Messages, "range delete should remove all acked messages")

	// Even with the table empty, the next id must not reuse deleted ids; the ack level anchors it.
	s.Equal(int64(3), s.enqueue(ctx, queueName, shardID, "m3"))

	resp, err = s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
	})
	s.Require().NoError(err)
	s.Require().Len(resp.Messages, 1)
	s.Equal(int64(3), resp.Messages[0].MessageID)
}

// TestDLQ verifies the DLQ enqueue/read/range-delete path is independent of the main queue.
func (s *AsyncWorkflowQueuePersistenceSuite) TestDLQ() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	queueName := s.uniqueQueueName()
	shardID := 8

	// A main-queue message on the same (queue, shard) must not appear in the DLQ.
	s.enqueue(ctx, queueName, shardID, "main")

	for i := 0; i < 3; i++ {
		resp, err := s.AsyncWorkflowQueueMgr.EnqueueToDLQ(ctx, &persistence.EnqueueAsyncWorkflowMessageRequest{
			QueueName: queueName, ShardID: shardID, Payload: []byte("poison"), Encoding: "thriftrw",
		})
		s.Require().NoError(err)
		s.Equal(int64(i), resp.MessageID)
	}

	resp, err := s.AsyncWorkflowQueueMgr.ReadMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
	})
	s.Require().NoError(err)
	s.Require().Len(resp.Messages, 3)
	for i, msg := range resp.Messages {
		s.Equal(int64(i), msg.MessageID)
		s.Equal([]byte("poison"), msg.Payload)
	}

	// The main queue still holds only its own single message.
	mainResp, err := s.AsyncWorkflowQueueMgr.ReadMessages(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
	})
	s.Require().NoError(err)
	s.Len(mainResp.Messages, 1)

	// Range-delete the DLQ.
	s.Require().NoError(s.AsyncWorkflowQueueMgr.RangeDeleteMessagesFromDLQ(ctx, &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, InclusiveEndMessageID: 2,
	}))
	resp, err = s.AsyncWorkflowQueueMgr.ReadMessagesFromDLQ(ctx, &persistence.ReadAsyncWorkflowMessagesRequest{
		QueueName: queueName, ShardID: shardID, LastMessageID: -1, PageSize: 100,
	})
	s.Require().NoError(err)
	s.Empty(resp.Messages)
}
