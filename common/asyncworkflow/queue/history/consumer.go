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
	"time"

	historyclient "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
)

const (
	// defaultPageSize is the number of messages fetched per GetAsyncWorkflowMessages call.
	defaultPageSize = 100
	// defaultPollInterval is how long a shard poll loop waits after an empty page before polling again.
	defaultPollInterval = 1 * time.Second
	// defaultCommitInterval is how often each shard flushes its ack level to history.
	defaultCommitInterval = 5 * time.Second
	// defaultErrorBackoff is the pause after a failed RPC to avoid hot-spinning.
	defaultErrorBackoff = 1 * time.Second
	// defaultRPCTimeout bounds a single ack-level commit or DLQ-enqueue RPC.
	defaultRPCTimeout = 10 * time.Second
	// defaultRebalanceInterval is the safety-net period for re-evaluating shard ownership.
	// The worker-service membership ring drives reassignment via Subscribe, but the
	// shard-distributor hashring has no native subscription so we also poll periodically.
	defaultRebalanceInterval = 30 * time.Second
	// msgChanBufferSize bounds the number of in-flight messages buffered towards the consumer.
	msgChanBufferSize = 1000
)

type (
	// consumerImpl is a history-backed messaging.Consumer scoped to a single logical
	// queue (queueName). It runs in the worker service, owns a subset of history
	// shards for that queue via the worker membership hashring, pulls async-workflow
	// messages from the history service for each owned shard, and feeds them to the
	// (unchanged) DefaultConsumer via the Messages() channel.
	consumerImpl struct {
		queueName        string
		historyClient    historyclient.Client
		numHistoryShards int
		resolver         membership.Resolver
		timeSource       clock.TimeSource
		logger           log.Logger
		metricsClient    metrics.Client

		pageSize          int
		pollInterval      time.Duration
		commitInterval    time.Duration
		errorBackoff      time.Duration
		rebalanceInterval time.Duration

		self       membership.HostInfo
		msgChan    chan messaging.Message
		ctx        context.Context
		cancel     context.CancelFunc
		wg         sync.WaitGroup
		membership chan *membership.ChangedEvent

		// shardWorkers is only accessed from the rebalance loop goroutine (and from
		// Start before that goroutine is launched, and from Stop after it has exited),
		// so it needs no additional locking.
		shardWorkers map[int32]*shardWorker
	}

	// shardWorker owns the poll + ack-commit lifecycle for a single history shard.
	shardWorker struct {
		consumer *consumerImpl
		shardID  int32
		ackMgr   messaging.AckManager
		ctx      context.Context
		cancel   context.CancelFunc

		mu            sync.Mutex
		lastCommitted int64
	}
)

var _ messaging.Consumer = (*consumerImpl)(nil)

func newConsumer(
	queueName string,
	historyClient historyclient.Client,
	numHistoryShards int,
	resolver membership.Resolver,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsClient metrics.Client,
) *consumerImpl {
	ctx, cancel := context.WithCancel(context.Background())
	return &consumerImpl{
		queueName:         queueName,
		historyClient:     historyClient,
		numHistoryShards:  numHistoryShards,
		resolver:          resolver,
		timeSource:        timeSource,
		logger:            logger.WithTags(tag.AsyncWFQueueID((&queueConfig{QueueName: queueName}).ID())),
		metricsClient:     metricsClient,
		pageSize:          defaultPageSize,
		pollInterval:      defaultPollInterval,
		commitInterval:    defaultCommitInterval,
		errorBackoff:      defaultErrorBackoff,
		rebalanceInterval: defaultRebalanceInterval,
		msgChan:           make(chan messaging.Message, msgChanBufferSize),
		ctx:               ctx,
		cancel:            cancel,
		membership:        make(chan *membership.ChangedEvent, 10),
		shardWorkers:      make(map[int32]*shardWorker),
	}
}

// Start resolves this host's identity, performs the initial shard assignment and
// launches the rebalance loop.
func (c *consumerImpl) Start() error {
	self, err := c.resolver.WhoAmI()
	if err != nil {
		return err
	}
	c.self = self

	if err := c.resolver.Subscribe(service.Worker, c.subscriberName(), c.membership); err != nil {
		c.logger.Warn("Failed to subscribe to membership changes, relying on periodic rebalance only", tag.Error(err))
	}

	c.reassignShards()

	c.wg.Add(1)
	go c.rebalanceLoop()

	c.logger.Info("Started history-backed async workflow consumer")
	return nil
}

// Stop cancels all goroutines, unsubscribes from membership and closes the
// Messages channel exactly once so DefaultConsumer's readers terminate.
func (c *consumerImpl) Stop() {
	c.logger.Info("Stopping history-backed async workflow consumer")
	if err := c.resolver.Unsubscribe(service.Worker, c.subscriberName()); err != nil {
		c.logger.Warn("Failed to unsubscribe from membership changes", tag.Error(err))
	}
	c.cancel()
	c.wg.Wait()
	close(c.msgChan)
	c.logger.Info("Stopped history-backed async workflow consumer")
}

// Messages returns the channel DefaultConsumer reads from.
func (c *consumerImpl) Messages() <-chan messaging.Message {
	return c.msgChan
}

func (c *consumerImpl) subscriberName() string {
	return "async-workflow-consumer:" + c.queueName
}

func (c *consumerImpl) rebalanceLoop() {
	defer c.wg.Done()

	ticker := c.timeSource.NewTicker(c.rebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.Chan():
			c.reassignShards()
		case <-c.membership:
			drainMembershipCh(c.membership)
			c.logger.Debug("Worker membership changed, re-evaluating owned shards")
			c.reassignShards()
		}
	}
}

// reassignShards computes the set of shards owned by this host for this queue and
// reconciles the running shardWorkers with it: starting workers for newly-owned
// shards and stopping workers for shards no longer owned. Double ownership across
// a rebalance is safe because StartWorkflowExecution is idempotent
// (WorkflowExecutionAlreadyStartedError is treated as success) and ack levels are
// committed with a compare-and-set, so no distributed lock is required.
func (c *consumerImpl) reassignShards() {
	owned, err := c.computeOwnedShards()
	if err != nil {
		c.logger.Warn("Failed to compute owned shards, keeping current assignment", tag.Error(err))
		return
	}

	for shardID := range owned {
		if _, running := c.shardWorkers[shardID]; running {
			continue
		}
		c.startShardWorker(shardID)
	}

	for shardID, sw := range c.shardWorkers {
		if _, keep := owned[shardID]; keep {
			continue
		}
		c.logger.Info("Stopping worker for shard no longer owned", tag.ShardID(int(shardID)))
		sw.cancel()
		delete(c.shardWorkers, shardID)
	}

	c.logger.Debug("Reassigned shards", tag.Dynamic("owned-shard-count", len(c.shardWorkers)))
}

func (c *consumerImpl) startShardWorker(shardID int32) {
	ctx, cancel := context.WithCancel(c.ctx)
	sw := &shardWorker{
		consumer:      c,
		shardID:       shardID,
		ackMgr:        messaging.NewContinuousAckManager(c.logger.WithTags(tag.ShardID(int(shardID)))),
		ctx:           ctx,
		cancel:        cancel,
		lastCommitted: -1,
	}
	c.shardWorkers[shardID] = sw

	c.wg.Add(2)
	go sw.pollLoop()
	go sw.commitLoop()
	c.logger.Info("Started worker for shard", tag.ShardID(int(shardID)))
}

// pollLoop repeatedly fetches messages for its shard and emits them onto the
// shared message channel in order.
func (s *shardWorker) pollLoop() {
	defer s.consumer.wg.Done()

	c := s.consumer
	// Start the cursor below the first possible message id (0). Message reads are
	// exclusive (message_id > cursor), and the history handler clamps the cursor up
	// to the committed ack level, so seeding at EmptyMessageID (-1) fetches from the
	// beginning for a fresh queue and resumes after the ack level otherwise.
	lastMessageID := int64(constants.EmptyMessageID)
	seeded := false

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		resp, err := c.historyClient.GetAsyncWorkflowMessages(s.ctx, &types.GetAsyncWorkflowMessagesRequest{
			ShardID:       s.shardID,
			QueueName:     c.queueName,
			LastMessageID: lastMessageID,
			PageSize:      int32(c.pageSize),
		})
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			c.logger.Warn("Failed to fetch async workflow messages", tag.ShardID(int(s.shardID)), tag.Error(err))
			if err := c.timeSource.SleepWithContext(s.ctx, c.errorBackoff); err != nil {
				return
			}
			continue
		}

		// Resume the cursor and committed-ack baseline from the server's committed
		// ack level on the first successful response, so subsequent polls (and
		// commits) start after what has already been acked.
		if !seeded {
			if resp.AckLevel > lastMessageID {
				lastMessageID = resp.AckLevel
			}
			s.setCommittedBaseline(resp.AckLevel)
			seeded = true
		}

		for _, msg := range resp.Messages {
			if msg.MessageID <= lastMessageID {
				// Defensive: the server serves strictly-increasing IDs after the
				// cursor, but guard the ackManager's strictly-increasing invariant.
				c.logger.Warn("Skipping out-of-order async workflow message",
					tag.ShardID(int(s.shardID)), tag.Dynamic("message-id", msg.MessageID), tag.Dynamic("last-message-id", lastMessageID))
				continue
			}
			if err := s.ackMgr.ReadItem(msg.MessageID); err != nil {
				c.logger.Error("Failed to register async workflow message with ack manager",
					tag.ShardID(int(s.shardID)), tag.Dynamic("message-id", msg.MessageID), tag.Error(err))
				continue
			}

			m := &messageImpl{
				sw:           s,
				payload:      msg.Payload,
				encoding:     msg.Encoding,
				partitionKey: msg.PartitionKey,
				shardID:      s.shardID,
				messageID:    msg.MessageID,
			}
			select {
			case c.msgChan <- m:
				lastMessageID = msg.MessageID
			case <-s.ctx.Done():
				return
			}
		}

		if len(resp.Messages) == 0 {
			if err := c.timeSource.SleepWithContext(s.ctx, c.pollInterval); err != nil {
				return
			}
		}
	}
}

// commitLoop periodically flushes the shard's contiguous ack level to the history
// service, batching many message acks into a single RPC. It performs a final
// commit when the shard is stopped.
func (s *shardWorker) commitLoop() {
	defer s.consumer.wg.Done()

	ticker := s.consumer.timeSource.NewTicker(s.consumer.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.commit()
			return
		case <-ticker.Chan():
			s.commit()
		}
	}
}

// commit flushes the current contiguous ack level to history if it advanced past
// the last committed value. It is safe to call concurrently.
func (s *shardWorker) commit() {
	s.mu.Lock()
	ackLevel := s.ackMgr.GetAckLevel()
	if ackLevel <= s.lastCommitted {
		s.mu.Unlock()
		return
	}
	toCommit := ackLevel
	s.mu.Unlock()

	c := s.consumer
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()
	_, err := c.historyClient.UpdateAsyncWorkflowAckLevel(ctx, &types.UpdateAsyncWorkflowAckLevelRequest{
		ShardID:   s.shardID,
		QueueName: c.queueName,
		AckLevel:  toCommit,
	})
	if err != nil {
		c.logger.Warn("Failed to commit async workflow ack level",
			tag.ShardID(int(s.shardID)), tag.Dynamic("ack-level", toCommit), tag.Error(err))
		return
	}

	s.mu.Lock()
	if toCommit > s.lastCommitted {
		s.lastCommitted = toCommit
	}
	s.mu.Unlock()
}

func (s *shardWorker) setCommittedBaseline(ackLevel int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ackLevel > s.lastCommitted {
		s.lastCommitted = ackLevel
	}
}

// ackMessage advances the contiguous ack level for a successfully processed message.
func (s *shardWorker) ackMessage(messageID int64) error {
	s.ackMgr.AckItem(messageID)
	return nil
}

// nackMessage dead-letters a failed message and then advances the ack level past
// it. If the DLQ write fails, the ack level is NOT advanced (see messageImpl.Nack).
func (s *shardWorker) nackMessage(m *messageImpl) error {
	c := s.consumer
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()
	_, err := c.historyClient.EnqueueAsyncWorkflowMessageToDLQ(ctx, &types.EnqueueAsyncWorkflowMessageToDLQRequest{
		ShardID:      s.shardID,
		QueueName:    c.queueName,
		Payload:      m.payload,
		Encoding:     m.encoding,
		PartitionKey: m.partitionKey,
	})
	if err != nil {
		c.logger.Error("Failed to enqueue async workflow message to DLQ, will retry on next poll",
			tag.ShardID(int(s.shardID)), tag.Dynamic("message-id", m.messageID), tag.Error(err))
		return err
	}
	s.ackMgr.AckItem(m.messageID)
	return nil
}
