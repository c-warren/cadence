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
	"github.com/uber/cadence/common/messaging"
)

// messageImpl is a history-backed implementation of messaging.Message. Each
// message belongs to a single history shard (its owning shardWorker) and carries
// enough context to acknowledge or dead-letter itself concurrently and
// idempotently. DefaultConsumer runs many goroutines that each call exactly one
// of Ack/Nack per message, from any goroutine, so all state mutations are
// delegated to the shardWorker which guards them via its (thread-safe) ackManager.
type messageImpl struct {
	sw           *shardWorker
	payload      []byte
	encoding     string
	partitionKey string
	shardID      int32
	messageID    int64
}

var _ messaging.Message = (*messageImpl)(nil)

// Value returns the raw message payload to be decoded by DefaultConsumer.
func (m *messageImpl) Value() []byte {
	return m.payload
}

// Partition returns the owning history shard ID.
func (m *messageImpl) Partition() int32 {
	return m.shardID
}

// Offset returns the message ID, which is monotonically increasing per shard.
func (m *messageImpl) Offset() int64 {
	return m.messageID
}

// Ack marks the message as successfully processed. It advances the shard's
// contiguous ack level and triggers a (batched) commit of that level.
func (m *messageImpl) Ack() error {
	return m.sw.ackMessage(m.messageID)
}

// Nack marks the message as failed. The message is immediately written to the
// DLQ and then acked so the contiguous ack level advances past it (mirroring the
// kafka consumer, where a nack publishes to the DLQ and still advances the
// offset). If the DLQ write fails we return the error WITHOUT advancing the ack
// level, so the message is left in the backlog and will be redelivered on the
// next poll / consumer restart rather than being silently dropped.
func (m *messageImpl) Nack() error {
	return m.sw.nackMessage(m)
}
