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
	"fmt"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/service"
)

// shardOwnershipKey builds the per-queue membership lookup key for a shard. Keying
// per-queue (rather than by shard alone) makes the worker ring balance each
// logical queue independently, mirroring how kafka balances each topic.
func shardOwnershipKey(queueName string, shardID int32) string {
	return fmt.Sprintf("%s/%d", queueName, shardID)
}

// computeOwnedShards returns the set of history shards this host owns for this
// queue, according to the worker-service membership hashring. On a lookup error
// for the whole ring it returns the error; per-shard lookup errors are logged and
// skipped so a transient failure for one shard does not disrupt the rest.
func (c *consumerImpl) computeOwnedShards() (map[int32]struct{}, error) {
	owned := make(map[int32]struct{})
	for shardID := int32(0); shardID < int32(c.numHistoryShards); shardID++ {
		owner, err := c.resolver.Lookup(service.Worker, shardOwnershipKey(c.queueName, shardID))
		if err != nil {
			c.logger.Warn("Failed to look up shard owner, skipping",
				tag.ShardID(int(shardID)), tag.Error(err))
			continue
		}
		if owner.Identity() == c.self.Identity() {
			owned[shardID] = struct{}{}
		}
	}
	return owned, nil
}

// drainMembershipCh consumes all pending events from the channel without blocking,
// so that a single reassign call covers all queued changes.
func drainMembershipCh(ch <-chan *membership.ChangedEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
