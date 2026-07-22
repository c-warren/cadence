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
	"encoding/base64"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/tools/common/commoncli"
)

// asyncWFDLQPayloadSnippetLen bounds the payload preview rendered in the read table.
const asyncWFDLQPayloadSnippetLen = 64

// AsyncWorkflowDLQRow is a single rendered row of the async workflow poison DLQ.
type AsyncWorkflowDLQRow struct {
	ShardID      int       `header:"Shard ID" json:"shardID"`
	MessageID    int64     `header:"Message ID" json:"messageID"`
	PartitionKey string    `header:"Partition Key" json:"partitionKey"`
	Encoding     string    `header:"Encoding" json:"encoding"`
	PayloadSize  int       `header:"Payload Size" json:"payloadSize"`
	Payload      string    `header:"Payload (base64)" json:"payload"`
	CreatedTime  time.Time `header:"Created Time" json:"createdTime"`
}

// AdminReadAsyncWorkflowDLQMessages reads messages from the async workflow poison DLQ.
func AdminReadAsyncWorkflowDLQMessages(c *cli.Context) error {
	queueName, err := getRequiredOption(c, FlagQueueName)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}

	pageSize := int32(defaultPageSize)
	if c.IsSet(FlagPageSize) {
		pageSize = int32(c.Int(FlagPageSize))
	}

	// LastMessageID is the exclusive cursor to start reading after.
	startMessageID := int64(constants.EmptyMessageID)
	if c.IsSet(FlagLastMessageID) {
		startMessageID = c.Int64(FlagLastMessageID)
	}

	remaining := int64(-1) // -1 means unbounded.
	if c.IsSet(FlagMaxMessageCount) {
		remaining = c.Int64(FlagMaxMessageCount)
	}

	adminClient, err := getDeps(c).ServerAdminClient(c)
	if err != nil {
		return err
	}

	var rows []AsyncWorkflowDLQRow
	for _, shardID := range getAsyncWFDLQShards(c) {
		if remaining == 0 {
			break
		}
		request := &types.ReadAsyncWorkflowMessagesFromDLQRequest{
			QueueName:     queueName,
			ShardID:       int32(shardID),
			LastMessageID: startMessageID,
			PageSize:      pageSize,
		}

		for {
			ctx, cancel, err := newContext(c)
			if err != nil {
				return commoncli.Problem("Error in creating context:", err)
			}
			resp, err := adminClient.ReadAsyncWorkflowMessagesFromDLQ(ctx, request)
			cancel()
			if err != nil {
				return commoncli.Problem(fmt.Sprintf("Failed to read async workflow DLQ messages for shard %d", shardID), err)
			}

			for _, msg := range resp.Messages {
				rows = append(rows, AsyncWorkflowDLQRow{
					ShardID:      shardID,
					MessageID:    msg.MessageID,
					PartitionKey: msg.PartitionKey,
					Encoding:     msg.Encoding,
					PayloadSize:  len(msg.Payload),
					Payload:      payloadSnippet(msg.Payload),
					CreatedTime:  msg.CreatedTime,
				})
				if remaining > 0 {
					remaining--
					if remaining == 0 {
						break
					}
				}
			}

			// Stop on a short (or empty) page, or once we've hit the requested cap.
			if remaining == 0 || len(resp.Messages) < int(pageSize) {
				break
			}
			request.LastMessageID = resp.LastMessageID
		}
	}

	return Render(c, rows, RenderOptions{DefaultTemplate: templateTable, Color: true})
}

// AdminMergeAsyncWorkflowDLQMessages re-enqueues messages from the async workflow poison DLQ back onto the queue.
func AdminMergeAsyncWorkflowDLQMessages(c *cli.Context) error {
	queueName, err := getRequiredOption(c, FlagQueueName)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}

	pageSize := int32(defaultPageSize)
	if c.IsSet(FlagPageSize) {
		pageSize = int32(c.Int(FlagPageSize))
	}

	// LastMessageID is the inclusive upper bound of messages to merge.
	inclusiveEndMessageID := constants.InclusiveEndMessageID
	if c.IsSet(FlagLastMessageID) {
		inclusiveEndMessageID = c.Int64(FlagLastMessageID)
	}

	// MaxMessageCount is an optional per-shard safety cap so a queue that keeps
	// being refilled (e.g. the consumer is still nacking new poison messages)
	// cannot make the drain loop run forever. Zero (unset) means unbounded.
	var maxMessageCount int64
	if c.IsSet(FlagMaxMessageCount) {
		maxMessageCount = c.Int64(FlagMaxMessageCount)
	}

	adminClient, err := getDeps(c).ServerAdminClient(c)
	if err != nil {
		return err
	}

	for _, shardID := range getAsyncWFDLQShards(c) {
		request := &types.MergeAsyncWorkflowMessagesFromDLQRequest{
			QueueName:             queueName,
			ShardID:               int32(shardID),
			InclusiveEndMessageID: inclusiveEndMessageID,
			PageSize:              pageSize,
		}

		var merged int64
		for {
			ctx, cancel, err := newContext(c)
			if err != nil {
				return commoncli.Problem("Error in creating context:", err)
			}
			resp, err := adminClient.MergeAsyncWorkflowMessagesFromDLQ(ctx, request)
			cancel()
			if err != nil {
				fmt.Printf("Failed to merge async workflow DLQ messages in shard %d with error: %v.\n", shardID, err)
				break
			}
			if resp.MessagesCount == 0 {
				break
			}
			merged += int64(resp.MessagesCount)
			if maxMessageCount > 0 && merged >= maxMessageCount {
				break
			}
		}
		fmt.Printf("Successfully merged %d async workflow DLQ messages in shard %d.\n", merged, shardID)
	}
	return nil
}

// AdminPurgeAsyncWorkflowDLQMessages deletes messages from the async workflow poison DLQ.
func AdminPurgeAsyncWorkflowDLQMessages(c *cli.Context) error {
	queueName, err := getRequiredOption(c, FlagQueueName)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}

	// LastMessageID is the inclusive upper bound of messages to purge.
	inclusiveEndMessageID := constants.InclusiveEndMessageID
	if c.IsSet(FlagLastMessageID) {
		inclusiveEndMessageID = c.Int64(FlagLastMessageID)
	}

	adminClient, err := getDeps(c).ServerAdminClient(c)
	if err != nil {
		return err
	}

	for _, shardID := range getAsyncWFDLQShards(c) {
		ctx, cancel, err := newContext(c)
		if err != nil {
			return commoncli.Problem("Error in creating context:", err)
		}
		_, err = adminClient.PurgeAsyncWorkflowMessagesFromDLQ(ctx, &types.PurgeAsyncWorkflowMessagesFromDLQRequest{
			QueueName:             queueName,
			ShardID:               int32(shardID),
			InclusiveEndMessageID: inclusiveEndMessageID,
		})
		cancel()
		if err != nil {
			fmt.Printf("Failed to purge async workflow DLQ messages in shard %d with error: %v.\n", shardID, err)
			continue
		}
		fmt.Printf("Successfully purged async workflow DLQ messages in shard %d.\n", shardID)
	}
	return nil
}

// getAsyncWFDLQShards resolves the set of shards to operate on. A single shard via
// --shard_id takes precedence; otherwise --shards ranges are used, falling back to
// STDIN (one shard id per line) when neither flag is provided.
func getAsyncWFDLQShards(c *cli.Context) []int {
	if c.IsSet(FlagShardID) {
		return []int{c.Int(FlagShardID)}
	}

	var shardChan chan int
	if c.IsSet(FlagShards) {
		shardChan = generateShardRangeFromFlags(c)
	} else {
		shardChan = getShards(c)
	}

	var shards []int
	for shardID := range shardChan {
		shards = append(shards, shardID)
	}
	return shards
}

func payloadSnippet(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	encoded := base64.StdEncoding.EncodeToString(payload)
	if len(encoded) > asyncWFDLQPayloadSnippetLen {
		return encoded[:asyncWFDLQPayloadSnippetLen] + "..."
	}
	return encoded
}
