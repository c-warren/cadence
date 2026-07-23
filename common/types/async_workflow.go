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

package types

import "time"

// AsyncWorkflowMessage is a single message on a shard-scoped async workflow queue.
type AsyncWorkflowMessage struct {
	MessageID    int64
	Payload      []byte
	Encoding     string
	PartitionKey string
	CreatedTime  time.Time
}

// EnqueueAsyncWorkflowMessageRequest is the request for HistoryAPI.EnqueueAsyncWorkflowMessage.
type EnqueueAsyncWorkflowMessageRequest struct {
	ShardID      int32
	QueueName    string
	Payload      []byte
	Encoding     string
	PartitionKey string
}

// GetShardID is an internal getter (TBD...)
func (v *EnqueueAsyncWorkflowMessageRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// EnqueueAsyncWorkflowMessageResponse is the response for HistoryAPI.EnqueueAsyncWorkflowMessage.
type EnqueueAsyncWorkflowMessageResponse struct {
	MessageID int64
}

// GetAsyncWorkflowMessagesRequest is the request for HistoryAPI.GetAsyncWorkflowMessages.
type GetAsyncWorkflowMessagesRequest struct {
	ShardID       int32
	QueueName     string
	LastMessageID int64
	PageSize      int32
}

// GetShardID is an internal getter (TBD...)
func (v *GetAsyncWorkflowMessagesRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// GetAsyncWorkflowMessagesResponse is the response for HistoryAPI.GetAsyncWorkflowMessages.
type GetAsyncWorkflowMessagesResponse struct {
	Messages []*AsyncWorkflowMessage
	AckLevel int64
}

// UpdateAsyncWorkflowAckLevelRequest is the request for HistoryAPI.UpdateAsyncWorkflowAckLevel.
type UpdateAsyncWorkflowAckLevelRequest struct {
	ShardID   int32
	QueueName string
	AckLevel  int64
}

// GetShardID is an internal getter (TBD...)
func (v *UpdateAsyncWorkflowAckLevelRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// UpdateAsyncWorkflowAckLevelResponse is the response for HistoryAPI.UpdateAsyncWorkflowAckLevel.
type UpdateAsyncWorkflowAckLevelResponse struct {
}

// EnqueueAsyncWorkflowMessageToDLQRequest is the request for HistoryAPI.EnqueueAsyncWorkflowMessageToDLQ.
type EnqueueAsyncWorkflowMessageToDLQRequest struct {
	ShardID      int32
	QueueName    string
	Payload      []byte
	Encoding     string
	PartitionKey string
}

// GetShardID is an internal getter (TBD...)
func (v *EnqueueAsyncWorkflowMessageToDLQRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// EnqueueAsyncWorkflowMessageToDLQResponse is the response for HistoryAPI.EnqueueAsyncWorkflowMessageToDLQ.
type EnqueueAsyncWorkflowMessageToDLQResponse struct {
	MessageID int64
}

// ReadAsyncWorkflowMessagesFromDLQRequest is the request for HistoryAPI.ReadAsyncWorkflowMessagesFromDLQ.
type ReadAsyncWorkflowMessagesFromDLQRequest struct {
	ShardID       int32
	QueueName     string
	LastMessageID int64
	PageSize      int32
}

// GetShardID is an internal getter (TBD...)
func (v *ReadAsyncWorkflowMessagesFromDLQRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// GetQueueName is an internal getter (TBD...)
func (v *ReadAsyncWorkflowMessagesFromDLQRequest) GetQueueName() (o string) {
	if v != nil {
		return v.QueueName
	}
	return
}

// GetLastMessageID is an internal getter (TBD...)
func (v *ReadAsyncWorkflowMessagesFromDLQRequest) GetLastMessageID() (o int64) {
	if v != nil {
		return v.LastMessageID
	}
	return
}

// GetPageSize is an internal getter (TBD...)
func (v *ReadAsyncWorkflowMessagesFromDLQRequest) GetPageSize() (o int32) {
	if v != nil {
		return v.PageSize
	}
	return
}

// ReadAsyncWorkflowMessagesFromDLQResponse is the response for HistoryAPI.ReadAsyncWorkflowMessagesFromDLQ.
// LastMessageID is the exclusive cursor to pass as LastMessageID on the next page.
type ReadAsyncWorkflowMessagesFromDLQResponse struct {
	Messages      []*AsyncWorkflowMessage
	LastMessageID int64
}

// MergeAsyncWorkflowMessagesFromDLQRequest is the request for HistoryAPI.MergeAsyncWorkflowMessagesFromDLQ.
type MergeAsyncWorkflowMessagesFromDLQRequest struct {
	ShardID               int32
	QueueName             string
	InclusiveEndMessageID int64
	PageSize              int32
}

// GetShardID is an internal getter (TBD...)
func (v *MergeAsyncWorkflowMessagesFromDLQRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// GetQueueName is an internal getter (TBD...)
func (v *MergeAsyncWorkflowMessagesFromDLQRequest) GetQueueName() (o string) {
	if v != nil {
		return v.QueueName
	}
	return
}

// GetInclusiveEndMessageID is an internal getter (TBD...)
func (v *MergeAsyncWorkflowMessagesFromDLQRequest) GetInclusiveEndMessageID() (o int64) {
	if v != nil {
		return v.InclusiveEndMessageID
	}
	return
}

// GetPageSize is an internal getter (TBD...)
func (v *MergeAsyncWorkflowMessagesFromDLQRequest) GetPageSize() (o int32) {
	if v != nil {
		return v.PageSize
	}
	return
}

// MergeAsyncWorkflowMessagesFromDLQResponse is the response for HistoryAPI.MergeAsyncWorkflowMessagesFromDLQ.
// MessagesCount is the number of messages merged this call; LastMessageID is the cursor of the last merged message.
type MergeAsyncWorkflowMessagesFromDLQResponse struct {
	MessagesCount int32
	LastMessageID int64
}

// PurgeAsyncWorkflowMessagesFromDLQRequest is the request for HistoryAPI.PurgeAsyncWorkflowMessagesFromDLQ.
type PurgeAsyncWorkflowMessagesFromDLQRequest struct {
	ShardID               int32
	QueueName             string
	InclusiveEndMessageID int64
}

// GetShardID is an internal getter (TBD...)
func (v *PurgeAsyncWorkflowMessagesFromDLQRequest) GetShardID() (o int32) {
	if v != nil {
		return v.ShardID
	}
	return
}

// GetQueueName is an internal getter (TBD...)
func (v *PurgeAsyncWorkflowMessagesFromDLQRequest) GetQueueName() (o string) {
	if v != nil {
		return v.QueueName
	}
	return
}

// GetInclusiveEndMessageID is an internal getter (TBD...)
func (v *PurgeAsyncWorkflowMessagesFromDLQRequest) GetInclusiveEndMessageID() (o int64) {
	if v != nil {
		return v.InclusiveEndMessageID
	}
	return
}

// PurgeAsyncWorkflowMessagesFromDLQResponse is the response for HistoryAPI.PurgeAsyncWorkflowMessagesFromDLQ.
type PurgeAsyncWorkflowMessagesFromDLQResponse struct {
}
