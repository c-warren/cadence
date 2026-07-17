// The MIT License (MIT)
//
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

package persistence

import (
	"context"

	"github.com/uber/cadence/common/clock"
)

type asyncWorkflowQueueManager struct {
	persistence AsyncWorkflowQueueStore
	timeSrc     clock.TimeSource
}

var _ AsyncWorkflowQueueManager = (*asyncWorkflowQueueManager)(nil)

// NewAsyncWorkflowQueueManager returns a new AsyncWorkflowQueueManager backed by the given store.
func NewAsyncWorkflowQueueManager(store AsyncWorkflowQueueStore) AsyncWorkflowQueueManager {
	return &asyncWorkflowQueueManager{
		persistence: store,
		timeSrc:     clock.NewRealTimeSource(),
	}
}

func (q *asyncWorkflowQueueManager) Close() {
	q.persistence.Close()
}

func (q *asyncWorkflowQueueManager) Enqueue(ctx context.Context, request *EnqueueAsyncWorkflowMessageRequest) (*EnqueueAsyncWorkflowMessageResponse, error) {
	request.CurrentTimeStamp = q.timeSrc.Now()
	return q.persistence.Enqueue(ctx, request)
}

func (q *asyncWorkflowQueueManager) ReadMessages(ctx context.Context, request *ReadAsyncWorkflowMessagesRequest) (*ReadAsyncWorkflowMessagesResponse, error) {
	return q.persistence.ReadMessages(ctx, request)
}

func (q *asyncWorkflowQueueManager) UpdateAckLevel(ctx context.Context, request *UpdateAsyncWorkflowAckLevelRequest) error {
	request.CurrentTimeStamp = q.timeSrc.Now()
	return q.persistence.UpdateAckLevel(ctx, request)
}

func (q *asyncWorkflowQueueManager) GetAckLevel(ctx context.Context, request *GetAsyncWorkflowAckLevelRequest) (*GetAsyncWorkflowAckLevelResponse, error) {
	return q.persistence.GetAckLevel(ctx, request)
}

func (q *asyncWorkflowQueueManager) RangeDeleteMessages(ctx context.Context, request *RangeDeleteAsyncWorkflowMessagesRequest) error {
	return q.persistence.RangeDeleteMessages(ctx, request)
}

func (q *asyncWorkflowQueueManager) EnqueueToDLQ(ctx context.Context, request *EnqueueAsyncWorkflowMessageRequest) (*EnqueueAsyncWorkflowMessageResponse, error) {
	request.CurrentTimeStamp = q.timeSrc.Now()
	return q.persistence.EnqueueToDLQ(ctx, request)
}

func (q *asyncWorkflowQueueManager) ReadMessagesFromDLQ(ctx context.Context, request *ReadAsyncWorkflowMessagesRequest) (*ReadAsyncWorkflowMessagesResponse, error) {
	return q.persistence.ReadMessagesFromDLQ(ctx, request)
}

func (q *asyncWorkflowQueueManager) RangeDeleteMessagesFromDLQ(ctx context.Context, request *RangeDeleteAsyncWorkflowMessagesRequest) error {
	return q.persistence.RangeDeleteMessagesFromDLQ(ctx, request)
}
