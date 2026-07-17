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

package asyncworkflowqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	defaultTestGCInterval = 15 * time.Second
	testShardID           = 1
)

type newProcessorParams struct {
	Manager    persistence.AsyncWorkflowQueueManager
	QueueNames []string
	Enabled    bool
	TimeSource clock.TimeSource
}

// toList converts a []string to the []interface{} shape a ListPropertyFn returns.
func toList(names []string) []interface{} {
	out := make([]interface{}, len(names))
	for i, n := range names {
		out[i] = n
	}
	return out
}

func newProcessor(t *testing.T, params newProcessorParams) *processorImpl {
	t.Helper()
	ts := params.TimeSource
	if ts == nil {
		ts = clock.NewMockedTimeSource()
	}
	return NewProcessor(ProcessorParams{
		ShardID:    testShardID,
		Manager:    params.Manager,
		QueueNames: func(...dynamicproperties.FilterOption) []interface{} { return toList(params.QueueNames) },
		Enabled:    dynamicproperties.GetBoolPropertyFn(params.Enabled),
		Interval:   dynamicproperties.GetDurationPropertyFnFilteredByShardID(defaultTestGCInterval),
		TimeSource: ts,
		Metrics:    metrics.NewNoopMetricsClient(),
		Logger:     testlogger.New(t),
	}).(*processorImpl)
}

func TestProcessShard_WhenAckLevelNonNegative_RangeDeletesUpToAckLevel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
	})

	mgr.EXPECT().GetAckLevel(gomock.Any(), &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: "queue-a",
		ShardID:   testShardID,
	}).Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: 42}, nil)
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName:             "queue-a",
		ShardID:               testShardID,
		InclusiveEndMessageID: 42,
	}).Return(nil)

	proc.processShard(context.Background())
}

func TestProcessShard_WhenAckLevelZero_RangeDeletesUpToZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
	})

	// AckLevel == 0 means message 0 has been consumed and is eligible for GC.
	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).
		Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: 0}, nil)
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName:             "queue-a",
		ShardID:               testShardID,
		InclusiveEndMessageID: 0,
	}).Return(nil)

	proc.processShard(context.Background())
}

func TestProcessShard_WhenAckLevelEmpty_DoesNotRangeDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
	})

	// EmptyMessageID (-1): queue never consumed, nothing to GC.
	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).
		Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: constants.EmptyMessageID}, nil)
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), gomock.Any()).Times(0)

	proc.processShard(context.Background())
}

func TestProcessShard_WhenGetAckLevelFails_SkipsQueueAndContinues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a", "queue-b"},
		Enabled:    true,
	})

	// First queue errors on GetAckLevel; second queue must still be processed.
	mgr.EXPECT().GetAckLevel(gomock.Any(), &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: "queue-a", ShardID: testShardID,
	}).Return(nil, errors.New("db error"))
	mgr.EXPECT().GetAckLevel(gomock.Any(), &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: "queue-b", ShardID: testShardID,
	}).Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: 5}, nil)
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName: "queue-b", ShardID: testShardID, InclusiveEndMessageID: 5,
	}).Return(nil)

	proc.processShard(context.Background())
}

func TestProcessShard_WhenRangeDeleteFails_ContinuesToNextQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a", "queue-b"},
		Enabled:    true,
	})

	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).
		Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: 1}, nil).Times(2)
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName: "queue-a", ShardID: testShardID, InclusiveEndMessageID: 1,
	}).Return(errors.New("delete failed"))
	mgr.EXPECT().RangeDeleteMessages(gomock.Any(), &persistence.RangeDeleteAsyncWorkflowMessagesRequest{
		QueueName: "queue-b", ShardID: testShardID, InclusiveEndMessageID: 1,
	}).Return(nil)

	proc.processShard(context.Background())
}

func TestProcessShard_WhenNoQueuesConfigured_IsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: nil,
		Enabled:    true,
	})
	// No manager calls expected.
	proc.processShard(context.Background())
}

func TestProcessShard_IgnoresNonStringAndEmptyQueueNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := NewProcessor(ProcessorParams{
		ShardID: testShardID,
		Manager: mgr,
		QueueNames: func(...dynamicproperties.FilterOption) []interface{} {
			return []interface{}{"queue-a", "", 123, nil}
		},
		Enabled:    dynamicproperties.GetBoolPropertyFn(true),
		Interval:   dynamicproperties.GetDurationPropertyFnFilteredByShardID(defaultTestGCInterval),
		TimeSource: clock.NewMockedTimeSource(),
		Metrics:    metrics.NewNoopMetricsClient(),
		Logger:     testlogger.New(t),
	}).(*processorImpl)

	// Only the valid non-empty string "queue-a" is processed.
	mgr.EXPECT().GetAckLevel(gomock.Any(), &persistence.GetAsyncWorkflowAckLevelRequest{
		QueueName: "queue-a", ShardID: testShardID,
	}).Return(&persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: constants.EmptyMessageID}, nil)

	proc.processShard(context.Background())
}

func TestStart_WhenNotEnabled_SkipsProcessingButContinuesLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := clock.NewMockedTimeSource()
	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	// enabled() is false ⇒ GetAckLevel must never be called.
	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).Times(0)

	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    false,
		TimeSource: ts,
	})

	proc.Start()
	defer proc.Stop()

	// The loop always starts; wait for the first timer registration.
	ts.BlockUntil(1)
	// Advance past the interval — enabled() returns false, so GetAckLevel must not be called.
	ts.Advance(defaultTestGCInterval)
	// Wait for the timer to be reset, confirming the loop ran and continued.
	ts.BlockUntil(1)
	// ctrl.Finish() verifies GetAckLevel was called 0 times.
}

func TestStart_WhenEnabled_CallsProcessShardOnInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := clock.NewMockedTimeSource()
	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	processed := make(chan struct{}, 1)
	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.GetAsyncWorkflowAckLevelRequest) (*persistence.GetAsyncWorkflowAckLevelResponse, error) {
			select {
			case processed <- struct{}{}:
			default:
			}
			return &persistence.GetAsyncWorkflowAckLevelResponse{AckLevel: constants.EmptyMessageID}, nil
		}).AnyTimes()

	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
		TimeSource: ts,
	})

	proc.Start()
	defer proc.Stop()

	ts.BlockUntil(1)
	ts.Advance(defaultTestGCInterval)

	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for processShard to be called by the background loop")
	}
}

func TestStartStop_ShouldBeIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
	})

	proc.Start()
	proc.Start() // second call must be a no-op
	proc.Stop()
	proc.Stop() // second call must be a no-op
}

func TestStop_WhenStoreRespectsContextCancellation_ReturnsPromptly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := clock.NewMockedTimeSource()
	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	inGetAckLevel := make(chan struct{}, 1)
	mgr.EXPECT().GetAckLevel(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *persistence.GetAsyncWorkflowAckLevelRequest) (*persistence.GetAsyncWorkflowAckLevelResponse, error) {
			select {
			case inGetAckLevel <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a"},
		Enabled:    true,
		TimeSource: ts,
	})

	proc.Start()

	ts.BlockUntil(1)
	ts.Advance(defaultTestGCInterval)

	select {
	case <-inGetAckLevel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for GetAckLevel to be called")
	}

	stopDone := make(chan struct{})
	go func() {
		proc.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return promptly after context cancellation")
	}
}

func TestProcessShard_WhenContextCancelled_StopsBeforeNextQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := persistence.NewMockAsyncWorkflowQueueManager(ctrl)
	proc := newProcessor(t, newProcessorParams{
		Manager:    mgr,
		QueueNames: []string{"queue-a", "queue-b"},
		Enabled:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// No manager calls: the cancelled context short-circuits before the first queue.
	proc.processShard(ctx)
	assert.Error(t, ctx.Err())
}
