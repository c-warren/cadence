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

package taskdlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

// newMockTask creates a mock persistence.Task whose GetTaskKey returns an immediate key for taskID.
// TODO(c-warren): Once the persistence layer is implemented, this can be a generated mock of the history task queue
func newMockTask(ctrl *gomock.Controller, taskID int64) *persistence.MockTask {
	t := persistence.NewMockTask(ctrl)
	t.EXPECT().GetTaskKey().Return(persistence.NewImmediateTaskKey(taskID)).AnyTimes()
	return t
}

func setupProcessor(t *testing.T, ctrl *gomock.Controller) (*processorImpl, *MockStore, *MockTaskExecutor) {
	t.Helper()
	store := NewMockStore(ctrl)
	executor := NewMockTaskExecutor(ctrl)
	proc := NewProcessor(
		1,
		store,
		map[int]TaskExecutor{
			persistence.HistoryTaskCategoryIDTransfer: executor,
		},
		10,
		dynamicproperties.GetDurationPropertyFn(defaultProcessingInterval),
		clock.NewMockedTimeSource(),
		testlogger.New(t),
	)
	return &proc, store, executor
}

func baseAckLevel(shardID int) AckLevel {
	return AckLevel{
		ShardID:               shardID,
		DomainID:              "test-domain",
		ClusterAttributeScope: "scope",
		ClusterAttributeName:  "name",
		TaskType:              persistence.HistoryTaskCategoryIDTransfer,
		AckLevelVisibilityTS:  time.Unix(0, 0).UTC(),
		AckLevelTaskID:        -1,
	}
}

// --- ProcessShard tests ---

func TestProcessShard_WhenNoAckLevels_ReturnsNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, _ := setupProcessor(t, ctrl)
	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return(nil, nil)

	assert.NoError(t, proc.ProcessShard(context.Background()))
}

func TestProcessShard_WhenGetAckLevelsFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, _ := setupProcessor(t, ctrl)
	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return(nil, errors.New("db error"))

	err := proc.ProcessShard(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "db error")
}

func TestProcessShard_WhenAllTasksSucceed_AdvancesAckLevel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, executor := setupProcessor(t, ctrl)
	al := baseAckLevel(1)
	task0 := newMockTask(ctrl, 0)
	task1 := newMockTask(ctrl, 1)
	tasks := []persistence.Task{task0, task1}

	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return([]AckLevel{al}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(GetTasksResponse{Tasks: tasks}, nil)
	executor.EXPECT().Execute(gomock.Any(), tasks[0]).Return(nil)
	executor.EXPECT().Execute(gomock.Any(), tasks[1]).Return(nil)
	store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().DeleteTasks(gomock.Any(), gomock.Any()).Return(nil)

	assert.NoError(t, proc.ProcessShard(context.Background()))
}

func TestProcessShard_WhenTasksSpanMultiplePages_ProcessesAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, executor := setupProcessor(t, ctrl)
	al := baseAckLevel(1)
	page1Token := []byte("token1")
	task0 := newMockTask(ctrl, 0)
	task1 := newMockTask(ctrl, 1)

	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return([]AckLevel{al}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(
		GetTasksResponse{Tasks: []persistence.Task{task0}, NextPageToken: page1Token}, nil,
	)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(
		GetTasksResponse{Tasks: []persistence.Task{task1}}, nil,
	)
	executor.EXPECT().Execute(gomock.Any(), task0).Return(nil)
	executor.EXPECT().Execute(gomock.Any(), task1).Return(nil)
	store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().DeleteTasks(gomock.Any(), gomock.Any()).Return(nil)

	assert.NoError(t, proc.ProcessShard(context.Background()))
}

func TestProcessShard_WhenExecutionFailsMidPage_AdvancesAckLevelToLastSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, executor := setupProcessor(t, ctrl)
	al := baseAckLevel(1)
	key0 := persistence.NewImmediateTaskKey(0)
	task0 := persistence.NewMockTask(ctrl)
	task0.EXPECT().GetTaskKey().Return(key0)
	task1 := persistence.NewMockTask(ctrl) // GetTaskKey never called: execution fails before it
	execErr := errors.New("execute failed")

	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return([]AckLevel{al}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(
		GetTasksResponse{Tasks: []persistence.Task{task0, task1}}, nil,
	)
	executor.EXPECT().Execute(gomock.Any(), task0).Return(nil)
	executor.EXPECT().Execute(gomock.Any(), task1).Return(execErr)
	store.EXPECT().UpdateAckLevel(gomock.Any(), UpdateAckLevelRequest{
		ShardID:               al.ShardID,
		DomainID:              al.DomainID,
		ClusterAttributeScope: al.ClusterAttributeScope,
		ClusterAttributeName:  al.ClusterAttributeName,
		TaskType:              al.TaskType,
		AckLevelVisibilityTS:  key0.GetScheduledTime(),
		AckLevelTaskID:        key0.GetTaskID(),
	}).Return(nil)
	store.EXPECT().DeleteTasks(gomock.Any(), gomock.Any()).Return(nil)

	err := proc.ProcessShard(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, execErr)
}

func TestProcessShard_WhenFirstTaskFails_DoesNotAdvanceAckLevel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, executor := setupProcessor(t, ctrl)
	al := baseAckLevel(1)
	task0 := persistence.NewMockTask(ctrl)
	execErr := errors.New("execute failed")

	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return([]AckLevel{al}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(
		GetTasksResponse{Tasks: []persistence.Task{task0}}, nil,
	)
	executor.EXPECT().Execute(gomock.Any(), task0).Return(execErr)

	err := proc.ProcessShard(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, execErr)
}

func TestProcessShard_WhenOnePartitionFails_ReturnsErrorButProcessesRemainingPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, _ := setupProcessor(t, ctrl)
	al1 := baseAckLevel(1)
	al2 := AckLevel{
		ShardID:               1,
		DomainID:              "other-domain",
		ClusterAttributeScope: "scope",
		ClusterAttributeName:  "name",
		TaskType:              persistence.HistoryTaskCategoryIDTransfer,
		AckLevelVisibilityTS:  time.Unix(0, 0).UTC(),
		AckLevelTaskID:        -1,
	}
	getTasksErr := errors.New("partition error")

	store.EXPECT().GetAckLevels(gomock.Any(), 1).Return([]AckLevel{al1, al2}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(GetTasksResponse{}, getTasksErr)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(GetTasksResponse{}, nil)

	err := proc.ProcessShard(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, getTasksErr)
}

// --- ProcessPartition tests ---

func TestProcessPartition_WhenGetAckLevelsFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, store, _ := setupProcessor(t, ctrl)
	storeErr := errors.New("partition error")
	store.EXPECT().
		GetAckLevelsForPartition(gomock.Any(), 1, "d", "s", "n").
		Return(nil, storeErr)

	err := proc.ProcessPartition(context.Background(), "d", "s", "n")
	require.Error(t, err)
	assert.ErrorContains(t, err, "partition error")
}

func TestProcessPartition_WhenMultipleTaskTypes_ProcessesAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	transferExecutor := NewMockTaskExecutor(ctrl)
	timerExecutor := NewMockTaskExecutor(ctrl)
	store := NewMockStore(ctrl)
	proc := NewProcessor(
		1,
		store,
		map[int]TaskExecutor{
			persistence.HistoryTaskCategoryIDTransfer: transferExecutor,
			persistence.HistoryTaskCategoryIDTimer:    timerExecutor,
		},
		10,
		dynamicproperties.GetDurationPropertyFn(defaultProcessingInterval),
		clock.NewMockedTimeSource(),
		testlogger.New(t),
	)

	transferAL := AckLevel{
		ShardID: 1, DomainID: "d", ClusterAttributeScope: "s", ClusterAttributeName: "n",
		TaskType: persistence.HistoryTaskCategoryIDTransfer,
		AckLevelVisibilityTS: time.Unix(0, 0).UTC(), AckLevelTaskID: -1,
	}
	timerAL := AckLevel{
		ShardID: 1, DomainID: "d", ClusterAttributeScope: "s", ClusterAttributeName: "n",
		TaskType: persistence.HistoryTaskCategoryIDTimer,
		AckLevelVisibilityTS: time.Unix(0, 0).UTC(), AckLevelTaskID: -1,
	}

	store.EXPECT().
		GetAckLevelsForPartition(gomock.Any(), 1, "d", "s", "n").
		Return([]AckLevel{transferAL, timerAL}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(GetTasksResponse{}, nil)
	store.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(GetTasksResponse{}, nil)

	assert.NoError(t, proc.ProcessPartition(context.Background(), "d", "s", "n"))
}

// --- advanceAckLevel tests ---

func TestAdvanceAckLevel(t *testing.T) {
	tests := []struct {
		name               string
		updateErr          error
		deleteErr          error
		expectDeleteCalled bool
		expectErr          bool
	}{
		{
			name:               "when UpdateAckLevel fails, returns error without calling DeleteTasks",
			updateErr:          errors.New("update failed"),
			expectDeleteCalled: false,
			expectErr:          true,
		},
		{
			name:               "when DeleteTasks fails, logs and returns nil",
			expectDeleteCalled: true,
			deleteErr:          errors.New("delete failed"),
			expectErr:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			proc, store, _ := setupProcessor(t, ctrl)
			al := baseAckLevel(1)
			newKey := persistence.NewImmediateTaskKey(5)

			store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).Return(tc.updateErr)
			if tc.expectDeleteCalled {
				store.EXPECT().DeleteTasks(gomock.Any(), gomock.Any()).Return(tc.deleteErr)
			}

			err := proc.advanceAckLevel(context.Background(), al, newKey)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Start / Stop tests ---

func TestStartStop_ShouldBeIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := NewMockStore(ctrl)
	proc := NewProcessor(
		1,
		store,
		map[int]TaskExecutor{},
		10,
		dynamicproperties.GetDurationPropertyFn(defaultProcessingInterval),
		clock.NewMockedTimeSource(),
		testlogger.New(t),
	)

	proc.Start()
	proc.Start() // second call must be a no-op
	proc.Stop()
	proc.Stop() // second call must be a no-op
}

func TestStart_ShouldCallProcessShardOnInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := clock.NewMockedTimeSource()
	store := NewMockStore(ctrl)
	processed := make(chan struct{}, 1)
	store.EXPECT().GetAckLevels(gomock.Any(), 1).DoAndReturn(func(_ context.Context, _ int) ([]AckLevel, error) {
		select {
		case processed <- struct{}{}:
		default:
		}
		return nil, nil
	}).AnyTimes()

	proc := NewProcessor(
		1,
		store,
		map[int]TaskExecutor{},
		10,
		dynamicproperties.GetDurationPropertyFn(defaultProcessingInterval),
		ts,
		testlogger.New(t),
	)

	proc.Start()
	defer proc.Stop()

	ts.BlockUntil(1)
	ts.Advance(defaultProcessingInterval)

	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ProcessShard to be called by the background loop")
	}
}
