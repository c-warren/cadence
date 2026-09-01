package queue

import (
	"testing"

	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/history/workflowcache"
	"github.com/uber/cadence/service/worker/archiver"
)

func TestTransferQueueFactory(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctrl := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		t, ctrl, &persistence.ShardInfo{
			ShardID:          10,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		config.NewForTest())
	defer mockShard.Finish(t)

	mockProcessor := task.NewMockProcessor(ctrl)
	mockArchiver := archiver.NewMockClient(ctrl)
	mockInvariant := invariant.NewMockInvariant(ctrl)
	mockWorkflowCache := workflowcache.NewMockWFCache(ctrl)

	f := NewTransferQueueFactory(mockProcessor, mockArchiver, mockWorkflowCache)

	processor := f.CreateQueue(mockShard, execution.NewCache(mockShard), mockInvariant)

	if processor == nil {
		t.Error("NewTransferQueueProcessor returned nil")
	}
}

func TestTimerQueueFactory(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctrl := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		t, ctrl, &persistence.ShardInfo{
			ShardID:          10,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		config.NewForTest())
	defer mockShard.Finish(t)

	mockProcessor := task.NewMockProcessor(ctrl)
	mockArchiver := archiver.NewMockClient(ctrl)
	mockInvariant := invariant.NewMockInvariant(ctrl)

	f := NewTimerQueueFactory(mockProcessor, mockArchiver)
	processor := f.CreateQueue(mockShard, execution.NewCache(mockShard), mockInvariant)

	if processor == nil {
		t.Error("NewTimerQueueProcessor returned nil")
	}
}
