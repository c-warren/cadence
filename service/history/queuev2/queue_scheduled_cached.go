package queuev2

import (
	"context"

	"github.com/uber/cadence/common/persistence"
	hcommon "github.com/uber/cadence/service/history/common"
)

type cachedScheduledQueue struct {
	*scheduledQueue
	reader CachedQueueReader
}

func newCachedScheduledQueue(inner *scheduledQueue, reader CachedQueueReader) Queue {
	// Wrap the queue state update to propagate min read level across all virtual queues
	// to CachedQueueReader each time the ack level is updated
	originalUpdateFn := inner.base.updateQueueStateFn
	inner.base.updateQueueStateFn = func(ctx context.Context) {
		originalUpdateFn(ctx)
		// MaximumHistoryTaskKey means no slices — fall back to ack level.
		readLevel := inner.base.virtualQueueManager.GetMinReadLevel()
		if readLevel.Equal(persistence.MaximumHistoryTaskKey) {
			readLevel = inner.base.exclusiveAckLevel
		}
		reader.UpdateReadLevel(readLevel)
	}

	return &cachedScheduledQueue{
		scheduledQueue: inner,
		reader:         reader,
	}
}

func (q *cachedScheduledQueue) NotifyNewTask(clusterName string, info *hcommon.NotifyTaskInfo) {
	if info.PersistenceError {
		q.reader.Clear()
	} else {
		q.reader.Inject(info.Tasks)
	}
	q.scheduledQueue.NotifyNewTask(clusterName, info)
}

func (q *cachedScheduledQueue) Start() {
	q.reader.Start()
	q.scheduledQueue.Start()
}

func (q *cachedScheduledQueue) Stop() {
	q.scheduledQueue.Stop()
	q.reader.Stop()
}
