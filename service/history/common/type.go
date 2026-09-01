package common

import (
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/events"
)

type (
	// NotifyTaskInfo defines the info of task notification
	NotifyTaskInfo struct {
		ExecutionInfo    *persistence.WorkflowExecutionInfo
		Tasks            []persistence.Task
		VersionHistories *persistence.VersionHistories
		Activities       map[int64]*persistence.ActivityInfo
		History          events.PersistedBlobs
		PersistenceError bool

		// TODO: remove this when queuev1 is replaced by queuev2
		// ClusterCurrentTimes is not nil for timer tasks
		// It contains current times per cluster of all tasks in this notification
		ClusterCurrentTimes map[string]time.Time
	}
)
