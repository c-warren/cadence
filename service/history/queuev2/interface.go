package queuev2

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	hcommon "github.com/uber/cadence/service/history/common"
	"github.com/uber/cadence/service/history/queue"
)

type (
	Queue interface {
		common.Daemon
		Category() persistence.HistoryTaskCategory
		NotifyNewTask(string, *hcommon.NotifyTaskInfo)

		FailoverDomain(map[string]struct{})
		HandleAction(context.Context, string, *queue.Action) (*queue.ActionResult, error)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}
)
