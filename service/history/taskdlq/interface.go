//go:generate mockgen -package $GOPACKAGE -destination interface_mock.go github.com/uber/cadence/service/history/taskdlq TaskReinjector,Processor

package taskdlq

import (
	"context"

	"github.com/uber/cadence/common/persistence"
)

// TaskReinjector writes DLQ tasks back into the executions table with fresh task IDs.
// Implemented by shard.Context.ReinjectHistoryTasks.
type TaskReinjector interface {
	ReinjectHistoryTasks(ctx context.Context, tasks []persistence.Task) error
}
