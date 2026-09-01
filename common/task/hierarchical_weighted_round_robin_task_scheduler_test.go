package task

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
)

func TestHierarchicalWeightedRoundRobinTaskScheduler_SchedulerContract(t *testing.T) {
	controller := gomock.NewController(t)

	realProcessor := NewParallelTaskProcessor(
		testlogger.New(t),
		metrics.NewClient(tally.NoopScope, metrics.Common, metrics.MigrationConfig{}),
		&ParallelTaskProcessorOptions{
			QueueSize:   1,
			WorkerCount: dynamicproperties.GetIntPropertyFn(1),
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	)

	// Create hierarchical scheduler with string keys based on priority
	scheduler, err := NewHierarchicalWeightedRoundRobinTaskScheduler(
		testlogger.New(t),
		metrics.NewClient(tally.NoopScope, metrics.Common, metrics.MigrationConfig{}),
		clock.NewMockedTimeSource(),
		realProcessor,
		&HierarchicalWeightedRoundRobinTaskPoolOptions[string, PriorityTask]{
			BufferSize: 1000,
			TaskToWeightedKeysFn: func(task PriorityTask) []WeightedKey[string] {
				priority := task.Priority()
				// Create a simple hierarchy: group -> priority
				// Groups based on priority ranges with different weights
				var group string
				var groupWeight int
				if priority == 0 {
					group = "group0"
					groupWeight = 3
				} else if priority == 1 {
					group = "group1"
					groupWeight = 2
				} else {
					group = "group2"
					groupWeight = 1
				}

				// Second level: individual priority
				priorityKey := string(rune('0' + priority))
				priorityWeight := 3 - priority
				if priorityWeight < 1 {
					priorityWeight = 1
				}

				return []WeightedKey[string]{
					{Key: group, Weight: groupWeight},
					{Key: priorityKey, Weight: priorityWeight},
				}
			},
		},
	)
	require.NoError(t, err)

	// Reuse the existing testSchedulerContract function
	testSchedulerContract(require.New(t), controller, scheduler, realProcessor)
}
