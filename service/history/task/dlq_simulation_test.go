package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
)

// TestDLQ_Simulation_ShortDiscardDelay simulates rapid task discard with short delay
func TestDLQ_Simulation_ShortDiscardDelay(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
	defer dlqManager.Close()

	ctx := context.Background()

	// Configure short discard delay (1 second for simulation)
	discardDelay := 1 * time.Second

	// Simulate multiple tasks being discarded
	numTasks := 10
	for i := 0; i < numTasks; i++ {
		taskInfo := &persistence.TransferTaskInfo{
			DomainID:            "sim-domain",
			WorkflowID:          fmt.Sprintf("wf-%d", i),
			RunID:               "run1",
			TaskID:              int64(100 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Now().Add(-discardDelay - time.Second),
		}

		taskPayload, err := json.Marshal(taskInfo)
		require.NoError(t, err)

		// Simulate standby task being discarded after delay
		time.Sleep(10 * time.Millisecond) // Small delay between tasks

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:               1,
			DomainID:              "sim-domain",
			ClusterAttributeScope: "sim-scope",
			ClusterAttributeName:  "sim-attr",
			WorkflowID:            fmt.Sprintf("wf-%d", i),
			RunID:                 "run1",
			TaskID:                int64(100 + i),
			VisibilityTimestamp:   taskInfo.VisibilityTimestamp.UnixNano(),
			TaskType:              persistence.TransferTaskTypeActivityTask,
			TaskPayload:           taskPayload,
			Version:               5,
		})
		require.NoError(t, err)
	}

	// Verify all tasks in DLQ
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
		PageSize:              100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, numTasks)

	// Simulate failover
	// Note: In the current POC, the processor doesn't call Execute,
	// so this counter won't increment. We track by DLQ removal instead.
	executedCount := 0
	mockExecutor := &mockTaskExecutor{
		executeFn: func(task Task) error {
			executedCount++
			return nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err = processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:               1,
		DomainID:              "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
	})
	require.NoError(t, err)

	// Verify DLQ is empty (tasks were processed and removed)
	resp, err = dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "sim-domain",
		ClusterAttributeScope: "sim-scope",
		ClusterAttributeName:  "sim-attr",
		PageSize:              100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0)
}

// TestDLQ_Simulation_ChaosInjection tests DLQ with error injection
// Note: This test demonstrates the framework for chaos testing, but the current POC
// doesn't actually execute tasks, so all tasks will be "successfully" processed (deserialized).
// When full task execution is implemented, this test will verify error handling.
func TestDLQ_Simulation_ChaosInjection(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue tasks
	numTasks := 20
	for i := 0; i < numTasks; i++ {
		taskInfo := &persistence.TransferTaskInfo{
			DomainID:            "chaos-domain",
			WorkflowID:          fmt.Sprintf("wf-%d", i),
			RunID:               "run1",
			TaskID:              int64(200 + i),
			TaskType:            persistence.TransferTaskTypeActivityTask,
			Version:             5,
			VisibilityTimestamp: time.Now(),
		}

		taskPayload, err := json.Marshal(taskInfo)
		require.NoError(t, err)

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:               1,
			DomainID:              "chaos-domain",
			ClusterAttributeScope: "chaos-scope",
			ClusterAttributeName:  "chaos-attr",
			WorkflowID:            fmt.Sprintf("wf-%d", i),
			RunID:                 "run1",
			TaskID:                int64(200 + i),
			VisibilityTimestamp:   taskInfo.VisibilityTimestamp.UnixNano(),
			TaskType:              persistence.TransferTaskTypeActivityTask,
			TaskPayload:           taskPayload,
			Version:               5,
		})
		require.NoError(t, err)
	}

	// Mock executor that could inject errors (not used in POC since tasks aren't actually executed)
	mockExecutor := &mockTaskExecutor{
		executeFn: func(task Task) error {
			// Chaos injection logic would go here
			// For now, just succeed since the processor doesn't call this
			return nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)
	err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
		ShardID:               1,
		DomainID:              "chaos-domain",
		ClusterAttributeScope: "chaos-scope",
		ClusterAttributeName:  "chaos-attr",
	})
	require.NoError(t, err)

	// In the POC, all tasks are successfully deserialized and removed
	resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
		ShardID:               1,
		DomainID:              "chaos-domain",
		ClusterAttributeScope: "chaos-scope",
		ClusterAttributeName:  "chaos-attr",
		PageSize:              100,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 0, "All tasks should be processed and removed in POC")
}

// TestDLQ_Simulation_ConcurrentFailovers tests concurrent failover processing
func TestDLQ_Simulation_ConcurrentFailovers(t *testing.T) {
	logger := testlogger.New(t)
	dlqManager := persistence.NewInMemoryStandbyTaskDLQManager(logger)
	defer dlqManager.Close()

	ctx := context.Background()

	// Enqueue tasks for multiple domains and cluster attributes
	numDomains := 5
	numAttrs := 3
	tasksPerCombo := 10

	for d := 0; d < numDomains; d++ {
		for a := 0; a < numAttrs; a++ {
			for i := 0; i < tasksPerCombo; i++ {
				taskInfo := &persistence.TransferTaskInfo{
					DomainID:            fmt.Sprintf("domain-%d", d),
					WorkflowID:          fmt.Sprintf("wf-%d-%d-%d", d, a, i),
					RunID:               "run1",
					TaskID:              int64(d*1000 + a*100 + i),
					TaskType:            persistence.TransferTaskTypeActivityTask,
					Version:             5,
					VisibilityTimestamp: time.Now(),
				}

				taskPayload, err := json.Marshal(taskInfo)
				require.NoError(t, err)

				err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
					ShardID:               1,
					DomainID:              fmt.Sprintf("domain-%d", d),
					ClusterAttributeScope: "scope1",
					ClusterAttributeName:  fmt.Sprintf("attr-%d", a),
					WorkflowID:            fmt.Sprintf("wf-%d-%d-%d", d, a, i),
					RunID:                 "run1",
					TaskID:                int64(d*1000 + a*100 + i),
					VisibilityTimestamp:   taskInfo.VisibilityTimestamp.UnixNano(),
					TaskType:              persistence.TransferTaskTypeActivityTask,
					TaskPayload:           taskPayload,
					Version:               5,
				})
				require.NoError(t, err)
			}
		}
	}

	// Process failovers concurrently
	var wg sync.WaitGroup

	// Mock executor (not called in POC, but implements interface)
	mockExecutor := &mockTaskExecutor{
		executeFn: func(task Task) error {
			return nil
		},
	}

	processor := NewStandbyTaskDLQProcessor(dlqManager, mockExecutor, logger)

	for d := 0; d < numDomains; d++ {
		for a := 0; a < numAttrs; a++ {
			wg.Add(1)
			go func(domainIdx, attrIdx int) {
				defer wg.Done()
				err := processor.ProcessFailover(ctx, &ProcessFailoverRequest{
					ShardID:               1,
					DomainID:              fmt.Sprintf("domain-%d", domainIdx),
					ClusterAttributeScope: "scope1",
					ClusterAttributeName:  fmt.Sprintf("attr-%d", attrIdx),
				})
				require.NoError(t, err)
			}(d, a)
		}
	}

	wg.Wait()

	// Verify all tasks were processed by checking DLQ is empty for all combinations
	for d := 0; d < numDomains; d++ {
		for a := 0; a < numAttrs; a++ {
			resp, err := dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
				ShardID:               1,
				DomainID:              fmt.Sprintf("domain-%d", d),
				ClusterAttributeScope: "scope1",
				ClusterAttributeName:  fmt.Sprintf("attr-%d", a),
				PageSize:              100,
			})
			require.NoError(t, err)
			require.Len(t, resp.Tasks, 0, "DLQ should be empty for domain-%d, attr-%d", d, a)
		}
	}
}

// mockTaskExecutor is a simple mock for testing DLQ processor
// Note: The DLQ processor doesn't actually call Execute in the current POC implementation,
// but we implement the interface properly for future use
type mockTaskExecutor struct {
	executeFn func(Task) error
}

func (m *mockTaskExecutor) Execute(task Task) (ExecuteResponse, error) {
	if m.executeFn != nil {
		err := m.executeFn(task)
		return ExecuteResponse{IsActiveTask: true}, err
	}
	return ExecuteResponse{IsActiveTask: true}, nil
}

func (m *mockTaskExecutor) Stop() {
	// No-op for mock
}
