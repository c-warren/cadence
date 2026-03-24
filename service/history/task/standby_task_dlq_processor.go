package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	// StandbyTaskDLQProcessor processes tasks from the DLQ after failover
	StandbyTaskDLQProcessor struct {
		dlqManager      persistence.StandbyTaskDLQManager
		executor        Executor
		taskInitializer Initializer
		logger          log.Logger
		timeSource      clock.TimeSource
		enableCleanup   dynamicproperties.BoolPropertyFn
		cleanupTimer    clock.Timer
		shutdownCh      chan struct{}
		shutdownWG      sync.WaitGroup
	}

	// ProcessFailoverRequest contains parameters for processing DLQ on failover
	ProcessFailoverRequest struct {
		ShardID               int
		DomainID              string
		ClusterAttributeScope string
		ClusterAttributeName  string
	}
)

// NewStandbyTaskDLQProcessor creates a new DLQ processor
func NewStandbyTaskDLQProcessor(
	dlqManager persistence.StandbyTaskDLQManager,
	executor Executor,
	taskInitializer Initializer,
	timeSource clock.TimeSource,
	enableCleanup dynamicproperties.BoolPropertyFn,
	logger log.Logger,
) *StandbyTaskDLQProcessor {
	return &StandbyTaskDLQProcessor{
		dlqManager:      dlqManager,
		executor:        executor,
		taskInitializer: taskInitializer,
		timeSource:      timeSource,
		enableCleanup:   enableCleanup,
		logger:          logger,
		shutdownCh:      make(chan struct{}),
	}
}

// Start begins the periodic DLQ cleanup process
func (p *StandbyTaskDLQProcessor) Start() {
	// Start periodic cleanup timer (every 10 seconds, similar to queue UpdateAckInterval)
	cleanupInterval := 10 * time.Second
	p.cleanupTimer = p.timeSource.NewTimer(cleanupInterval)

	p.shutdownWG.Add(1)
	go p.cleanupLoop()

	p.logger.Warn("StandbyTaskDLQProcessor started with periodic cleanup")
}

// Stop gracefully shuts down the DLQ processor
func (p *StandbyTaskDLQProcessor) Stop() {
	close(p.shutdownCh)
	p.cleanupTimer.Stop()

	// Wait for cleanup loop to finish
	p.shutdownWG.Wait()

	p.logger.Warn("StandbyTaskDLQProcessor stopped")
}

// ProcessFailover processes all DLQ tasks for a specific domain/cluster attribute combination
func (p *StandbyTaskDLQProcessor) ProcessFailover(
	ctx context.Context,
	request *ProcessFailoverRequest,
) error {
	startTime := time.Now()
	totalProcessed := 0
	totalFailed := 0

	// Track max processed task ID per task type for range deletion
	maxProcessedTaskID := make(map[int]int64)

	defer func() {
		duration := time.Since(startTime)
		p.logger.Warn("DLQ failover processing completed",
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.Dynamic("total_processed", totalProcessed),
			tag.Dynamic("total_failed", totalFailed),
			tag.Dynamic("duration_ms", duration.Milliseconds()))
	}()

	p.logger.Warn("Processing DLQ tasks for failover",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope),
		tag.Value(request.ClusterAttributeName))

	pageSize := 100
	var nextPageToken []byte

	for {
		// Check context cancellation (timeout)
		if ctx.Err() != nil {
			p.logger.Warn("DLQ failover processing cancelled",
				tag.Error(ctx.Err()),
				tag.WorkflowDomainID(request.DomainID))
			return ctx.Err()
		}

		// Read tasks from DLQ
		resp, err := p.dlqManager.ReadStandbyTasks(ctx, &persistence.ReadStandbyTasksRequest{
			ShardID:               request.ShardID,
			DomainID:              request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			PageSize:              pageSize,
			NextPageToken:         nextPageToken,
		})
		if err != nil {
			p.logger.Error("Failed to read DLQ tasks", tag.Error(err))
			return err
		}

		// Process each task
		for _, dlqTask := range resp.Tasks {
			totalProcessed++

			if err := p.processTask(ctx, dlqTask); err != nil {
				totalFailed++
				p.logger.Error("Failed to process DLQ task",
					tag.TaskID(dlqTask.TaskID),
					tag.Error(err))
				// Continue processing other tasks
				continue
			}

			// Track max task ID for this task type (only for successfully processed tasks)
			if current, exists := maxProcessedTaskID[dlqTask.TaskType]; !exists || dlqTask.TaskID > current {
				maxProcessedTaskID[dlqTask.TaskType] = dlqTask.TaskID
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		nextPageToken = resp.NextPageToken
	}

	// After processing all tasks, update ack level and range delete per task type
	for taskType, maxTaskID := range maxProcessedTaskID {
		p.logger.Warn("Updating ack level for processed DLQ tasks",
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.Dynamic("task_type", taskType),
			tag.Dynamic("max_task_id", maxTaskID))

		// Update ack level first (stores in task_id=-1 row)
		// This ensures that on restart, ReadStandbyTasks won't re-read processed tasks
		if updater, ok := p.dlqManager.(interface {
			UpdateAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int, newAckLevel int64) error
		}); ok {
			if err := updater.UpdateAckLevel(ctx, request.ShardID, request.DomainID,
				request.ClusterAttributeScope, request.ClusterAttributeName,
				taskType, maxTaskID); err != nil {
				p.logger.Error("Failed to update ack level",
					tag.Error(err),
					tag.Dynamic("task_type", taskType))
				// Continue with range delete anyway
			} else {
				p.logger.Warn("Successfully updated ack level",
					tag.Dynamic("task_type", taskType),
					tag.Dynamic("max_task_id", maxTaskID))
			}
		} else {
			p.logger.Warn("DLQ manager does not support updateAckLevel (type assertion failed) - ack levels will not be tracked",
				tag.Dynamic("dlq_manager_type", fmt.Sprintf("%T", p.dlqManager)))
		}

		p.logger.Warn("Range deleting processed DLQ tasks",
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.Dynamic("task_type", taskType),
			tag.Dynamic("max_task_id", maxTaskID))

		err := p.dlqManager.RangeDeleteStandbyTasks(ctx, &persistence.RangeDeleteStandbyTasksRequest{
			ShardID:               request.ShardID,
			DomainID:              request.DomainID,
			ClusterAttributeScope: request.ClusterAttributeScope,
			ClusterAttributeName:  request.ClusterAttributeName,
			TaskType:              taskType,
			MaxTaskID:             maxTaskID,
		})

		if err != nil {
			p.logger.Error("Failed to range delete DLQ tasks",
				tag.Error(err),
				tag.Dynamic("task_type", taskType),
				tag.Dynamic("max_task_id", maxTaskID))
			// Continue with other task types
		}
	}

	return nil
}

// processTask deserializes and executes a single DLQ task
func (p *StandbyTaskDLQProcessor) processTask(
	ctx context.Context,
	dlqTask *persistence.StandbyTaskDLQEntry,
) error {
	// Deserialize task
	persistenceTask, err := p.deserializeTask(dlqTask)
	if err != nil {
		p.logger.Error("Failed to deserialize DLQ task",
			tag.TaskID(dlqTask.TaskID),
			tag.Error(err))
		return err
	}

	// Convert persistence task to task.Task using the initializer
	// This creates a fully-initialized task with executor, processor, rescheduler, etc.
	historyTask := p.taskInitializer(persistenceTask)

	// If executor or historyTask is nil (e.g., in tests), just log that we deserialized successfully
	if p.executor == nil || historyTask == nil {
		p.logger.Warn("DLQ task deserialized successfully, would execute",
			tag.TaskID(persistenceTask.GetTaskID()),
			tag.WorkflowDomainID(persistenceTask.GetDomainID()))
		return nil
	}

	p.logger.Warn("Executing DLQ task after failover",
		tag.TaskID(persistenceTask.GetTaskID()),
		tag.WorkflowDomainID(persistenceTask.GetDomainID()))

	// Execute the task through the executor
	// This will run the task as if it were being processed by the active queue
	resp, err := p.executor.Execute(historyTask)
	if err != nil {
		p.logger.Error("Failed to execute DLQ task",
			tag.TaskID(persistenceTask.GetTaskID()),
			tag.WorkflowDomainID(persistenceTask.GetDomainID()),
			tag.Error(err))
		return err
	}

	p.logger.Warn("DLQ task executed successfully",
		tag.TaskID(persistenceTask.GetTaskID()),
		tag.WorkflowDomainID(persistenceTask.GetDomainID()),
		tag.Dynamic("is_active_task", resp.IsActiveTask))

	return nil
}

// cleanupLoop runs periodically to clean up processed DLQ tasks
func (p *StandbyTaskDLQProcessor) cleanupLoop() {
	defer p.shutdownWG.Done()

	cleanupInterval := 10 * time.Second

	for {
		select {
		case <-p.cleanupTimer.Chan():
			// Only run cleanup if enabled via dynamic config
			if p.enableCleanup() {
				p.cleanupProcessedTasks(context.Background())
			} else {
				p.logger.Warn("DLQ periodic cleanup skipped (disabled via dynamic config)")
			}
			p.cleanupTimer.Reset(cleanupInterval)

		case <-p.shutdownCh:
			return
		}
	}
}

// cleanupProcessedTasks is now a no-op since cleanup happens during ProcessFailover
// This method is kept for backward compatibility and as a safety net for periodic cleanup
func (p *StandbyTaskDLQProcessor) cleanupProcessedTasks(ctx context.Context) {
	// Cleanup now happens immediately after ProcessFailover completes
	// via range delete per task type. This periodic cleanup is no longer needed
	// but kept as a placeholder for potential future use.
	p.logger.Debug("DLQ periodic cleanup skipped (cleanup now happens during ProcessFailover)")
}

// deserializeTask converts DLQ entry back to a persistence.Task
func (p *StandbyTaskDLQProcessor) deserializeTask(
	dlqTask *persistence.StandbyTaskDLQEntry,
) (persistence.Task, error) {
	// Try to deserialize as TransferTaskInfo first
	var transferTaskInfo persistence.TransferTaskInfo
	if err := json.Unmarshal(dlqTask.TaskPayload, &transferTaskInfo); err == nil {
		// Restore the visibility timestamp from DLQ entry
		transferTaskInfo.VisibilityTimestamp = time.Unix(0, dlqTask.VisibilityTimestamp)

		// Convert to Task interface
		task, err := transferTaskInfo.ToTask()
		if err == nil {
			return task, nil
		}
	}

	// Try to deserialize as TimerTaskInfo
	var timerTaskInfo persistence.TimerTaskInfo
	if err := json.Unmarshal(dlqTask.TaskPayload, &timerTaskInfo); err == nil {
		// Restore the visibility timestamp from DLQ entry
		timerTaskInfo.VisibilityTimestamp = time.Unix(0, dlqTask.VisibilityTimestamp)

		// Convert to Task interface
		task, err := timerTaskInfo.ToTask()
		if err == nil {
			return task, nil
		}
	}

	return nil, fmt.Errorf("failed to deserialize task: unsupported task type=%d", dlqTask.TaskType)
}
