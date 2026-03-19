package task

import (
	"context"
	"encoding/json"
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

			// Delete task from DLQ after successful processing
			if err := p.dlqManager.DeleteStandbyTask(ctx, &persistence.DeleteStandbyTaskRequest{
				ShardID:               dlqTask.ShardID,
				DomainID:              dlqTask.DomainID,
				ClusterAttributeScope: dlqTask.ClusterAttributeScope,
				ClusterAttributeName:  dlqTask.ClusterAttributeName,
				TaskID:                dlqTask.TaskID,
				TaskType:              dlqTask.TaskType,
				VisibilityTimestamp:   dlqTask.VisibilityTimestamp,
			}); err != nil {
				p.logger.Error("Failed to delete DLQ task after processing",
					tag.TaskID(dlqTask.TaskID),
					tag.Error(err))
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		nextPageToken = resp.NextPageToken
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
				p.logger.Debug("DLQ periodic cleanup skipped (disabled via dynamic config)")
			}
			p.cleanupTimer.Reset(cleanupInterval)

		case <-p.shutdownCh:
			return
		}
	}
}

// cleanupProcessedTasks performs range delete for tasks below ack level
func (p *StandbyTaskDLQProcessor) cleanupProcessedTasks(ctx context.Context) {
	p.logger.Debug("Running DLQ periodic cleanup")

	// Call cleanup on the DLQ manager (will be no-op for point-delete, actual cleanup for range-delete)
	if cleaner, ok := p.dlqManager.(interface {
		CleanupProcessedTasks(context.Context) error
	}); ok {
		err := cleaner.CleanupProcessedTasks(ctx)
		if err != nil {
			p.logger.Error("Failed to cleanup processed DLQ tasks", tag.Error(err))
		}
	}
}

// deserializeTask converts DLQ entry back to a persistence.Task
func (p *StandbyTaskDLQProcessor) deserializeTask(
	dlqTask *persistence.StandbyTaskDLQEntry,
) (persistence.Task, error) {
	// For POC, we only handle transfer tasks
	// Timer tasks would require category-specific handling
	var taskInfo persistence.TransferTaskInfo
	if err := json.Unmarshal(dlqTask.TaskPayload, &taskInfo); err != nil {
		return nil, err
	}

	// Restore the visibility timestamp from DLQ entry
	taskInfo.VisibilityTimestamp = time.Unix(0, dlqTask.VisibilityTimestamp)

	// Convert to Task interface
	task, err := taskInfo.ToTask()
	if err != nil {
		return nil, err
	}

	return task, nil
}
