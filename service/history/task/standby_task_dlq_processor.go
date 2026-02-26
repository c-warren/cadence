package task

import (
	"context"
	"encoding/json"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	// StandbyTaskDLQProcessor processes tasks from the DLQ after failover
	StandbyTaskDLQProcessor struct {
		dlqManager persistence.StandbyTaskDLQManager
		executor   Executor
		logger     log.Logger
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
	logger log.Logger,
) *StandbyTaskDLQProcessor {
	return &StandbyTaskDLQProcessor{
		dlqManager: dlqManager,
		executor:   executor,
		logger:     logger,
	}
}

// ProcessFailover processes all DLQ tasks for a specific domain/cluster attribute combination
func (p *StandbyTaskDLQProcessor) ProcessFailover(
	ctx context.Context,
	request *ProcessFailoverRequest,
) error {
	p.logger.Info("Processing DLQ tasks for failover",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope),
		tag.Value(request.ClusterAttributeName))

	pageSize := 100
	var nextPageToken []byte

	for {
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
			if err := p.processTask(ctx, dlqTask); err != nil {
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

	p.logger.Info("Completed processing DLQ tasks for failover",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID))

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
		return err
	}

	// For POC, we'll execute the raw persistence task
	// In production, this would wrap it with NewHistoryTask and execute through the queue
	// For now, just log that we would execute it
	p.logger.Info("DLQ task deserialized successfully, would execute",
		tag.TaskID(persistenceTask.GetTaskID()),
		tag.WorkflowDomainID(persistenceTask.GetDomainID()))

	return nil
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
