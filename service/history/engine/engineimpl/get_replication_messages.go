package engineimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func (e *historyEngineImpl) GetReplicationMessages(
	ctx context.Context,
	pollingCluster string,
	lastReadMessageID int64,
) (*types.ReplicationMessages, error) {

	scope := metrics.HistoryGetReplicationMessagesScope
	replMsgStart := time.Now()
	sw := e.metricsClient.StartTimer(scope, metrics.GetReplicationMessagesForShardLatency)
	defer func() {
		sw.Stop()
		e.metricsClient.Scope(scope).ExponentialHistogram(metrics.GetReplicationMessagesForShardLatencyHistogram, time.Since(replMsgStart))
	}()

	replicationMessages, err := e.replicationAckManager.GetTasks(
		ctx,
		pollingCluster,
		lastReadMessageID,
	)
	if err != nil {
		e.logger.Error("Failed to retrieve replication messages.", tag.Error(err))
		return nil, err
	}

	// Set cluster status for sync shard info
	replicationMessages.SyncShardStatus = &types.SyncShardStatus{
		Timestamp: common.Int64Ptr(e.timeSource.Now().UnixNano()),
	}
	e.logger.Debug("Successfully fetched replication messages.", tag.Counter(len(replicationMessages.ReplicationTasks)), tag.ClusterName(pollingCluster))

	if e.logger.DebugOn() {
		for _, task := range replicationMessages.ReplicationTasks {
			data, err := json.Marshal(task)
			if err != nil {
				e.logger.Error("Failed to marshal replication task.", tag.Error(err))
				continue
			}
			e.logger.Debugf("Replication task: %s", string(data))
		}
	}
	return replicationMessages, nil
}

func (e *historyEngineImpl) GetDLQReplicationMessages(
	ctx context.Context,
	taskInfos []*types.ReplicationTaskInfo,
) ([]*types.ReplicationTask, error) {

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	dlqStart := time.Now()
	sw := e.metricsClient.StartTimer(scope, metrics.GetDLQReplicationMessagesLatency)
	defer func() {
		sw.Stop()
		e.metricsClient.Scope(scope).ExponentialHistogram(metrics.GetDLQReplicationMessagesLatencyHistogram, time.Since(dlqStart))
	}()

	tasks := make([]*types.ReplicationTask, 0, len(taskInfos))
	for _, taskInfo := range taskInfos {
		t, err := convertToReplicationTask(taskInfo)
		if err != nil {
			e.logger.Error("Failed to convert replication task.", tag.Error(err))
			return nil, err
		}
		task, err := e.replicationHydrator.Hydrate(ctx, t)
		if err != nil {
			e.logger.Error("Failed to fetch DLQ replication messages.", tag.Error(err))
			return nil, err
		}
		if task != nil {
			tasks = append(tasks, task)
		}
	}

	return tasks, nil
}

func convertToReplicationTask(taskInfo *types.ReplicationTaskInfo) (persistence.Task, error) {
	switch taskInfo.TaskType {
	case persistence.ReplicationTaskTypeHistory:
		return &persistence.HistoryReplicationTask{
			WorkflowIdentifier: persistence.WorkflowIdentifier{
				DomainID:   taskInfo.DomainID,
				WorkflowID: taskInfo.WorkflowID,
				RunID:      taskInfo.RunID,
			},
			TaskData: persistence.TaskData{
				TaskID:  taskInfo.TaskID,
				Version: taskInfo.Version,
			},
			FirstEventID: taskInfo.FirstEventID,
			NextEventID:  taskInfo.NextEventID,
		}, nil
	case persistence.ReplicationTaskTypeSyncActivity:
		return &persistence.SyncActivityTask{
			WorkflowIdentifier: persistence.WorkflowIdentifier{
				DomainID:   taskInfo.DomainID,
				WorkflowID: taskInfo.WorkflowID,
				RunID:      taskInfo.RunID,
			},
			TaskData: persistence.TaskData{
				TaskID:  taskInfo.TaskID,
				Version: taskInfo.Version,
			},
			ScheduledID: taskInfo.ScheduledID,
		}, nil
	case persistence.ReplicationTaskTypeFailoverMarker:
		return &persistence.FailoverMarkerTask{
			DomainID: taskInfo.DomainID,
			TaskData: persistence.TaskData{
				TaskID:  taskInfo.TaskID,
				Version: taskInfo.Version,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported task type: %v", taskInfo.TaskType)
	}
}
