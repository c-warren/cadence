package engineimpl

import (
	"context"
	"errors"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	hcommon "github.com/uber/cadence/service/history/common"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/replication"
)

func (e *historyEngineImpl) NotifyNewHistoryEvent(event *events.Notification) {
	e.historyEventNotifier.NotifyNewHistoryEvent(event)
}

func (e *historyEngineImpl) NotifyNewTransferTasks(info *hcommon.NotifyTaskInfo) {
	if len(info.Tasks) == 0 {
		return
	}

	task := info.Tasks[0]
	clusterName, err := e.shard.GetClusterMetadata().ClusterNameForFailoverVersion(task.GetVersion())
	if err != nil {
		e.logger.Warn("cluster name for failover version not found", tag.Error(err), tag.Value(task.GetVersion()), tag.TaskID(task.GetTaskID()))
		return
	}

	transferProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTransfer]
	if !ok {
		e.logger.Error("transfer processor not found", tag.Error(err))
		return
	}
	transferProcessor.NotifyNewTask(clusterName, info)
}

func (e *historyEngineImpl) NotifyNewTimerTasks(info *hcommon.NotifyTaskInfo) {
	if len(info.Tasks) == 0 {
		return
	}

	task := info.Tasks[0]
	clusterName, err := e.shard.GetClusterMetadata().ClusterNameForFailoverVersion(task.GetVersion())
	if err != nil {
		e.logger.Warn("cluster name for failover version not found", tag.Error(err), tag.Value(task.GetVersion()), tag.TaskID(task.GetTaskID()))
		return
	}
	timerProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTimer]
	if !ok {
		e.logger.Error("timer processor not found", tag.Error(err))
		return
	}
	timerProcessor.NotifyNewTask(clusterName, info)
}

func (e *historyEngineImpl) NotifyNewReplicationTasks(info *hcommon.NotifyTaskInfo) {
	for _, task := range info.Tasks {
		hTask, err := hydrateReplicationTask(task, info.ExecutionInfo, info.VersionHistories, info.Activities, info.History)
		if err != nil {
			e.logger.Error("failed to preemptively hydrate replication task", tag.Error(err))
			continue
		}
		e.replicationTaskStore.Put(hTask)
	}
}

func hydrateReplicationTask(
	task persistence.Task,
	exec *persistence.WorkflowExecutionInfo,
	versionHistories *persistence.VersionHistories,
	activities map[int64]*persistence.ActivityInfo,
	history events.PersistedBlobs,
) (*types.ReplicationTask, error) {
	info := persistence.ReplicationTaskInfo{
		DomainID:     exec.DomainID,
		WorkflowID:   exec.WorkflowID,
		RunID:        exec.RunID,
		TaskType:     task.GetTaskType(),
		CreationTime: task.GetVisibilityTimestamp().UnixNano(),
		TaskID:       task.GetTaskID(),
		Version:      task.GetVersion(),
	}

	switch t := task.(type) {
	case *persistence.HistoryReplicationTask:
		info.BranchToken = t.BranchToken
		info.NewRunBranchToken = t.NewRunBranchToken
		info.FirstEventID = t.FirstEventID
		info.NextEventID = t.NextEventID
	case *persistence.SyncActivityTask:
		info.ScheduledID = t.ScheduledID
	case *persistence.FailoverMarkerTask:
		// No specific fields, but supported
	default:
		return nil, errors.New("unknown replication task")
	}

	hydrator := replication.NewImmediateTaskHydrator(
		exec.IsRunning(),
		versionHistories,
		activities,
		history.Find(info.BranchToken, info.FirstEventID),
		history.Find(info.NewRunBranchToken, constants.FirstEventID),
	)

	return hydrator.Hydrate(context.Background(), task)
}
