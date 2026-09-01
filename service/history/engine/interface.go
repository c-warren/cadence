//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package engine

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	hcommon "github.com/uber/cadence/service/history/common"
	"github.com/uber/cadence/service/history/events"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx context.Context, request *types.HistoryStartWorkflowExecutionRequest) (*types.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *types.GetMutableStateRequest) (*types.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *types.PollMutableStateRequest) (*types.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *types.DescribeMutableStateRequest) (*types.DescribeMutableStateResponse, error)
		ResetStickyTaskList(ctx context.Context, resetRequest *types.HistoryResetStickyTaskListRequest) (*types.HistoryResetStickyTaskListResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *types.HistoryDescribeWorkflowExecutionRequest) (*types.DescribeWorkflowExecutionResponse, error)
		RecordDecisionTaskStarted(ctx context.Context, request *types.RecordDecisionTaskStartedRequest) (*types.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx context.Context, request *types.RecordActivityTaskStartedRequest) (*types.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(ctx context.Context, request *types.HistoryRespondDecisionTaskCompletedRequest) (*types.HistoryRespondDecisionTaskCompletedResponse, error)
		RespondDecisionTaskFailed(ctx context.Context, request *types.HistoryRespondDecisionTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *types.HistoryRespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx context.Context, request *types.HistoryRespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx context.Context, request *types.HistoryRespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx context.Context, request *types.HistoryRecordActivityTaskHeartbeatRequest) (*types.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *types.HistoryRequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx context.Context, request *types.HistorySignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx context.Context, request *types.HistorySignalWithStartWorkflowExecutionRequest) (*types.StartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *types.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *types.HistoryTerminateWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx context.Context, request *types.HistoryResetWorkflowExecutionRequest) (*types.ResetWorkflowExecutionResponse, error)
		ScheduleDecisionTask(ctx context.Context, request *types.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *types.RecordChildExecutionCompletedRequest) error
		ReplicateEventsV2(ctx context.Context, request *types.ReplicateEventsV2Request) error
		SyncShardStatus(ctx context.Context, request *types.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *types.SyncActivityRequest) error
		GetReplicationMessages(ctx context.Context, pollingCluster string, lastReadMessageID int64) (*types.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx context.Context, taskInfos []*types.ReplicationTaskInfo) ([]*types.ReplicationTask, error)
		QueryWorkflow(ctx context.Context, request *types.HistoryQueryWorkflowRequest) (*types.HistoryQueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, domainUUID string, workflowID string, runID string, events []*types.HistoryEvent) error
		CountDLQMessages(ctx context.Context, forceFetch bool) (map[string]int64, error)
		ReadDLQMessages(ctx context.Context, messagesRequest *types.ReadDLQMessagesRequest) (*types.ReadDLQMessagesResponse, error)
		PurgeDLQMessages(ctx context.Context, messagesRequest *types.PurgeDLQMessagesRequest) error
		MergeDLQMessages(ctx context.Context, messagesRequest *types.MergeDLQMessagesRequest) (*types.MergeDLQMessagesResponse, error)
		RefreshWorkflowTasks(ctx context.Context, domainUUID string, execution types.WorkflowExecution) error
		ResetTransferQueue(ctx context.Context, clusterName string) error
		ResetTimerQueue(ctx context.Context, clusterName string) error
		DescribeTransferQueue(ctx context.Context, clusterName string) (*types.DescribeQueueResponse, error)
		DescribeTimerQueue(ctx context.Context, clusterName string) (*types.DescribeQueueResponse, error)

		NotifyNewHistoryEvent(event *events.Notification)
		NotifyNewTransferTasks(info *hcommon.NotifyTaskInfo)
		NotifyNewTimerTasks(info *hcommon.NotifyTaskInfo)
		NotifyNewReplicationTasks(info *hcommon.NotifyTaskInfo)
	}
)
