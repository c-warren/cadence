package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
)

func (e *historyEngineImpl) CountDLQMessages(ctx context.Context, forceFetch bool) (map[string]int64, error) {
	return e.replicationDLQHandler.GetMessageCount(ctx, forceFetch)
}

func (e *historyEngineImpl) ReadDLQMessages(
	ctx context.Context,
	request *types.ReadDLQMessagesRequest,
) (*types.ReadDLQMessagesResponse, error) {

	tasks, taskInfo, token, err := e.replicationDLQHandler.ReadMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}

	// ReadMessages returns nil entries for tasks that could not be hydrated (e.g. the
	// source workflow was deleted). A nil *ReplicationTask cannot be serialized into the
	// response: the generated protobuf MarshalToSizedBuffer dereferences each repeated
	// element without a nil check, so a nil entry panics the history service while
	// marshaling the RPC response. Callers correlate tasks with their metadata by
	// SourceTaskID and the full set is still represented in ReplicationTasksInfo, so
	// dropping the unhydrated tasks here is safe.
	hydratedTasks := make([]*types.ReplicationTask, 0, len(tasks))
	for _, task := range tasks {
		if task != nil {
			hydratedTasks = append(hydratedTasks, task)
		}
	}

	return &types.ReadDLQMessagesResponse{
		Type:                 request.GetType().Ptr(),
		ReplicationTasks:     hydratedTasks,
		ReplicationTasksInfo: taskInfo,
		NextPageToken:        token,
	}, nil
}

func (e *historyEngineImpl) PurgeDLQMessages(
	ctx context.Context,
	request *types.PurgeDLQMessagesRequest,
) error {

	return e.replicationDLQHandler.PurgeMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
	)
}

func (e *historyEngineImpl) MergeDLQMessages(
	ctx context.Context,
	request *types.MergeDLQMessagesRequest,
) (*types.MergeDLQMessagesResponse, error) {

	token, err := e.replicationDLQHandler.MergeMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return &types.MergeDLQMessagesResponse{
		NextPageToken: token,
	}, nil
}
