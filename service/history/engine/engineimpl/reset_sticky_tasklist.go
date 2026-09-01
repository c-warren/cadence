package engineimpl

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/workflow"
)

// ResetStickyTaskList reset the volatile information in mutable state of a given types.
// Volatile information are the information related to client, such as:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (e *historyEngineImpl) ResetStickyTaskList(
	ctx context.Context,
	resetRequest *types.HistoryResetStickyTaskListRequest,
) (*types.HistoryResetStickyTaskListResponse, error) {

	if err := common.ValidateDomainUUID(resetRequest.DomainUUID); err != nil {
		return nil, err
	}
	domainID := resetRequest.DomainUUID

	err := workflow.UpdateWithAction(ctx, e.logger, e.executionCache, domainID, *resetRequest.Execution, false, e.timeSource.Now(),
		func(wfContext execution.Context, mutableState execution.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return workflow.ErrAlreadyCompleted
			}
			mutableState.ClearStickyness()
			return nil
		},
	)

	if err != nil {
		return nil, err
	}
	return &types.HistoryResetStickyTaskListResponse{}, nil
}
