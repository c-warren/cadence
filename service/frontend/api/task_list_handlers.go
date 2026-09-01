package api

import (
	"context"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/frontend/validate"
)

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes. If includeTaskListStatus field is true,
// it will also return status of tasklist's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (wh *WorkflowHandler) DescribeTaskList(
	ctx context.Context,
	request *types.DescribeTaskListRequest,
) (resp *types.DescribeTaskListResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateDescribeTaskListRequest(ctx, request); err != nil {
		return nil, err
	}
	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, err
	}
	response, err := wh.GetMatchingClient().DescribeTaskList(ctx, &types.MatchingDescribeTaskListRequest{
		DomainUUID:  domainID,
		DescRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// ListTaskListPartitions returns all the partition and host for a taskList
func (wh *WorkflowHandler) ListTaskListPartitions(
	ctx context.Context,
	request *types.ListTaskListPartitionsRequest,
) (resp *types.ListTaskListPartitionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateListTaskListPartitionsRequest(ctx, request); err != nil {
		return nil, err
	}
	resp, err := wh.GetMatchingClient().ListTaskListPartitions(ctx, &types.MatchingListTaskListPartitionsRequest{
		Domain:   request.Domain,
		TaskList: request.TaskList,
	})
	return resp, err
}

// GetTaskListsByDomain returns all the partition and host for a taskList
func (wh *WorkflowHandler) GetTaskListsByDomain(
	ctx context.Context,
	request *types.GetTaskListsByDomainRequest,
) (resp *types.GetTaskListsByDomainResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateGetTaskListsByDomainRequest(ctx, request); err != nil {
		return nil, err
	}
	resp, err := wh.GetMatchingClient().GetTaskListsByDomain(ctx, &types.GetTaskListsByDomainRequest{
		Domain: request.Domain,
	})
	return resp, err
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
func (wh *WorkflowHandler) ResetStickyTaskList(
	ctx context.Context,
	resetRequest *types.ResetStickyTaskListRequest,
) (resp *types.ResetStickyTaskListResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateResetStickyTaskListRequest(ctx, resetRequest); err != nil {
		return nil, err
	}
	domainID, err := wh.GetDomainCache().GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, err
	}
	_, err = wh.GetHistoryClient().ResetStickyTaskList(ctx, &types.HistoryResetStickyTaskListRequest{
		DomainUUID: domainID,
		Execution:  resetRequest.Execution,
	})
	if err != nil {
		return nil, wh.normalizeVersionedErrors(ctx, err)
	}
	return &types.ResetStickyTaskListResponse{}, nil
}
