package api

import (
	"context"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/frontend/validate"
)

// RefreshWorkflowTasks re-generates the workflow tasks
func (wh *WorkflowHandler) RefreshWorkflowTasks(
	ctx context.Context,
	request *types.RefreshWorkflowTasksRequest,
) error {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}
	if err := wh.requestValidator.ValidateRefreshWorkflowTasksRequest(ctx, request); err != nil {
		return err
	}
	domainEntry, err := wh.GetDomainCache().GetDomain(request.GetDomain())
	if err != nil {
		return err
	}
	err = wh.GetHistoryClient().RefreshWorkflowTasks(ctx, &types.HistoryRefreshWorkflowTasksRequest{
		DomainUIID: domainEntry.GetInfo().ID,
		Request:    request,
	})
	if err != nil {
		return err
	}
	return nil
}
