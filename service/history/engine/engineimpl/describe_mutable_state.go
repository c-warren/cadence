package engineimpl

import (
	"context"
	"encoding/json"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
)

func (e *historyEngineImpl) DescribeMutableState(
	ctx context.Context,
	request *types.DescribeMutableStateRequest,
) (response *types.DescribeMutableStateResponse, retError error) {

	if err := common.ValidateDomainUUID(request.DomainUUID); err != nil {
		return nil, err
	}

	domainID := request.DomainUUID
	execution := types.WorkflowExecution{
		WorkflowID: request.Execution.WorkflowID,
		RunID:      request.Execution.RunID,
	}

	cacheCtx, dbCtx, release, cacheHit, err := e.executionCache.GetAndCreateWorkflowExecution(
		ctx, domainID, execution,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	response = &types.DescribeMutableStateResponse{}

	if cacheHit {
		if msb := cacheCtx.GetWorkflowExecution(); msb != nil {
			response.MutableStateInCache, err = e.toMutableStateJSON(msb)
			if err != nil {
				return nil, err
			}
		}
	}

	msb, err := dbCtx.LoadWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}
	response.MutableStateInDatabase, err = e.toMutableStateJSON(msb)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *historyEngineImpl) toMutableStateJSON(msb execution.MutableState) (string, error) {
	ms := msb.CopyToPersistence()

	jsonBytes, err := json.Marshal(ms)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
