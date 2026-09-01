package isolationgroupapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/types"
)

func (z *handlerImpl) UpdateGlobalState(ctx context.Context, in types.UpdateGlobalIsolationGroupsRequest) error {
	if z.globalIsolationGroupDrains == nil {
		return &types.BadRequestError{"global isolation group drain is not supported in this cluster"}
	}
	mappedInput, err := MapUpdateGlobalIsolationGroupsRequest(in.IsolationGroups)
	if err != nil {
		return err
	}
	return z.globalIsolationGroupDrains.UpdateValue(
		dynamicproperties.DefaultIsolationGroupConfigStoreManagerGlobalMapping,
		mappedInput,
	)
}

func (z *handlerImpl) GetGlobalState(ctx context.Context) (*types.GetGlobalIsolationGroupsResponse, error) {
	if z.globalIsolationGroupDrains == nil {
		return nil, &types.BadRequestError{"global isolation group drain is not supported in this cluster"}
	}
	res, err := z.globalIsolationGroupDrains.GetListValue(dynamicproperties.DefaultIsolationGroupConfigStoreManagerGlobalMapping, nil)
	if err != nil {
		var e types.EntityNotExistsError
		if errors.As(err, &e) {
			return &types.GetGlobalIsolationGroupsResponse{}, nil
		}
		return nil, fmt.Errorf("failed to get global isolation groups from datastore: %w", err)
	}
	resp, err := MapDynamicConfigResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get global isolation groups from datastore: %w", err)
	}
	return &types.GetGlobalIsolationGroupsResponse{IsolationGroups: resp}, nil
}
