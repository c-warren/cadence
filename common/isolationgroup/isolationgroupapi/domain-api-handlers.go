package isolationgroupapi

import (
	"context"

	"github.com/uber/cadence/common/types"
)

func (z *handlerImpl) GetDomainState(ctx context.Context, request types.GetDomainIsolationGroupsRequest) (*types.GetDomainIsolationGroupsResponse, error) {
	res, err := z.domainHandler.DescribeDomain(ctx, &types.DescribeDomainRequest{
		Name: &request.Domain,
	})
	if err != nil {
		return nil, err
	}
	if res == nil || res.Configuration == nil || res.Configuration.IsolationGroups == nil {
		return &types.GetDomainIsolationGroupsResponse{}, nil
	}
	return &types.GetDomainIsolationGroupsResponse{
		IsolationGroups: *res.Configuration.IsolationGroups,
	}, nil
}

// UpdateDomainState is the read operation for updating a domain's isolation-groups
// todo (david.porter) delete this handler and use domain-handler directly
func (z *handlerImpl) UpdateDomainState(ctx context.Context, request types.UpdateDomainIsolationGroupsRequest) error {
	err := z.domainHandler.UpdateIsolationGroups(ctx, request)
	return err
}
