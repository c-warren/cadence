package isolationgroupapi

import (
	"context"

	"github.com/uber/cadence/common/types"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination isolation_handler_mock.go -self_package github.com/uber/cadence/common/isolationgroup/isolationgrouphandler

type Handler interface {
	// GetGlobalState returns the current configuration of isolation-groups which apply to all domains
	GetGlobalState(ctx context.Context) (*types.GetGlobalIsolationGroupsResponse, error)
	// UpdateGlobalState updates isolation-groups which apply to all domains
	UpdateGlobalState(ctx context.Context, state types.UpdateGlobalIsolationGroupsRequest) error
	// GetDomainState is the read operation for getting the current state of a domain's isolation-groups
	GetDomainState(ctx context.Context, request types.GetDomainIsolationGroupsRequest) (*types.GetDomainIsolationGroupsResponse, error)
	// UpdateDomainState is the read operation for updating a domain's isolation-groups
	UpdateDomainState(ctx context.Context, state types.UpdateDomainIsolationGroupsRequest) error
}
