package activecluster

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/types"
)

//go:generate mockgen -package $GOPACKAGE -destination manager_mock.go -self_package github.com/uber/cadence/common/activecluster github.com/uber/cadence/common/activecluster Manager

// Manager is the interface for active cluster manager.
// It is used to get active cluster info by cluster attribute or workflow.
type Manager interface {
	// GetActiveClusterInfoByClusterAttribute returns the active cluster info by cluster attribute
	// If clusterAttribute is nil, returns the domain-level active cluster info
	// If clusterAttribute is not nil and exists in the domain metadata, returns the active cluster info of the cluster attribute
	// If clusterAttribute is not nil and does not exist in the domain metadata, returns an error
	GetActiveClusterInfoByClusterAttribute(ctx context.Context, domainID string, clusterAttribute *types.ClusterAttribute) (*types.ActiveClusterInfo, error)

	// GetActiveClusterInfoByWorkflow returns the active cluster info by workflow
	// It will first look up the cluster selection policy for the workflow and then get the active cluster info by cluster attribute from the policy
	GetActiveClusterInfoByWorkflow(ctx context.Context, domainID, wfID, rID string) (*types.ActiveClusterInfo, error)

	// GetActiveClusterSelectionPolicyForWorkflow returns the active cluster selection policy for a workflow
	GetActiveClusterSelectionPolicyForWorkflow(ctx context.Context, domainID, wfID, rID string) (*types.ActiveClusterSelectionPolicy, error)

	// GetActiveClusterSelectionPolicyForCurrentWorkflow returns the active cluster selection policy for the current workflow
	// if the workflow is NOT closed, returns policy and true, otherwise returns nil and false
	GetActiveClusterSelectionPolicyForCurrentWorkflow(ctx context.Context, domainID, wfID string) (*types.ActiveClusterSelectionPolicy, bool, error)
}

type ClusterAttributeNotFoundError struct {
	DomainID         string
	ClusterAttribute *types.ClusterAttribute
	// ActiveClusterInfo *types.ActiveClusterInfo
}

func (e *ClusterAttributeNotFoundError) Error() string {
	return fmt.Sprintf("could not find cluster attribute %s in the domain %s's active cluster config", e.ClusterAttribute, e.DomainID)
}
