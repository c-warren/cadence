package audit

import (
	"context"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// HydrateListResponse controls whether ListFailoverHistory decompresses and hydrates full details
// POC Toggle: Set to true to test hydrated list responses
const HydrateListResponse = false

// ChangeSummary represents a lightweight summary of what changed in a domain update
type ChangeSummary struct {
	ChangedFields            []string               `json:"changed_fields"`
	DefaultClusterChanged    bool                   `json:"default_cluster_changed"`
	ClusterAttributesChanged []*ClusterAttributeRef `json:"cluster_attributes_changed,omitempty"`
}

// ClusterAttributeRef references a specific cluster attribute that changed
type ClusterAttributeRef struct {
	Scope string `json:"scope"`
	Name  string `json:"name"`
}

// ComputeChangeSummary compares two domain states and returns a lightweight summary
// This is cached at write time to enable fast filtering at read time
func ComputeChangeSummary(
	before, after *persistence.GetDomainResponse,
) (*ChangeSummary, error) {
	summary := &ChangeSummary{
		ChangedFields:            []string{},
		ClusterAttributesChanged: []*ClusterAttributeRef{},
	}

	// Check default cluster change
	if before.ReplicationConfig != nil && after.ReplicationConfig != nil {
		if before.ReplicationConfig.ActiveClusterName != after.ReplicationConfig.ActiveClusterName {
			summary.ChangedFields = append(summary.ChangedFields, "ActiveClusterName")
			summary.DefaultClusterChanged = true
		}

		// Check cluster attributes
		changedAttrs := compareClusterAttributes(
			before.ReplicationConfig.ActiveClusters,
			after.ReplicationConfig.ActiveClusters,
		)

		if len(changedAttrs) > 0 {
			summary.ChangedFields = append(summary.ChangedFields, "ActiveClusters")
			summary.ClusterAttributesChanged = changedAttrs
		}
	}

	return summary, nil
}

// ComputeClusterFailovers performs deep comparison of domain states and returns detailed failover info
// This is computed at read time for GetFailoverEvent
func ComputeClusterFailovers(
	before, after *persistence.GetDomainResponse,
) ([]*types.ClusterFailover, error) {
	var failovers []*types.ClusterFailover

	// Handle nil ReplicationConfig cases
	beforeRC := before.ReplicationConfig
	afterRC := after.ReplicationConfig

	if beforeRC == nil && afterRC == nil {
		// No replication config in either state - nothing to report
		return failovers, nil
	}

	// Check default cluster change
	beforeCluster := ""
	afterCluster := ""
	if beforeRC != nil {
		beforeCluster = beforeRC.ActiveClusterName
	}
	if afterRC != nil {
		afterCluster = afterRC.ActiveClusterName
	}

	if beforeCluster != afterCluster {
		isDefault := true
		failovers = append(failovers, &types.ClusterFailover{
			FromCluster: &types.ActiveClusterInfo{
				ActiveClusterName: beforeCluster,
			},
			ToCluster: &types.ActiveClusterInfo{
				ActiveClusterName: afterCluster,
			},
			IsDefaultCluster: &isDefault,
		})
	}

	// Check cluster attributes
	var beforeAttrs, afterAttrs *types.ActiveClusters
	if beforeRC != nil {
		beforeAttrs = beforeRC.ActiveClusters
	}
	if afterRC != nil {
		afterAttrs = afterRC.ActiveClusters
	}

	// Handle all cluster attribute changes (added, removed, changed)
	failovers = append(failovers, computeClusterAttributeChanges(beforeAttrs, afterAttrs)...)

	return failovers, nil
}

// computeClusterAttributeChanges compares cluster attributes and returns all changes
func computeClusterAttributeChanges(
	beforeAttrs, afterAttrs *types.ActiveClusters,
) []*types.ClusterFailover {
	var failovers []*types.ClusterFailover

	// Track all scopes and names we've seen
	allScopes := make(map[string]bool)

	// Collect all scopes from both before and after
	if beforeAttrs != nil && beforeAttrs.AttributeScopes != nil {
		for scope := range beforeAttrs.AttributeScopes {
			allScopes[scope] = true
		}
	}
	if afterAttrs != nil && afterAttrs.AttributeScopes != nil {
		for scope := range afterAttrs.AttributeScopes {
			allScopes[scope] = true
		}
	}

	// Iterate through all scopes
	for scope := range allScopes {
		var beforeScopeData, afterScopeData types.ClusterAttributeScope
		var beforeExists, afterExists bool

		if beforeAttrs != nil && beforeAttrs.AttributeScopes != nil {
			beforeScopeData, beforeExists = beforeAttrs.AttributeScopes[scope]
		}
		if afterAttrs != nil && afterAttrs.AttributeScopes != nil {
			afterScopeData, afterExists = afterAttrs.AttributeScopes[scope]
		}

		// Track all attribute names in this scope
		allNames := make(map[string]bool)

		if beforeExists && beforeScopeData.ClusterAttributes != nil {
			for name := range beforeScopeData.ClusterAttributes {
				allNames[name] = true
			}
		}
		if afterExists && afterScopeData.ClusterAttributes != nil {
			for name := range afterScopeData.ClusterAttributes {
				allNames[name] = true
			}
		}

		// Check each attribute name
		for name := range allNames {
			var beforeInfo, afterInfo types.ActiveClusterInfo
			var beforeHasInfo, afterHasInfo bool

			if beforeExists && beforeScopeData.ClusterAttributes != nil {
				beforeInfo, beforeHasInfo = beforeScopeData.ClusterAttributes[name]
			}
			if afterExists && afterScopeData.ClusterAttributes != nil {
				afterInfo, afterHasInfo = afterScopeData.ClusterAttributes[name]
			}

			// Determine if cluster changed
			beforeCluster := ""
			afterCluster := ""
			var beforeVersion, afterVersion int64

			if beforeHasInfo {
				beforeCluster = beforeInfo.ActiveClusterName
				beforeVersion = beforeInfo.FailoverVersion
			}
			if afterHasInfo {
				afterCluster = afterInfo.ActiveClusterName
				afterVersion = afterInfo.FailoverVersion
			}

			// Record if cluster changed (including additions and removals)
			if beforeCluster != afterCluster {
				isDefault := false
				failovers = append(failovers, &types.ClusterFailover{
					FromCluster: &types.ActiveClusterInfo{
						ActiveClusterName: beforeCluster,
						FailoverVersion:   beforeVersion,
					},
					ToCluster: &types.ActiveClusterInfo{
						ActiveClusterName: afterCluster,
						FailoverVersion:   afterVersion,
					},
					ClusterAttribute: &types.ClusterAttribute{
						Scope: scope,
						Name:  name,
					},
					IsDefaultCluster: &isDefault,
				})
			}
		}
	}

	return failovers
}

// compareClusterAttributes identifies which cluster attributes changed between two states
func compareClusterAttributes(
	before, after *types.ActiveClusters,
) []*ClusterAttributeRef {
	var changed []*ClusterAttributeRef

	if before == nil || after == nil {
		return changed
	}

	if before.AttributeScopes == nil || after.AttributeScopes == nil {
		return changed
	}

	for scope, afterScopeData := range after.AttributeScopes {
		beforeScopeData, exists := before.AttributeScopes[scope]
		if !exists {
			continue
		}

		if afterScopeData.ClusterAttributes == nil || beforeScopeData.ClusterAttributes == nil {
			continue
		}

		for name, afterInfo := range afterScopeData.ClusterAttributes {
			beforeInfo, exists := beforeScopeData.ClusterAttributes[name]
			if !exists {
				continue
			}

			if beforeInfo.ActiveClusterName != afterInfo.ActiveClusterName {
				changed = append(changed, &ClusterAttributeRef{
					Scope: scope,
					Name:  name,
				})
			}
		}
	}

	return changed
}

// DetermineOperationType determines the operation type from the update request
func DetermineOperationType(request *types.UpdateDomainRequest) persistence.DomainOperationType {
	// Check if this is a failover (active cluster change)
	if request.ActiveClusterName != nil {
		return persistence.DomainOperationTypeFailover
	}

	// Check for active clusters (active-active failover)
	if request.ActiveClusters != nil {
		return persistence.DomainOperationTypeFailover
	}

	// Otherwise it's a regular update
	return persistence.DomainOperationTypeUpdate
}

// ExtractIdentity extracts identity information from the context
// For POC, return placeholder values
// TODO: Implement proper identity extraction from context
func ExtractIdentity(ctx context.Context) (identity, identityType string) {
	// Placeholder for POC
	return "unknown", "system"
}
