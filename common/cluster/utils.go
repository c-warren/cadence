package cluster

import (
	"github.com/uber/cadence/common/persistence"
)

// GetOrUseDefaultActiveCluster return the current cluster name or use the input if valid
func GetOrUseDefaultActiveCluster(currentClusterName string, activeClusterName string) string {
	if len(activeClusterName) == 0 {
		return currentClusterName
	}
	return activeClusterName
}

// GetOrUseDefaultClusters return the current cluster or use the input if valid
func GetOrUseDefaultClusters(currentClusterName string, clusters []*persistence.ClusterReplicationConfig) []*persistence.ClusterReplicationConfig {
	if len(clusters) == 0 {
		return []*persistence.ClusterReplicationConfig{
			&persistence.ClusterReplicationConfig{
				ClusterName: currentClusterName,
			},
		}
	}
	return clusters
}
