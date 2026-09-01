package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/persistence"
)

func TestGetOrUseDefaultActiveCluster(t *testing.T) {
	tests := []struct {
		name            string
		currentCluster  string
		activeCluster   string
		expectedCluster string
	}{
		{
			name:            "empty active cluster",
			currentCluster:  "cluster1",
			activeCluster:   "",
			expectedCluster: "cluster1",
		},
		{
			name:            "non-empty active cluster",
			currentCluster:  "cluster1",
			activeCluster:   "cluster2",
			expectedCluster: "cluster2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOrUseDefaultActiveCluster(tt.currentCluster, tt.activeCluster)
			assert.Equal(t, tt.expectedCluster, got)
		})
	}
}

func TestGetOrUseDefaultClusters(t *testing.T) {
	tests := []struct {
		name             string
		currentCluster   string
		clusters         []*persistence.ClusterReplicationConfig
		expectedClusters []*persistence.ClusterReplicationConfig
	}{
		{
			name:           "empty clusters",
			currentCluster: "cluster1",
			clusters:       []*persistence.ClusterReplicationConfig{},
			expectedClusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{
					ClusterName: "cluster1",
				},
			},
		},
		{
			name:           "non-empty clusters",
			currentCluster: "cluster1",
			clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{
					ClusterName: "cluster2",
				},
			},
			expectedClusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{
					ClusterName: "cluster2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetOrUseDefaultClusters(tt.currentCluster, tt.clusters)
			assert.Equal(t, tt.expectedClusters, got)
		})
	}
}
