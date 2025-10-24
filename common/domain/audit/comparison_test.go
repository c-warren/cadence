package audit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func TestComputeChangeSummary_DefaultClusterChange(t *testing.T) {
	before := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
		},
	}

	after := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster2",
		},
	}

	summary, err := ComputeChangeSummary(before, after)
	require.NoError(t, err)
	assert.True(t, summary.DefaultClusterChanged)
	assert.Contains(t, summary.ChangedFields, "ActiveClusterName")
	assert.Len(t, summary.ClusterAttributesChanged, 0)
}

func TestComputeChangeSummary_ClusterAttributeChange(t *testing.T) {
	before := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster1",
								FailoverVersion:   100,
							},
						},
					},
				},
			},
		},
	}

	after := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster2", // Changed
								FailoverVersion:   101,
							},
						},
					},
				},
			},
		},
	}

	summary, err := ComputeChangeSummary(before, after)
	require.NoError(t, err)
	assert.False(t, summary.DefaultClusterChanged)
	assert.Contains(t, summary.ChangedFields, "ActiveClusters")
	require.Len(t, summary.ClusterAttributesChanged, 1)
	assert.Equal(t, "region", summary.ClusterAttributesChanged[0].Scope)
	assert.Equal(t, "us-east-1", summary.ClusterAttributesChanged[0].Name)
}

func TestComputeChangeSummary_NoChanges(t *testing.T) {
	domain := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
		},
	}

	summary, err := ComputeChangeSummary(domain, domain)
	require.NoError(t, err)
	assert.False(t, summary.DefaultClusterChanged)
	assert.Len(t, summary.ChangedFields, 0)
	assert.Len(t, summary.ClusterAttributesChanged, 0)
}

func TestComputeClusterFailovers_DefaultCluster(t *testing.T) {
	before := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
		},
	}

	after := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster2",
		},
	}

	failovers, err := ComputeClusterFailovers(before, after)
	require.NoError(t, err)
	require.Len(t, failovers, 1)

	failover := failovers[0]
	assert.NotNil(t, failover.IsDefaultCluster)
	assert.True(t, *failover.IsDefaultCluster)
	assert.Equal(t, "cluster1", failover.FromCluster.ActiveClusterName)
	assert.Equal(t, "cluster2", failover.ToCluster.ActiveClusterName)
	assert.Nil(t, failover.ClusterAttribute)
}

func TestComputeClusterFailovers_ClusterAttributes(t *testing.T) {
	before := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster1",
								FailoverVersion:   100,
							},
							"us-west-1": {
								ActiveClusterName: "cluster1",
								FailoverVersion:   100,
							},
						},
					},
				},
			},
		},
	}

	after := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster2", // Changed
								FailoverVersion:   101,
							},
							"us-west-1": {
								ActiveClusterName: "cluster1", // Unchanged
								FailoverVersion:   100,
							},
						},
					},
				},
			},
		},
	}

	failovers, err := ComputeClusterFailovers(before, after)
	require.NoError(t, err)
	require.Len(t, failovers, 1, "Only us-east-1 should have failed over")

	failover := failovers[0]
	assert.NotNil(t, failover.IsDefaultCluster)
	assert.False(t, *failover.IsDefaultCluster)
	assert.Equal(t, "cluster1", failover.FromCluster.ActiveClusterName)
	assert.Equal(t, int64(100), failover.FromCluster.FailoverVersion)
	assert.Equal(t, "cluster2", failover.ToCluster.ActiveClusterName)
	assert.Equal(t, int64(101), failover.ToCluster.FailoverVersion)
	require.NotNil(t, failover.ClusterAttribute)
	assert.Equal(t, "region", failover.ClusterAttribute.Scope)
	assert.Equal(t, "us-east-1", failover.ClusterAttribute.Name)
}

func TestComputeClusterFailovers_MixedFailover(t *testing.T) {
	before := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster1",
								FailoverVersion:   100,
							},
						},
					},
				},
			},
		},
	}

	after := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster2", // Default cluster changed
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: map[string]types.ActiveClusterInfo{
							"us-east-1": {
								ActiveClusterName: "cluster3", // Attribute changed
								FailoverVersion:   101,
							},
						},
					},
				},
			},
		},
	}

	failovers, err := ComputeClusterFailovers(before, after)
	require.NoError(t, err)
	require.Len(t, failovers, 2, "Both default and attribute should have failed over")

	// Find default and attribute failovers
	var defaultFailover, attrFailover *types.ClusterFailover
	for _, f := range failovers {
		if *f.IsDefaultCluster {
			defaultFailover = f
		} else {
			attrFailover = f
		}
	}

	require.NotNil(t, defaultFailover)
	require.NotNil(t, attrFailover)

	assert.Equal(t, "cluster1", defaultFailover.FromCluster.ActiveClusterName)
	assert.Equal(t, "cluster2", defaultFailover.ToCluster.ActiveClusterName)

	assert.Equal(t, "cluster1", attrFailover.FromCluster.ActiveClusterName)
	assert.Equal(t, "cluster3", attrFailover.ToCluster.ActiveClusterName)
	assert.Equal(t, "region", attrFailover.ClusterAttribute.Scope)
	assert.Equal(t, "us-east-1", attrFailover.ClusterAttribute.Name)
}

func TestComputeClusterFailovers_NoChanges(t *testing.T) {
	domain := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID: "test-domain",
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
		},
	}

	failovers, err := ComputeClusterFailovers(domain, domain)
	require.NoError(t, err)
	assert.Len(t, failovers, 0, "No changes should result in zero failovers")
}
