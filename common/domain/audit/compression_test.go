package audit

import (
	"fmt"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func TestSerializeAndCompress_SmallDomain(t *testing.T) {
	domain := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:   "test-domain-id",
			Name: "test-domain",
		},
		Config: &persistence.DomainConfig{
			Retention: 7,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
		},
	}

	compressed, err := SerializeAndCompress(domain)
	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Greater(t, len(compressed), 0, "Compressed data should not be empty")
}

func TestSerializeAndCompress_LargeDomain(t *testing.T) {
	// Create domain with 100 cluster attributes to simulate a larger domain
	clusterAttrs := make(map[string]types.ActiveClusterInfo)
	for i := 0; i < 100; i++ {
		clusterAttrs[fmt.Sprintf("attr-%d", i)] = types.ActiveClusterInfo{
			ActiveClusterName: "cluster1",
			FailoverVersion:   int64(i),
		}
	}

	domain := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:   "test-domain-id",
			Name: "test-domain-large",
		},
		Config: &persistence.DomainConfig{
			Retention: 7,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster1",
			ActiveClusters: &types.ActiveClusters{
				AttributeScopes: map[string]types.ClusterAttributeScope{
					"region": {
						ClusterAttributes: clusterAttrs,
					},
				},
			},
		},
	}

	compressed, err := SerializeAndCompress(domain)
	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Greater(t, len(compressed), 0)

	// Log compression ratio for reference
	// Note: Can't easily get uncompressed size without re-marshaling
	t.Logf("Compressed size for domain with 100 attributes: %d bytes", len(compressed))
}

func TestDecompressAndDeserialize_RoundTrip(t *testing.T) {
	original := &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:   "test-domain-id",
			Name: "test-domain",
		},
		Config: &persistence.DomainConfig{
			Retention: 30,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "cluster2",
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: "cluster1"},
				{ClusterName: "cluster2"},
			},
		},
	}

	// Compress
	compressed, err := SerializeAndCompress(original)
	require.NoError(t, err)

	// Decompress
	restored, err := DecompressAndDeserialize(compressed)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Info.ID, restored.Info.ID)
	assert.Equal(t, original.Info.Name, restored.Info.Name)
	assert.Equal(t, original.Config.Retention, restored.Config.Retention)
	assert.Equal(t, original.ReplicationConfig.ActiveClusterName, restored.ReplicationConfig.ActiveClusterName)
	assert.Len(t, restored.ReplicationConfig.Clusters, 2)
}

func TestDecompressAndDeserialize_CorruptedData(t *testing.T) {
	corrupted := []byte("this is not valid snappy compressed data")

	_, err := DecompressAndDeserialize(corrupted)
	assert.Error(t, err, "Should fail on corrupted data")
}

func TestDecompressAndDeserialize_InvalidJSON(t *testing.T) {
	// Create valid Snappy compression of invalid JSON
	invalidJSON := []byte("{this is not valid json}")
	compressed := snappy.Encode(nil, invalidJSON)

	_, err := DecompressAndDeserialize(compressed)
	assert.Error(t, err, "Should fail on invalid JSON")
}
