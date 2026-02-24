package queuev2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailoverDomainWithClusterAttributes(t *testing.T) {
	// Create a mock queue that tracks failover calls
	type failoverCall struct {
		domainIDs        map[string]struct{}
		clusterAttribute *ClusterAttributeKey
	}

	var calls []failoverCall

	mockQueue := &mockQueue{
		failoverFn: func(domainIDs map[string]struct{}, clusterAttr *ClusterAttributeKey) {
			calls = append(calls, failoverCall{
				domainIDs:        domainIDs,
				clusterAttribute: clusterAttr,
			})
		},
	}

	// Call with cluster attribute
	clusterAttr := &ClusterAttributeKey{
		Scope: "scope1",
		Name:  "attr1",
	}
	mockQueue.FailoverDomainWithClusterAttribute(
		map[string]struct{}{"domain1": {}},
		clusterAttr,
	)

	require.Len(t, calls, 1)
	require.Contains(t, calls[0].domainIDs, "domain1")
	require.Equal(t, "scope1", calls[0].clusterAttribute.Scope)
	require.Equal(t, "attr1", calls[0].clusterAttribute.Name)
}

// mockQueue for testing
type mockQueue struct {
	failoverFn func(map[string]struct{}, *ClusterAttributeKey)
}

func (m *mockQueue) FailoverDomainWithClusterAttribute(
	domainIDs map[string]struct{},
	clusterAttribute *ClusterAttributeKey,
) {
	if m.failoverFn != nil {
		m.failoverFn(domainIDs, clusterAttribute)
	}
}
