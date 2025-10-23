// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package host

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

type DomainAuditIntegrationSuite struct {
	*require.Assertions
	IntegrationBase
}

func TestDomainAuditIntegrationSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	s := new(DomainAuditIntegrationSuite)
	s.Assertions = require.New(t)
	suite.Run(t, s)
}

func (s *DomainAuditIntegrationSuite) SetupSuite() {
	s.setupSuite()
}

func (s *DomainAuditIntegrationSuite) TearDownSuite() {
	s.TearDownBaseSuite()
}

func (s *DomainAuditIntegrationSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *DomainAuditIntegrationSuite) TestActivePassiveFailoverAuditLog() {
	domainName := "audit-test-ap-" + common.GenerateRandomString(5)
	ctx := context.Background()

	// Register global domain
	registerReq := &types.RegisterDomainRequest{
		Name:                                   domainName,
		IsGlobalDomain:                         true,
		Clusters:                               []*types.ClusterReplicationConfiguration{{ClusterName: "active"}, {ClusterName: "standby"}},
		ActiveClusterName:                      "active",
		WorkflowExecutionRetentionPeriodInDays: 1,
	}

	err := s.Engine.RegisterDomain(ctx, registerReq)
	s.NoError(err)

	// Get domain ID
	descResp, err := s.Engine.DescribeDomain(ctx, &types.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.NoError(err)
	domainID := descResp.DomainInfo.GetUUID()

	// Perform failover
	updateReq := &types.UpdateDomainRequest{
		Name:              domainName,
		ActiveClusterName: common.StringPtr("standby"),
	}

	_, err = s.Engine.UpdateDomain(ctx, updateReq)
	s.NoError(err)

	// Wait a bit for write to complete (synchronous in POC, but good practice)
	time.Sleep(100 * time.Millisecond)

	// Query audit log
	pageSize := int32(10)
	historyResp, err := s.Engine.ListFailoverHistory(ctx, &types.ListFailoverHistoryRequest{
		Filters: &types.ListFailoverHistoryRequestFilters{
			DomainID:             &domainID,
			DefaultActiveCluster: common.BoolPtr(true),
		},
		Pagination: &types.PaginationOptions{
			PageSize: &pageSize,
		},
	})
	s.NoError(err)
	s.NotNil(historyResp)
	s.Greater(len(historyResp.FailoverEvents), 0, "Should have at least one failover event")

	// Get detailed failover info
	event := historyResp.FailoverEvents[0]
	s.NotNil(event.ID)
	s.NotNil(event.CreatedTime)

	detailResp, err := s.Engine.GetFailoverEvent(ctx, &types.GetFailoverEventRequest{
		DomainID:        &domainID,
		FailoverEventID: event.ID,
		CreatedTime:     event.CreatedTime,
	})
	s.NoError(err)
	s.NotNil(detailResp)
	s.Greater(len(detailResp.ClusterFailovers), 0)

	// Verify it's a default cluster failover
	failover := detailResp.ClusterFailovers[0]
	s.NotNil(failover.IsDefaultCluster)
	s.True(*failover.IsDefaultCluster)
	s.NotNil(failover.ToCluster)
	s.Equal("standby", failover.ToCluster.ActiveClusterName)
}

func (s *DomainAuditIntegrationSuite) TestActiveActiveClusterAttributeFailoverAuditLog() {
	domainName := "audit-test-aa-" + common.GenerateRandomString(5)
	ctx := context.Background()

	// Register global domain with cluster attributes
	registerReq := &types.RegisterDomainRequest{
		Name:                                   domainName,
		IsGlobalDomain:                         true,
		Clusters:                               []*types.ClusterReplicationConfiguration{{ClusterName: "cluster1"}, {ClusterName: "cluster2"}},
		ActiveClusterName:                      "cluster1",
		WorkflowExecutionRetentionPeriodInDays: 1,
		ActiveClusters: &types.ActiveClusters{
			AttributeScopes: map[string]types.ClusterAttributeScope{
				"region": {
					ClusterAttributes: map[string]types.ActiveClusterInfo{
						"us-east-1": {ActiveClusterName: "cluster1"},
						"us-west-1": {ActiveClusterName: "cluster2"},
					},
				},
			},
		},
	}

	err := s.Engine.RegisterDomain(ctx, registerReq)
	s.NoError(err)

	// Get domain ID
	descResp, err := s.Engine.DescribeDomain(ctx, &types.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	})
	s.NoError(err)
	domainID := descResp.DomainInfo.GetUUID()

	// Failover us-east-1 from cluster1 to cluster2
	updateReq := &types.UpdateDomainRequest{
		Name: domainName,
		ActiveClusters: &types.ActiveClusters{
			AttributeScopes: map[string]types.ClusterAttributeScope{
				"region": {
					ClusterAttributes: map[string]types.ActiveClusterInfo{
						"us-east-1": {ActiveClusterName: "cluster2"},
					},
				},
			},
		},
	}

	_, err = s.Engine.UpdateDomain(ctx, updateReq)
	s.NoError(err)

	time.Sleep(100 * time.Millisecond)

	// Query audit log filtering for cluster attributes
	usEast1Scope := "region"
	usEast1Name := "us-east-1"
	pageSize := int32(10)
	historyResp, err := s.Engine.ListFailoverHistory(ctx, &types.ListFailoverHistoryRequest{
		Filters: &types.ListFailoverHistoryRequestFilters{
			DomainID: &domainID,
			Attributes: []*types.ClusterAttribute{
				{Scope: usEast1Scope, Name: usEast1Name},
			},
		},
		Pagination: &types.PaginationOptions{
			PageSize: &pageSize,
		},
	})
	s.NoError(err)
	s.Greater(len(historyResp.FailoverEvents), 0)

	// Get details
	event := historyResp.FailoverEvents[0]
	detailResp, err := s.Engine.GetFailoverEvent(ctx, &types.GetFailoverEventRequest{
		DomainID:        &domainID,
		FailoverEventID: event.ID,
		CreatedTime:     event.CreatedTime,
	})
	s.NoError(err)
	s.Greater(len(detailResp.ClusterFailovers), 0)

	// Verify it's a cluster attribute failover
	failover := detailResp.ClusterFailovers[0]
	s.NotNil(failover.IsDefaultCluster)
	s.False(*failover.IsDefaultCluster)
	s.NotNil(failover.ClusterAttribute)
	s.Equal("region", failover.ClusterAttribute.Scope)
	s.Equal("us-east-1", failover.ClusterAttribute.Name)
	s.NotNil(failover.ToCluster)
	s.Equal("cluster2", failover.ToCluster.ActiveClusterName)
}
