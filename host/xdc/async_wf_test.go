// Copyright (c) 2018 Uber Technologies, Inc.
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

//go:build !race && asyncwfxdcintegration
// +build !race,asyncwfxdcintegration

/*
This is the cross-region (xdc) integration test for the history-backed async
workflow queue (Phase 5). It proves that a StartWorkflowExecutionAsync request
submitted to the ACTIVE cluster replicates to the STANDBY cluster and results in
a running workflow visible on both.

It requires a full 2-cluster xdc stack (active + standby frontends, kafka for
replication, and Cassandra for both the workflow history and the
async_workflow_queue* tables), so it is opt-in behind the asyncwfxdcintegration
build tag and is never compiled or run by the default `make test` / `make build`.

To run locally against a running 2-cluster stack:

	go test -v ./host/xdc -run TestAsyncWFCrossDCTestSuite -tags asyncwfxdcintegration
*/
package xdc

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	pt "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/host"

	_ "github.com/uber/cadence/common/asyncworkflow/queue/history" // needed to load the history-backed asyncworkflow queue provider
)

const (
	asyncWFNumOfRetry   = 60
	asyncWFWaitTimeInMs = 500
)

// cacheRefreshInterval is how long to wait for the domain cache (and replicated
// domain metadata) to pick up a change. It mirrors the value used by the other
// host integration tests.
var cacheRefreshInterval = cache.DomainCacheRefreshInterval + time.Second

var (
	clusterNameAsyncWF              = []string{"active", "standby"}
	clusterReplicationConfigAsyncWF = []*types.ClusterReplicationConfiguration{
		{
			ClusterName: clusterNameAsyncWF[0],
		},
		{
			ClusterName: clusterNameAsyncWF[1],
		},
	}
)

// createAsyncWFContext returns a context with a generous timeout suitable for the
// cross-region RPCs exercised by this suite.
func createAsyncWFContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 90*time.Second)
}

type asyncWFCrossDCTestSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that
	// s.NotNil(nil) will stop the test, not merely log an error.
	*require.Assertions
	suite.Suite
	cluster1       *host.TestCluster // active
	cluster2       *host.TestCluster // standby
	logger         log.Logger
	clusterConfigs []*host.TestClusterConfig
}

func TestAsyncWFCrossDCTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(asyncWFCrossDCTestSuite))
}

func (s *asyncWFCrossDCTestSuite) SetupSuite() {
	s.logger = testlogger.New(s.T())

	fileName := "../testdata/xdc_async_wf_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	s.Require().NoError(environment.SetupEnv())
	// #nosec - this is just reading a test config file
	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	s.clusterConfigs = clusterConfigs

	s.cluster1 = s.newCluster(clusterConfigs[0], clusterNameAsyncWF[0])
	s.cluster2 = s.newCluster(clusterConfigs[1], clusterNameAsyncWF[1])
}

// newCluster spins up a single test cluster from its config, wiring up the
// persistence test cluster, cluster metadata and dynamic configuration the same
// way the single-region history-backed async suite does
// (host/async_wf_history_test.go), but for one of the two xdc clusters.
func (s *asyncWFCrossDCTestSuite) newCluster(cfg *host.TestClusterConfig, clusterName string) *host.TestCluster {
	// TestClusterConfig.TimeSource is a MockedTimeSource. Only a few components
	// respect it -- most notably the worker's async-workflow ConsumerManager (its
	// poll ticker and its async domain cache). We therefore drive it forward
	// manually during polling (see advanceTime) so the consumer keeps pulling from
	// the history-backed queue, mirroring the single-region Phase 4 suite
	// (host/async_wf_history_test.go). The xdc replication stack itself runs on
	// wall-clock time and is unaffected.
	cfg.TimeSource = clock.NewMockedTimeSource()

	persistenceCluster := host.NewPersistenceTestCluster(s.T(), cfg)
	clusterMetadata := host.NewClusterMetadata(s.T(), cfg)

	dc := persistence.DynamicConfiguration{
		EnableCassandraAllConsistencyLevelDelete: dynamicproperties.GetBoolPropertyFn(true),
		EnableShardIDMetrics:                     dynamicproperties.GetBoolPropertyFn(true),
		EnableHistoryTaskDualWriteMode:           dynamicproperties.GetBoolPropertyFn(true),
		ReadNoSQLHistoryTaskFromDataBlob:         dynamicproperties.GetBoolPropertyFn(false),
		SerializationEncoding:                    dynamicproperties.GetStringPropertyFn(string(constants.EncodingTypeThriftRW)),
		ReadNoSQLShardFromDataBlob:               dynamicproperties.GetBoolPropertyFn(true),
		HistoryNodeDeleteBatchSize:               dynamicproperties.GetIntPropertyFn(1000),
	}

	params := pt.TestBaseParams{
		DefaultTestCluster:    persistenceCluster,
		VisibilityTestCluster: persistenceCluster,
		ClusterMetadata:       clusterMetadata,
		DynamicConfiguration:  dc,
	}

	c, err := host.NewCluster(s.T(), cfg, s.logger.WithTags(tag.ClusterName(clusterName)), params)
	s.Require().NoError(err)
	return c
}

func (s *asyncWFCrossDCTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it
	// earlier, s.T() will return nil.
	s.Assertions = require.New(s.T())
}

func (s *asyncWFCrossDCTestSuite) TearDownSuite() {
	if s.cluster1 != nil {
		s.cluster1.TearDownCluster()
	}
	if s.cluster2 != nil {
		s.cluster2.TearDownCluster()
	}
}

// TestStartWorkflowExecutionAsync_ReplicatesToStandby verifies the end-to-end
// cross-region liveness of the history-backed async workflow path:
//
//  1. A global domain is registered on the ACTIVE cluster (active = primary) and
//     is observable on the STANDBY cluster after domain replication.
//  2. The domain is pointed at the predefined history-backed async queue
//     ("test-async-wf-queue") via UpdateDomainAsyncWorkflowConfiguraton on the
//     active cluster; that configuration replicates to standby.
//  3. StartWorkflowExecutionAsync is submitted to the ACTIVE frontend. On active,
//     the frontend enqueues into the local history-backed async_workflow_queue
//     and the worker's ConsumerManager pulls the request and calls the sync
//     StartWorkflowExecution, creating the execution.
//  4. The execution becomes visible on BOTH the active and the standby clusters.
//
// LIMITATION: This test proves end-to-end liveness (an async request submitted to
// active results in a running workflow that is also visible on standby), but it
// does NOT by itself isolate async-REQUEST replication (the Phase 5.1/5.2
// AsyncWorkflowRequestTask -> standby async_workflow_queue path) from ordinary
// workflow-history replication. Once the workflow actually starts on active, its
// history replicates to standby through the normal replication stack, so a
// successful DescribeWorkflowExecution on standby would succeed regardless of
// whether the async REQUEST itself was replicated. A stronger, more targeted
// follow-up would probe the standby cluster's async_workflow_queue directly for
// the replicated request row (e.g. via the persistence layer / an admin queue
// read) before the consumer drains it, to confirm the request — not just the
// resulting workflow — crossed the region boundary.
func (s *asyncWFCrossDCTestSuite) TestStartWorkflowExecutionAsync_ReplicatesToStandby() {
	activeClient := s.cluster1.GetFrontendClient()
	standbyClient := s.cluster2.GetFrontendClient()
	activeAdminClient := s.cluster1.GetAdminClient()

	domainName := "test-xdc-async-wf-" + common.GenerateRandomString(5)

	// 1. Register a global domain on the active cluster.
	regReq := &types.RegisterDomainRequest{
		Name:                                   domainName,
		Clusters:                               clusterReplicationConfigAsyncWF,
		ActiveClusterName:                      clusterNameAsyncWF[0],
		IsGlobalDomain:                         true,
		WorkflowExecutionRetentionPeriodInDays: 1,
	}
	regCtx, cancel := createAsyncWFContext()
	err := activeClient.RegisterDomain(regCtx, regReq)
	cancel()
	s.NoError(err)

	// Wait for the domain to be replicated + cached, then verify it is visible on
	// the standby cluster.
	s.advanceTime(cacheRefreshInterval)
	time.Sleep(cacheRefreshInterval)

	descReq := &types.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	}
	descCtx, cancel := createAsyncWFContext()
	standbyResp, err := standbyClient.DescribeDomain(descCtx, descReq)
	cancel()
	s.NoError(err)
	s.NotNil(standbyResp)
	s.Equal(domainName, standbyResp.DomainInfo.GetName())

	// 2. Point the domain at the predefined history-backed async queue via the
	// active cluster's admin client, then allow it to replicate.
	updCtx, cancel := createAsyncWFContext()
	_, err = activeAdminClient.UpdateDomainAsyncWorkflowConfiguraton(updCtx, &types.UpdateDomainAsyncWorkflowConfiguratonRequest{
		Domain: domainName,
		Configuration: &types.AsyncWorkflowConfiguration{
			Enabled:             true,
			PredefinedQueueName: "test-async-wf-queue",
		},
	})
	cancel()
	s.NoError(err)

	s.advanceTime(cacheRefreshInterval)
	time.Sleep(cacheRefreshInterval)

	// 3. Submit StartWorkflowExecutionAsync to the ACTIVE frontend.
	wfID := "xdc-async-wf-test-" + uuid.New()
	wfType := "xdc-async-wf-test-type"
	taskList := "xdc-async-wf-test-tasklist"
	identity := "worker1"

	asyncReq := &types.StartWorkflowExecutionAsyncRequest{
		StartWorkflowExecutionRequest: &types.StartWorkflowExecutionRequest{
			RequestID:  uuid.New(),
			Domain:     domainName,
			WorkflowID: wfID,
			WorkflowType: &types.WorkflowType{
				Name: wfType,
			},
			TaskList: &types.TaskList{
				Name: taskList,
			},
			Input:                               nil,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			Identity:                            identity,
		},
	}
	startCtx, cancel := createAsyncWFContext()
	_, err = activeClient.StartWorkflowExecutionAsync(startCtx, asyncReq)
	cancel()
	s.NoError(err)

	// 4. Assert the workflow eventually starts on the ACTIVE cluster (the worker's
	// ConsumerManager pulled the request from the history-backed queue and called
	// StartWorkflowExecution), then assert cross-region propagation to STANDBY.
	s.waitForWorkflowVisible(activeClient, domainName, wfID, "active")
	s.waitForWorkflowVisible(standbyClient, domainName, wfID, "standby")
}

// waitForWorkflowVisible polls DescribeWorkflowExecution on the given frontend
// until the workflow appears or the retry budget is exhausted. There is no
// decider/poller, so we only assert the execution was created, not that it made
// progress.
func (s *asyncWFCrossDCTestSuite) waitForWorkflowVisible(client host.FrontendClient, domainName, wfID, clusterLabel string) {
	descReq := &types.DescribeWorkflowExecutionRequest{
		Domain: domainName,
		Execution: &types.WorkflowExecution{
			WorkflowID: wfID,
		},
	}
	for i := 0; i < asyncWFNumOfRetry; i++ {
		ctx, cancel := createAsyncWFContext()
		resp, err := client.DescribeWorkflowExecution(ctx, descReq)
		cancel()
		if err == nil && resp.GetWorkflowExecutionInfo() != nil {
			s.logger.Info("workflow visible",
				tag.ClusterName(clusterLabel),
				tag.WorkflowID(wfID))
			return
		}
		// Advance both clusters' (mocked) time sources so the worker's async
		// ConsumerManager poll ticker fires and pulls the enqueued request.
		s.advanceTime(time.Second)
		time.Sleep(asyncWFWaitTimeInMs * time.Millisecond)
	}
	s.Failf("workflow not visible", "async started workflow %q not found on %s cluster", wfID, clusterLabel)
}

// advanceTime moves both clusters' mocked time sources forward. Only the async
// ConsumerManager (and its async domain cache) consult this time source; the xdc
// replication stack runs on wall-clock time.
func (s *asyncWFCrossDCTestSuite) advanceTime(d time.Duration) {
	for _, cfg := range s.clusterConfigs {
		if cfg.TimeSource != nil {
			cfg.TimeSource.Advance(d)
		}
	}
}
