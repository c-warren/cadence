package cluster

import (
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	commonMetrics "github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

const (
	// TestCurrentClusterInitialFailoverVersion is initial failover version for current cluster
	TestCurrentClusterInitialFailoverVersion = int64(0)
	// TestAlternativeClusterInitialFailoverVersion is initial failover version for alternative cluster
	TestAlternativeClusterInitialFailoverVersion = int64(1)
	// TestDisabledClusterInitialFailoverVersion is initial failover version for disabled cluster
	TestDisabledClusterInitialFailoverVersion = int64(2)
	// TestFailoverVersionIncrement is failover version increment used for test
	TestFailoverVersionIncrement = int64(10)
	// TestCurrentClusterName is current cluster used for test
	TestCurrentClusterName = "active"
	// TestAlternativeClusterName is alternative cluster used for test
	TestAlternativeClusterName = "standby"
	// TestDisabledClusterName is disabled cluster used for test
	TestDisabledClusterName = "disabled"
	// TestRegion1 is region1 used for test
	TestRegion1 = "region1"
	// TestRegion2 is region2 used for test
	TestRegion2 = "region2"
	// TestCurrentClusterFrontendAddress is the ip port address of current cluster
	TestCurrentClusterFrontendAddress = "127.0.0.1:7104"
	// TestAlternativeClusterFrontendAddress is the ip port address of alternative cluster
	TestAlternativeClusterFrontendAddress = "127.0.0.1:8104"
	// TestClusterXDCTransport is the RPC transport used for XDC traffic <tchannel|grpc>
	TestClusterXDCTransport = "grpc"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterInfo is the same as above, just convenient for test mocking
	TestAllClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                service.Frontend,
			RPCAddress:             TestCurrentClusterFrontendAddress,
			RPCTransport:           TestClusterXDCTransport,
		},
		TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestAlternativeClusterInitialFailoverVersion,
			RPCName:                service.Frontend,
			RPCAddress:             TestAlternativeClusterFrontendAddress,
			RPCTransport:           TestClusterXDCTransport,
		},
		TestDisabledClusterName: {
			Enabled:                false,
			InitialFailoverVersion: TestDisabledClusterInitialFailoverVersion,
		},
	}

	// TestSingleDCAllClusterNames is the all cluster names used for test
	TestSingleDCAllClusterNames = []string{TestCurrentClusterName}
	// TestSingleDCClusterInfo is the same as above, just convenient for test mocking
	TestSingleDCClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                service.Frontend,
			RPCAddress:             TestCurrentClusterFrontendAddress,
			RPCTransport:           TestClusterXDCTransport,
		},
	}

	// TestActiveClusterMetadata is metadata for an active cluster
	TestActiveClusterMetadata = NewMetadata(
		config.ClusterGroupMetadata{
			FailoverVersionIncrement: TestFailoverVersionIncrement,
			PrimaryClusterName:       TestCurrentClusterName,
			CurrentClusterName:       TestCurrentClusterName,
			ClusterGroup:             TestAllClusterInfo,
		},
		func(d string) bool { return false },
		commonMetrics.NewNoopMetricsClient(),
		log.NewNoop(),
	)

	// TestPassiveClusterMetadata is metadata for a passive cluster
	TestPassiveClusterMetadata = NewMetadata(
		config.ClusterGroupMetadata{
			FailoverVersionIncrement: TestFailoverVersionIncrement,
			PrimaryClusterName:       TestCurrentClusterName,
			CurrentClusterName:       TestAlternativeClusterName,
			ClusterGroup:             TestAllClusterInfo,
		},
		func(d string) bool { return false },
		commonMetrics.NewNoopMetricsClient(),
		log.NewNoop(),
	)
)

// GetTestClusterMetadata return an cluster metadata instance, which is initialized
func GetTestClusterMetadata(isPrimaryCluster bool) Metadata {
	primaryClusterName := TestCurrentClusterName
	if !isPrimaryCluster {
		primaryClusterName = TestAlternativeClusterName
	}

	return NewMetadata(
		config.ClusterGroupMetadata{
			FailoverVersionIncrement: TestFailoverVersionIncrement,
			PrimaryClusterName:       primaryClusterName,
			CurrentClusterName:       TestCurrentClusterName,
			ClusterGroup:             TestAllClusterInfo,
		},
		func(d string) bool { return false },
		commonMetrics.NewNoopMetricsClient(),
		log.NewNoop(),
	)
}
