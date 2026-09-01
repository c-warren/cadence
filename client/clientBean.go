//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination clientBean_mock.go -self_package github.com/uber/cadence/client

package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/executorclient"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/client/wrappers/timeout"
	"github.com/uber/cadence/common/cluster"
)

type (
	// Bean in an collection of clients
	Bean interface {
		GetHistoryClient() history.Client
		GetHistoryPeers() history.PeerResolver
		GetMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error)
		GetFrontendClient() frontend.Client
		GetShardDistributorExecutorClient() executorclient.Client
		GetRemoteAdminClient(cluster string) (admin.Client, error)
		SetRemoteAdminClient(cluster string, client admin.Client)
		GetRemoteFrontendClient(cluster string) (frontend.Client, error)
	}

	clientBeanImpl struct {
		sync.Mutex
		historyClient                  history.Client
		historyPeers                   history.PeerResolver
		matchingClient                 atomic.Value
		frontendClient                 frontend.Client
		shardDistributorExecutorClient executorclient.Client
		remoteAdminClients             map[string]admin.Client
		remoteFrontendClients          map[string]frontend.Client
		factory                        Factory
	}
)

// NewClientBean provides a collection of clients
func NewClientBean(factory Factory, dispatcher *yarpc.Dispatcher, clusterMetadata cluster.Metadata) (Bean, error) {

	historyClient, historyPeers, err := factory.NewHistoryClient()
	if err != nil {
		return nil, err
	}

	remoteAdminClients := map[string]admin.Client{}
	remoteFrontendClients := map[string]frontend.Client{}
	for clusterName := range clusterMetadata.GetEnabledClusterInfo() {
		clientConfig := dispatcher.ClientConfig(clusterName)

		adminClient, err := factory.NewAdminClientWithTimeoutAndConfig(
			clientConfig,
			timeout.AdminDefaultTimeout,
			timeout.AdminDefaultLargeTimeout,
		)
		if err != nil {
			return nil, err
		}

		frontendClient, err := factory.NewFrontendClientWithTimeoutAndConfig(
			clientConfig,
			timeout.FrontendDefaultTimeout,
			timeout.FrontendDefaultLongPollTimeout,
		)
		if err != nil {
			return nil, err
		}

		remoteAdminClients[clusterName] = adminClient
		remoteFrontendClients[clusterName] = frontendClient
	}

	shardDistributorExecutorClient, err := factory.NewShardDistributorExecutorClient()
	if err != nil {
		return nil, err
	}

	return &clientBeanImpl{
		factory:                        factory,
		historyClient:                  historyClient,
		historyPeers:                   historyPeers,
		frontendClient:                 remoteFrontendClients[clusterMetadata.GetCurrentClusterName()],
		shardDistributorExecutorClient: shardDistributorExecutorClient,
		remoteAdminClients:             remoteAdminClients,
		remoteFrontendClients:          remoteFrontendClients,
	}, nil
}

func (h *clientBeanImpl) GetHistoryClient() history.Client {
	return h.historyClient
}

func (h *clientBeanImpl) GetHistoryPeers() history.PeerResolver {
	return h.historyPeers
}

func (h *clientBeanImpl) GetMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error) {
	if client := h.matchingClient.Load(); client != nil {
		return client.(matching.Client), nil
	}
	return h.lazyInitMatchingClient(domainIDToName)
}

func (h *clientBeanImpl) GetFrontendClient() frontend.Client {
	return h.frontendClient
}

func (h *clientBeanImpl) GetShardDistributorExecutorClient() executorclient.Client {
	return h.shardDistributorExecutorClient
}

func (h *clientBeanImpl) GetRemoteAdminClient(cluster string) (admin.Client, error) {
	client, ok := h.remoteAdminClients[cluster]
	if !ok {
		return nil, fmt.Errorf("unknown cluster name: %v with given cluster client map: %v", cluster, h.remoteAdminClients)
	}
	return client, nil
}

func (h *clientBeanImpl) SetRemoteAdminClient(
	cluster string,
	client admin.Client,
) {

	h.remoteAdminClients[cluster] = client
}

func (h *clientBeanImpl) GetRemoteFrontendClient(cluster string) (frontend.Client, error) {
	client, ok := h.remoteFrontendClients[cluster]
	if !ok {
		return nil, fmt.Errorf("unknown cluster name: %v with given cluster client map: %v", cluster, h.remoteFrontendClients)
	}
	return client, nil
}

func (h *clientBeanImpl) lazyInitMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error) {
	h.Lock()
	defer h.Unlock()
	if cached := h.matchingClient.Load(); cached != nil {
		return cached.(matching.Client), nil
	}
	client, err := h.factory.NewMatchingClient(domainIDToName)
	if err != nil {
		return nil, err
	}
	h.matchingClient.Store(client)
	return client, nil
}
