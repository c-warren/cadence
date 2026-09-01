package replicator

import (
	"time"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
)

type (
	// Replicator is the processor for replication tasks
	Replicator struct {
		clusterMetadata               cluster.Metadata
		domainReplicationTaskExecutor domain.ReplicationTaskExecutor
		clientBean                    client.Bean
		domainProcessors              []*domainReplicationProcessor
		logger                        log.Logger
		metricsClient                 metrics.Client
		hostInfo                      membership.HostInfo
		membershipResolver            membership.Resolver
		domainReplicationQueue        domain.ReplicationQueue
		replicationMaxRetry           time.Duration
	}
)

// NewReplicator creates a new replicator for processing replication tasks
func NewReplicator(
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
	logger log.Logger,
	metricsClient metrics.Client,
	hostInfo membership.HostInfo,
	membership membership.Resolver,
	domainReplicationQueue domain.ReplicationQueue,
	domainReplicationTaskExecutor domain.ReplicationTaskExecutor,
	replicationMaxRetry time.Duration,
) *Replicator {

	logger = logger.WithTags(tag.ComponentReplicator)
	return &Replicator{
		hostInfo:                      hostInfo,
		membershipResolver:            membership,
		clusterMetadata:               clusterMetadata,
		domainReplicationTaskExecutor: domainReplicationTaskExecutor,
		clientBean:                    clientBean,
		logger:                        logger,
		metricsClient:                 metricsClient,
		domainReplicationQueue:        domainReplicationQueue,
		replicationMaxRetry:           replicationMaxRetry,
	}
}

// Start is called to start replicator
func (r *Replicator) Start() error {
	currentClusterName := r.clusterMetadata.GetCurrentClusterName()
	for clusterName := range r.clusterMetadata.GetRemoteClusterInfo() {
		adminClient, err := r.clientBean.GetRemoteAdminClient(clusterName)
		if err != nil {
			return err
		}
		processor := newDomainReplicationProcessor(
			clusterName,
			currentClusterName,
			r.logger.WithTags(tag.ComponentReplicationTaskProcessor, tag.SourceCluster(clusterName)),
			adminClient,
			r.metricsClient,
			r.domainReplicationTaskExecutor,
			r.hostInfo,
			r.membershipResolver,
			r.domainReplicationQueue,
			r.replicationMaxRetry,
			clock.NewRealTimeSource(),
		)
		r.domainProcessors = append(r.domainProcessors, processor)
	}

	for _, domainProcessor := range r.domainProcessors {
		domainProcessor.Start()
	}

	return nil
}

// Stop is called to stop replicator
func (r *Replicator) Stop() {

	for _, domainProcessor := range r.domainProcessors {
		domainProcessor.Stop()
	}
}
