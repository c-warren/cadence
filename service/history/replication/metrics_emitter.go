package replication

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	metricsEmissionInterval = time.Minute
)

type (
	// MetricsEmitterImpl is responsible for emitting source side replication metrics occasionally.
	MetricsEmitterImpl struct {
		shardID        int
		currentCluster string
		remoteClusters map[string]config.ClusterInformation
		shardData      metricsEmitterShardData
		reader         taskReader
		scope          metrics.Scope
		logger         log.Logger
		status         int32
		interval       time.Duration
		ctx            context.Context
		cancelCtx      context.CancelFunc
		wg             sync.WaitGroup
	}

	// metricsEmitterShardData is for testing.
	metricsEmitterShardData interface {
		GetLogger() log.Logger
		GetClusterMetadata() cluster.Metadata
		GetQueueClusterAckLevel(category persistence.HistoryTaskCategory, cluster string) persistence.HistoryTaskKey
		GetTimeSource() clock.TimeSource
	}
)

// NewMetricsEmitter creates a new metrics emitter, which starts a goroutine to emit replication metrics occasionally.
func NewMetricsEmitter(
	shardID int,
	shardData metricsEmitterShardData,
	reader taskReader,
	metricsClient metrics.Client,
) *MetricsEmitterImpl {
	currentCluster := shardData.GetClusterMetadata().GetCurrentClusterName()
	remoteClusters := shardData.GetClusterMetadata().GetRemoteClusterInfo()

	scope := metricsClient.Scope(
		metrics.ReplicationMetricEmitterScope,
		metrics.ActiveClusterTag(currentCluster),
		metrics.InstanceTag(strconv.Itoa(shardID)),
	)
	logger := shardData.GetLogger().WithTags(
		tag.ClusterName(currentCluster),
		tag.ShardID(shardID))

	ctx, cancel := context.WithCancel(context.Background())
	return &MetricsEmitterImpl{
		shardID:        shardID,
		currentCluster: currentCluster,
		remoteClusters: remoteClusters,
		status:         common.DaemonStatusInitialized,
		shardData:      shardData,
		reader:         reader,
		scope:          scope,
		interval:       metricsEmissionInterval,
		logger:         logger,
		ctx:            ctx,
		cancelCtx:      cancel,
	}
}

func (m *MetricsEmitterImpl) Start() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	m.wg.Add(1)
	go m.emitMetricsLoop()
	m.logger.Info("ReplicationMetricsEmitter started.")
}

func (m *MetricsEmitterImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	m.logger.Info("ReplicationMetricsEmitter shutting down.")
	m.cancelCtx()
	if !common.AwaitWaitGroup(&m.wg, 5*time.Second) {
		m.logger.Warn("ReplicationMetricsEmitter timed out on shutdown.")
	}
}

func (m *MetricsEmitterImpl) emitMetricsLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	defer func() { log.CapturePanic(recover(), m.logger, nil) }()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.emitMetrics()
		}
	}
}

func (m *MetricsEmitterImpl) emitMetrics() {
	for remoteClusterName := range m.remoteClusters {
		logger := m.logger.WithTags(tag.RemoteCluster(remoteClusterName))
		scope := m.scope.Tagged(metrics.TargetClusterTag(remoteClusterName))

		replicationLatency, err := m.determineReplicationLatency(remoteClusterName)
		if err != nil {
			return
		}

		scope.UpdateGauge(metrics.ReplicationLatency, float64(replicationLatency.Nanoseconds()))
		logger.Debug(fmt.Sprintf("ReplicationLatency metric emitted: %v", float64(replicationLatency.Nanoseconds())))
	}
}

func (m *MetricsEmitterImpl) determineReplicationLatency(remoteClusterName string) (time.Duration, error) {
	logger := m.logger.WithTags(tag.RemoteCluster(remoteClusterName))
	lastReadTaskID := m.shardData.GetQueueClusterAckLevel(persistence.HistoryTaskCategoryReplication, remoteClusterName).GetTaskID()

	tasks, _, err := m.reader.Read(m.ctx, lastReadTaskID, lastReadTaskID+1, 1)
	if err != nil {
		logger.Error(fmt.Sprintf(
			"Error reading when determining replication latency, lastReadTaskID=%v", lastReadTaskID),
			tag.Error(err))
		return 0, err
	}
	logger.Debug("Number of tasks retrieved", tag.Number(int64(len(tasks))))

	var replicationLatency time.Duration
	if len(tasks) > 0 {
		creationTime := tasks[0].GetVisibilityTimestamp()
		replicationLatency = m.shardData.GetTimeSource().Now().Sub(creationTime)
	}

	return replicationLatency, nil
}
