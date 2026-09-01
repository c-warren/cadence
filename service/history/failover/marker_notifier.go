//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination marker_notifier_mock.go -self_package github.com/uber/cadence/service/history/failover

package failover

import (
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

type (
	// MarkerNotifier notifies failover markers to the remote failover coordinator
	MarkerNotifier interface {
		common.Daemon
	}

	markerNotifierImpl struct {
		status              int32
		shutdownCh          chan struct{}
		shard               shard.Context
		config              *config.Config
		failoverCoordinator Coordinator
		logger              log.Logger
		metrics             metrics.Client
	}
)

// NewMarkerNotifier creates a new instance of failover marker notifier
func NewMarkerNotifier(
	shard shard.Context,
	config *config.Config,
	failoverCoordinator Coordinator,
) MarkerNotifier {

	return &markerNotifierImpl{
		status:              common.DaemonStatusInitialized,
		shutdownCh:          make(chan struct{}, 1),
		shard:               shard,
		config:              config,
		failoverCoordinator: failoverCoordinator,
		logger:              shard.GetLogger().WithTags(tag.ComponentFailoverMarkerNotifier),
		metrics:             shard.GetMetricsClient(),
	}
}

func (m *markerNotifierImpl) Start() {

	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go m.notifyPendingFailoverMarker()
	m.logger.Info("Marker notifier state changed", tag.LifeCycleStarted)
}

func (m *markerNotifierImpl) Stop() {

	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	close(m.shutdownCh)
	m.logger.Info("Marker notifier state changed", tag.LifeCycleStopped)
}

func (m *markerNotifierImpl) notifyPendingFailoverMarker() {

	ticker := time.NewTicker(m.config.NotifyFailoverMarkerInterval())
	defer ticker.Stop()

	for {
		select {
		case <-m.shutdownCh:
			return
		case <-ticker.C:
			markers, err := m.shard.ValidateAndUpdateFailoverMarkers()
			if err != nil {
				m.metrics.IncCounter(metrics.FailoverMarkerScope, metrics.FailoverMarkerUpdateShardFailure)
				m.logger.Error("Failed to update pending failover markers in shard info.", tag.Error(err))
			}

			if len(markers) > 0 {
				m.failoverCoordinator.NotifyFailoverMarkers(int32(m.shard.GetShardID()), markers)
			}
		}
	}
}
