package cache

import (
	"testing"
	"time"

	p8s "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	tallyp8s "github.com/uber-go/tally/prometheus"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

func TestCacheMetricsLabelConsistency(t *testing.T) {
	var registrationErrors []error
	promCfg := &tallyp8s.Configuration{
		OnError:   "none",
		TimerType: "histogram",
	}
	reporter, err := promCfg.NewReporter(tallyp8s.ConfigurationOptions{
		Registry: p8s.NewRegistry(),
		OnError: func(err error) {
			registrationErrors = append(registrationErrors, err)
		},
	})
	require.NoError(t, err)

	rootScope, closer := tally.NewRootScope(tally.ScopeOptions{
		Tags: map[string]string{
			metrics.CadenceServiceTagName: "history",
		},
		CachedReporter: reporter,
		Separator:      tallyp8s.DefaultSeparator,
	}, time.Second)
	defer closer.Close()

	historyMetrics := metrics.NewClient(rootScope, metrics.History, metrics.MigrationConfig{})
	lruCacheScopes := []metrics.Scope{
		historyMetrics.Scope(metrics.HistoryExecutionCacheScope).Tagged(metrics.ShardIDTag(1)),
		historyMetrics.Scope(metrics.HistoryWorkflowCacheScope).Tagged(metrics.NonShardTag()),
		historyMetrics.Scope(metrics.EventsCacheGetEventScope).Tagged(metrics.ShardIDTag(7)),
		historyMetrics.Scope(metrics.ActiveClusterManagerWorkflowCacheScope).Tagged(metrics.NonShardTag()),
		historyMetrics.Scope(metrics.LoadBalancerScope).Tagged(metrics.NonShardTag()),
		historyMetrics.Scope(metrics.PartitionConfigProviderScope).Tagged(metrics.NonShardTag()),
		historyMetrics.Scope(metrics.PersistenceGetShardScope).Tagged(metrics.NonShardTag()),
	}
	for _, scope := range lruCacheScopes {
		emitLRUCacheMetrics(t, scope)
	}
	emitMutableStateCacheMetrics(historyMetrics)
	emitEventsCacheMetrics(historyMetrics)
	emitReplicationCacheMetrics(historyMetrics)

	require.Empty(t, registrationErrors, "Prometheus registration errors must not be emitted")
}

func emitLRUCacheMetrics(t *testing.T, metricScope metrics.Scope) {
	t.Helper()

	lruCache := New(&Options{
		MaxCount:     1,
		MetricsScope: metricScope,
		Logger:       log.NewNoop(),
		IsSizeBased:  dynamicproperties.GetBoolPropertyFn(false),
	})

	require.Nil(t, lruCache.Get("missing"))
	lruCache.Put("first", "value")
	require.Equal(t, "value", lruCache.Get("first"))
	lruCache.Put("second", "value")
}

func emitMutableStateCacheMetrics(client metrics.Client) {
	scope := metrics.WithCacheScopeLabels(
		client.Scope(metrics.HistoryCacheGetOrCreateScope),
		metrics.ShardIDTag(1),
		metrics.SourceClusterNoneTagValue,
		metrics.MutableStateCacheTypeTagValue,
	)

	scope.IncCounter(metrics.CacheRequests)
	scope.IncCounter(metrics.CacheMissCounter)
	scope.RecordTimer(metrics.CacheLatency, time.Millisecond)
}

func emitEventsCacheMetrics(client metrics.Client) {
	scope := metrics.WithCacheScopeLabels(
		client.Scope(metrics.EventsCacheGetEventScope),
		metrics.ShardIDTag(1),
		metrics.SourceClusterNoneTagValue,
		metrics.EventsCacheTypeTagValue,
	)

	scope.IncCounter(metrics.CacheRequests)
	scope.IncCounter(metrics.CacheMissCounter)
	scope.RecordTimer(metrics.CacheLatency, time.Millisecond)
}

func emitReplicationCacheMetrics(client metrics.Client) {
	scope := client.Scope(
		metrics.ReplicatorCacheManagerScope,
		metrics.CacheTypeTag(metrics.ReplicationCacheTypeTagValue),
		metrics.SourceClusterTag("active"),
		metrics.ShardIDTag(1),
	)

	scope.IncCounter(metrics.CacheRequests)
	scope.IncCounter(metrics.CacheHitCounter)
	scope.IncCounter(metrics.CacheMissCounter)
	scope.IncCounter(metrics.CacheFullCounter)
	scope.RecordTimer(metrics.CacheLatency, time.Millisecond)
	scope.ExponentialHistogram(metrics.ExponentialCacheLatency, time.Millisecond)
	scope.RecordTimer(metrics.CacheSize, time.Duration(1))
	scope.RecordHistogramValue(metrics.CacheSizeHistogram, 1)
	scope.UpdateGauge(metrics.CacheSizeGauge, 1)
}
