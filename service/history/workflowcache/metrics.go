package workflowcache

import (
	"sync"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/metrics"
)

// workflowIDCountMetric holds the count of requests for a given second, for a domain/workflowID pair
// This is used to emit the max count of requests for a given domain
// Ideally we would just emit the count of requests for a given domain/workflowID pair, but this is not
// possible, due to the high cardinality of workflowIDs
type workflowIDCountMetric struct {
	sync.Mutex

	startingSecond time.Time
	count          int
}

func (cm *workflowIDCountMetric) reset(now time.Time) {
	cm.startingSecond = now
	cm.count = 0
}

func (cm *workflowIDCountMetric) updatePerDomainMaxWFRequestCount(
	domainName string,
	timeSource clock.TimeSource,
	metricsClient metrics.Client,
	metric metrics.MetricIdx,
) {
	cm.Lock()
	defer cm.Unlock()

	if timeSource.Since(cm.startingSecond) > time.Second {
		cm.reset(timeSource.Now().UTC())
	}
	cm.count++

	// We can just use the upper of the metric, so it is not an issue to emit all the counts
	metricsClient.Scope(metrics.HistoryClientWfIDCacheScope, metrics.DomainTag(domainName)).
		RecordTimer(metric, time.Duration(cm.count))

	var histMetric metrics.MetricIdx
	switch metric {
	case metrics.WorkflowIDCacheRequestsExternalMaxRequestsPerSecondsTimer:
		histMetric = metrics.WorkflowIDCacheRequestsExternalMaxRequestsPerSecondsHistogram
	case metrics.WorkflowIDCacheRequestsInternalMaxRequestsPerSecondsTimer:
		histMetric = metrics.WorkflowIDCacheRequestsInternalMaxRequestsPerSecondsHistogram
	}
	if histMetric != 0 {
		metricsClient.Scope(metrics.HistoryClientWfIDCacheScope, metrics.DomainTag(domainName)).
			IntExponentialHistogram(histMetric, cm.count)
	}
}
