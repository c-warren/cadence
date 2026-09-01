package clusterredirection

import (
	"time"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

type (
	domainIDGetter interface {
		GetDomainID() string
	}
)

func (handler *clusterRedirectionHandler) beforeCall(
	scope metrics.ScopeIdx,
) (metrics.Scope, time.Time) {
	return handler.GetMetricsClient().Scope(scope), handler.GetTimeSource().Now()
}

func (handler *clusterRedirectionHandler) afterCall(
	recovered interface{},
	scope metrics.Scope,
	startTime time.Time,
	domainEntry *cache.DomainCacheEntry,
	cluster string,
	retError *error,
) {
	var extraTags []tag.Tag
	if domainEntry != nil {
		extraTags = append(extraTags, tag.WorkflowDomainName(domainEntry.GetInfo().Name))
		extraTags = append(extraTags, tag.WorkflowDomainID(domainEntry.GetInfo().ID))
	}
	log.CapturePanic(recovered, handler.GetLogger().WithTags(extraTags...), retError)

	scope = scope.Tagged(metrics.TargetClusterTag(cluster))
	scope.IncCounter(metrics.CadenceDcRedirectionClientRequests)
	elapsed := handler.GetTimeSource().Now().Sub(startTime)
	scope.RecordTimer(metrics.CadenceDcRedirectionClientLatency, elapsed)
	scope.ExponentialHistogram(metrics.CadenceDcRedirectionClientLatencyHistogram, elapsed)
	if *retError != nil {
		scope.IncCounter(metrics.CadenceDcRedirectionClientFailures)
	}
}
