package quotas

import (
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/quotas"
)

// NewSimpleDynamicRateLimiterFactory creates a new LimiterFactory which creates
// a new DynamicRateLimiter for each domain, the RPS for the DynamicRateLimiter is given by the dynamic config
func NewSimpleDynamicRateLimiterFactory(rps dynamicproperties.IntPropertyFnWithDomainFilter) quotas.LimiterFactory[string] {
	return dynamicRateLimiterFactory{
		rps: rps,
	}
}

type dynamicRateLimiterFactory struct {
	rps dynamicproperties.IntPropertyFnWithDomainFilter
}

// GetLimiter returns a new Limiter for the given domain
func (f dynamicRateLimiterFactory) GetLimiter(domain string) quotas.Limiter {
	return quotas.NewDynamicRateLimiter(func() float64 { return float64(f.rps(domain)) })
}
