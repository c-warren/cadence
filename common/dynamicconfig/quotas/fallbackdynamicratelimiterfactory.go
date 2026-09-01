package quotas

import (
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/quotas"
)

// NewFallbackDynamicRateLimiterFactory is used to create a Limiter for a given domain
// the created Limiter will use the primary dynamic config if it is set
// otherwise it will use the secondary dynamic config
func NewFallbackDynamicRateLimiterFactory(
	primary dynamicproperties.IntPropertyFnWithDomainFilter,
	secondary dynamicproperties.IntPropertyFn,
) quotas.LimiterFactory[string] {
	return fallbackDynamicRateLimiterFactory{
		primary:   primary,
		secondary: secondary,
	}
}

type fallbackDynamicRateLimiterFactory struct {
	primary dynamicproperties.IntPropertyFnWithDomainFilter
	// secondary is used when primary is not set
	secondary dynamicproperties.IntPropertyFn
}

// GetLimiter returns a new Limiter for the given domain
func (f fallbackDynamicRateLimiterFactory) GetLimiter(domain string) quotas.Limiter {
	return quotas.NewDynamicRateLimiter(func() float64 {
		if primaryLimit := f.primary(domain); primaryLimit > 0 {
			return float64(primaryLimit)
		}
		return float64(f.secondary())
	})
}
