package permember

import (
	"math"

	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/quotas"
)

// PerMember allows creating per instance RPS based on globalRPS averaged by member count for a given service.
// If member count can not be retrieved or globalRPS is not provided it falls back to instanceRPS.
func PerMember(service string, globalRPS, instanceRPS float64, resolver membership.Resolver) float64 {
	if globalRPS <= 0 {
		return instanceRPS
	}

	memberCount, err := resolver.MemberCount(service)
	if err != nil || memberCount < 1 {
		return instanceRPS
	}

	avgQuota := math.Max(globalRPS/float64(memberCount), 1)
	return math.Min(avgQuota, instanceRPS)
}

// NewPerMemberDynamicRateLimiterFactory creates a new LimiterFactory which creates
// a new DynamicRateLimiter for each domain, the RPS for the DynamicRateLimiter is given
// by the globalRPS and averaged by member count for a given service.
// instanceRPS is used as a fallback if globalRPS is not provided.
func NewPerMemberDynamicRateLimiterFactory(
	service string,
	globalRPS func(key string) int,
	instanceRPS func(key string) int,
	resolver membership.Resolver,
) quotas.LimiterFactory[string] {
	return perMemberFactory{
		service:     service,
		globalRPS:   globalRPS,
		instanceRPS: instanceRPS,
		resolver:    resolver,
	}
}

type perMemberFactory struct {
	service     string
	globalRPS   func(key string) int
	instanceRPS func(key string) int
	resolver    membership.Resolver
}

func (f perMemberFactory) GetLimiter(key string) quotas.Limiter {
	return quotas.NewDynamicRateLimiter(func() float64 {
		return PerMember(
			f.service,
			float64(f.globalRPS(key)),
			float64(f.instanceRPS(key)),
			f.resolver,
		)
	})
}
