package quotas

import (
	"context"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/types"
)

// CallerBypass encapsulates the logic for bypassing rate limits based on caller type
type CallerBypass struct {
	bypassCallerTypes dynamicproperties.ListPropertyFn
}

// NewCallerBypass creates a new CallerBypass with the given bypass caller types configuration
func NewCallerBypass(bypassCallerTypes dynamicproperties.ListPropertyFn) CallerBypass {
	return CallerBypass{
		bypassCallerTypes: bypassCallerTypes,
	}
}

// AllowLimiter checks if a request should be allowed through a Limiter.
// It first checks the limiter's Allow() method, and if that returns false,
// it checks if the caller type should bypass rate limiting.
func (c CallerBypass) AllowLimiter(ctx context.Context, limiter Limiter) bool {
	if limiter.Allow() {
		return true
	}
	return c.ShouldBypass(ctx)
}

// AllowPolicy checks if a request should be allowed through a Policy.
// It first checks the policy's Allow() method, and if that returns false,
// it checks if the caller type should bypass rate limiting.
func (c CallerBypass) AllowPolicy(ctx context.Context, policy Policy, info Info) bool {
	if policy.Allow(info) {
		return true
	}
	return c.ShouldBypass(ctx)
}

// ShouldBypass checks if the caller type from the context should bypass rate limiting
// based on the configured bypass caller types.
func (c CallerBypass) ShouldBypass(ctx context.Context) bool {
	if c.bypassCallerTypes == nil {
		return false
	}

	callerInfo := types.GetCallerInfoFromContext(ctx)
	bypassCallerTypes := c.bypassCallerTypes()

	for _, bypassType := range bypassCallerTypes {
		if bypassTypeStr, ok := bypassType.(string); ok {
			if types.ParseCallerType(bypassTypeStr) == callerInfo.GetCallerType() {
				return true
			}
		}
	}
	return false
}
