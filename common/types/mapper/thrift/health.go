package thrift

import (
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/common/types"
)

// FromHealthStatus converts internal HealthStatus type to thrift
func FromHealthStatus(t *types.HealthStatus) *health.HealthStatus {
	if t == nil {
		return nil
	}
	return &health.HealthStatus{
		Ok:  t.Ok,
		Msg: &t.Msg,
	}
}

// ToHealthStatus converts thrift HealthStatus type to internal
func ToHealthStatus(t *health.HealthStatus) *types.HealthStatus {
	if t == nil {
		return nil
	}
	return &types.HealthStatus{
		Ok:  t.Ok,
		Msg: t.GetMsg(),
	}
}
