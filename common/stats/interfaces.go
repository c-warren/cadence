//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/common/stats QPSTracker

package stats

import (
	"github.com/uber/cadence/common"
)

// QPSTracker is an interface for reporting statistics related to quotas.
type QPSTracker interface {
	common.Daemon
	// ReportCounter reports the value of a counter.
	ReportCounter(int64)

	// QPS returns the current queries per second (QPS) value.
	QPS() float64
}

// QPSTrackerGroup allows for estimating QPS metrics with an additional dimension
type QPSTrackerGroup interface {
	QPSTracker

	ReportGroup(group string, amount int64)

	GroupQPS(group string) float64
}
