package isolationgroup

import (
	"context"

	"github.com/uber/cadence/common"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination isolation_group_mock.go -self_package github.com/uber/cadence/common/isolationgroup

// State is a heavily cached in-memory library for returning the state of what zones are healthy or
// drained presently. It may return an inclusive (allow-list based) or an exclusive (deny-list based) set of IsolationGroups
// depending on the implementation.
type State interface {
	common.Daemon
	// IsDrained answers the question - "is this particular isolationGroup drained?". Used by startWorkflow calls
	// and similar sync frontend calls to make routing decisions
	IsDrained(ctx context.Context, Domain string, IsolationGroup string) (bool, error)
	IsDrainedByDomainID(ctx context.Context, DomainID string, IsolationGroup string) (bool, error)
}
