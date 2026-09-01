package ratelimited

import (
	"github.com/uber/cadence/common/types"
)

var (
	// ErrPersistenceLimitExceeded is the error indicating QPS limit reached.
	ErrPersistenceLimitExceeded = &types.ServiceBusyError{Message: "Persistence Max QPS Reached."}
)
