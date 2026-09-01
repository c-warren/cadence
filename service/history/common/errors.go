package common

import (
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// IsOperationPossiblySuccessfulError returns true for errors where a persistence
// write may have succeeded despite the error being returned (e.g. timeout, unknown
// network error). Returns false for errors that definitively indicate the write
// did not occur.
func IsOperationPossiblySuccessfulError(err error) bool {
	switch err.(type) {
	case nil,
		*types.WorkflowExecutionAlreadyStartedError,
		*persistence.WorkflowExecutionAlreadyStartedError,
		*persistence.CurrentWorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*types.ServiceBusyError,
		*types.LimitExceededError,
		*persistence.ShardOwnershipLostError:
		return false
	default:
		return true
	}
}
