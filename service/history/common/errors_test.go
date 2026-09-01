package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func TestIsOperationPossiblySuccessfulError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"WorkflowExecutionAlreadyStartedError (types)", &types.WorkflowExecutionAlreadyStartedError{}, false},
		{"WorkflowExecutionAlreadyStartedError (persistence)", &persistence.WorkflowExecutionAlreadyStartedError{}, false},
		{"CurrentWorkflowConditionFailedError", &persistence.CurrentWorkflowConditionFailedError{}, false},
		{"ConditionFailedError", &persistence.ConditionFailedError{}, false},
		{"ServiceBusyError", &types.ServiceBusyError{}, false},
		{"LimitExceededError", &types.LimitExceededError{}, false},
		{"ShardOwnershipLostError", &persistence.ShardOwnershipLostError{}, false},
		{"TimeoutError", &persistence.TimeoutError{}, true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"generic error", assert.AnError, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsOperationPossiblySuccessfulError(tc.err))
		})
	}
}
