package persistence

import (
	"fmt"

	"github.com/uber/cadence/common/types"
)

var (
	validWorkflowStates = map[int]struct{}{
		WorkflowStateCreated:   {},
		WorkflowStateRunning:   {},
		WorkflowStateCompleted: {},
		WorkflowStateZombie:    {},
		WorkflowStateCorrupted: {},
	}

	validWorkflowCloseStatuses = map[int]struct{}{
		WorkflowCloseStatusNone:           {},
		WorkflowCloseStatusCompleted:      {},
		WorkflowCloseStatusFailed:         {},
		WorkflowCloseStatusCanceled:       {},
		WorkflowCloseStatusTerminated:     {},
		WorkflowCloseStatusContinuedAsNew: {},
		WorkflowCloseStatusTimedOut:       {},
	}
)

// ValidateCreateWorkflowStateCloseStatus validate workflow state and close status
func ValidateCreateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowCloseStatus(closeStatus); err != nil {
		return err
	}

	// validate workflow state & close status
	if state == WorkflowStateCompleted || closeStatus != WorkflowCloseStatusNone {
		return &types.InternalServiceError{
			Message: fmt.Sprintf("Create workflow with invalid state: %v or close status: %v",
				state, closeStatus),
		}
	}
	return nil
}

// ValidateUpdateWorkflowStateCloseStatus validate workflow state and close status
func ValidateUpdateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	if err := validateWorkflowState(state); err != nil {
		return err
	}
	if err := validateWorkflowCloseStatus(closeStatus); err != nil {
		return err
	}

	// validate workflow state & close status
	if closeStatus == WorkflowCloseStatusNone {
		if state == WorkflowStateCompleted {
			return &types.InternalServiceError{
				Message: fmt.Sprintf("Update workflow with invalid state: %v or close status: %v",
					state, closeStatus),
			}
		}
	} else {
		// WorkflowCloseStatusCompleted
		// WorkflowCloseStatusFailed
		// WorkflowCloseStatusCanceled
		// WorkflowCloseStatusTerminated
		// WorkflowCloseStatusContinuedAsNew
		// WorkflowCloseStatusTimedOut
		if state != WorkflowStateCompleted {
			return &types.InternalServiceError{
				Message: fmt.Sprintf("Update workflow with invalid state: %v or close status: %v",
					state, closeStatus),
			}
		}
	}
	return nil
}

// validateWorkflowState validate workflow state
func validateWorkflowState(
	state int,
) error {

	if _, ok := validWorkflowStates[state]; !ok {
		return &types.InternalServiceError{
			Message: fmt.Sprintf("Invalid workflow state: %v", state),
		}
	}

	return nil
}

// validateWorkflowCloseStatus validate workflow close status
func validateWorkflowCloseStatus(
	closeStatus int,
) error {

	if _, ok := validWorkflowCloseStatuses[closeStatus]; !ok {
		return &types.InternalServiceError{
			Message: fmt.Sprintf("Invalid workflow close status: %v", closeStatus),
		}
	}

	return nil
}

// ToInternalWorkflowExecutionCloseStatus convert persistence representation of close status to internal representation
func ToInternalWorkflowExecutionCloseStatus(
	closeStatus int,
) *types.WorkflowExecutionCloseStatus {

	switch closeStatus {
	case WorkflowCloseStatusNone:
		return nil
	case WorkflowCloseStatusCompleted:
		return types.WorkflowExecutionCloseStatusCompleted.Ptr()
	case WorkflowCloseStatusFailed:
		return types.WorkflowExecutionCloseStatusFailed.Ptr()
	case WorkflowCloseStatusCanceled:
		return types.WorkflowExecutionCloseStatusCanceled.Ptr()
	case WorkflowCloseStatusTerminated:
		return types.WorkflowExecutionCloseStatusTerminated.Ptr()
	case WorkflowCloseStatusContinuedAsNew:
		return types.WorkflowExecutionCloseStatusContinuedAsNew.Ptr()
	case WorkflowCloseStatusTimedOut:
		return types.WorkflowExecutionCloseStatusTimedOut.Ptr()
	default:
		panic("Invalid value for enum WorkflowExecutionCloseStatus")
	}
}

// FromInternalWorkflowExecutionCloseStatus convert internal representation of close status to persistence representation
func FromInternalWorkflowExecutionCloseStatus(
	closeStatus *types.WorkflowExecutionCloseStatus,
) int {
	if closeStatus == nil {
		return WorkflowCloseStatusNone
	}
	switch *closeStatus {
	case types.WorkflowExecutionCloseStatusCompleted:
		return WorkflowCloseStatusCompleted
	case types.WorkflowExecutionCloseStatusFailed:
		return WorkflowCloseStatusFailed
	case types.WorkflowExecutionCloseStatusCanceled:
		return WorkflowCloseStatusCanceled
	case types.WorkflowExecutionCloseStatusTerminated:
		return WorkflowCloseStatusTerminated
	case types.WorkflowExecutionCloseStatusContinuedAsNew:
		return WorkflowCloseStatusContinuedAsNew
	case types.WorkflowExecutionCloseStatusTimedOut:
		return WorkflowCloseStatusTimedOut
	default:
		panic("Invalid value for enum WorkflowExecutionCloseStatus")
	}
}
