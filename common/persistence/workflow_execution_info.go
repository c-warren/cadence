package persistence

import (
	"fmt"

	"github.com/uber/cadence/common/types"
)

// SetNextEventID sets the nextEventID
func (e *WorkflowExecutionInfo) SetNextEventID(id int64) {
	e.NextEventID = id
}

// IncreaseNextEventID increase the nextEventID by 1
func (e *WorkflowExecutionInfo) IncreaseNextEventID() {
	e.NextEventID++
}

// SetLastFirstEventID set the LastFirstEventID
func (e *WorkflowExecutionInfo) SetLastFirstEventID(id int64) {
	e.LastFirstEventID = id
}

// UpdateWorkflowStateCloseStatus update the workflow state
func (e *WorkflowExecutionInfo) UpdateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	switch e.State {
	case WorkflowStateVoid:
		// no validation
	case WorkflowStateCreated:
		switch state {
		case WorkflowStateCreated:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus != WorkflowCloseStatusTerminated &&
				closeStatus != WorkflowCloseStatusTimedOut &&
				closeStatus != WorkflowCloseStatusContinuedAsNew {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &types.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateRunning:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &types.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateCompleted:
		switch state {
		case WorkflowStateCreated:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateRunning:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		case WorkflowStateCompleted:
			if closeStatus != e.CloseStatus {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

			}
		case WorkflowStateZombie:
			return e.createInvalidStateTransitionErr(e.State, state, closeStatus)

		default:
			return &types.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	case WorkflowStateZombie:
		switch state {
		case WorkflowStateCreated:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateRunning:
			if closeStatus != WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateCompleted:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		case WorkflowStateZombie:
			if closeStatus == WorkflowCloseStatusNone {
				return e.createInvalidStateTransitionErr(e.State, state, closeStatus)
			}

		default:
			return &types.InternalServiceError{
				Message: fmt.Sprintf("unknown workflow state: %v", state),
			}
		}
	default:
		return &types.InternalServiceError{
			Message: fmt.Sprintf("unknown workflow state: %v", state),
		}
	}

	e.State = state
	e.CloseStatus = closeStatus
	return nil

}

func (e *WorkflowExecutionInfo) IsRunning() bool {
	switch e.State {
	case WorkflowStateCreated:
		return true
	case WorkflowStateRunning:
		return true
	case WorkflowStateCompleted:
		return false
	case WorkflowStateZombie:
		return false
	case WorkflowStateCorrupted:
		return false
	default:
		panic(fmt.Sprintf("unknown workflow state: %v", e.State))
	}
}

// UpdateWorkflowStateCloseStatus update the workflow state
func (e *WorkflowExecutionInfo) createInvalidStateTransitionErr(
	currentState int,
	targetState int,
	targetCloseStatus int,
) error {
	return &types.InternalServiceError{
		Message: fmt.Sprintf(invalidStateTransitionMsg, currentState, targetState, targetCloseStatus),
	}
}
