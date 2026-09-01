package execution

import "github.com/uber/cadence/common/types"

// TerminateWorkflow is a helper function to terminate workflow
func TerminateWorkflow(
	mutableState MutableState,
	eventBatchFirstEventID int64,
	terminateReason string,
	terminateDetails []byte,
	terminateIdentity string,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := FailDecision(
			mutableState,
			decision,
			types.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
	)
	return err
}
