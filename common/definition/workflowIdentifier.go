package definition

import (
	"github.com/uber/cadence/common/constants"
)

type (
	// WorkflowIdentifier is the combinations which represent a workflow
	WorkflowIdentifier struct {
		DomainID   string
		WorkflowID string
		RunID      string
	}
)

// Size calculates the size in bytes of the WorkflowIdentifier struct.
func (wi *WorkflowIdentifier) ByteSize() uint64 {
	// Calculate the size of strings in bytes, we assume that all those fields are using ASCII which is 1 byte per char
	size := len(wi.DomainID) + len(wi.WorkflowID) + len(wi.RunID)
	// Each string internally holds a reference pointer and a length, which are 8 bytes each
	stringOverhead := 3 * constants.StringSizeOverheadBytes
	return uint64(size + stringOverhead)
}

// NewWorkflowIdentifier create a new WorkflowIdentifier
func NewWorkflowIdentifier(domainID string, workflowID string, runID string) WorkflowIdentifier {
	return WorkflowIdentifier{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}
}
