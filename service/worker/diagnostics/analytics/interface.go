package analytics

import (
	"context"
	"time"
)

type WfDiagnosticsUsageData struct {
	Domain                string
	WorkflowID            string
	RunID                 string
	Identity              string
	IssueType             string
	DiagnosticsWorkflowID string
	DiagnosticsRunID      string
	Environment           string
	DiagnosticsStartTime  time.Time
	DiagnosticsEndTime    time.Time
	SatisfactionFeedback  bool
}

// DataEmitter is the interface to emit workflow diagnostics data
type DataEmitter interface {
	EmitUsageData(context.Context, WfDiagnosticsUsageData) error
}
