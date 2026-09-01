package invariant

import (
	"context"
	"encoding/json"

	"github.com/uber/cadence/common/types"
)

// InvariantCheckResult is the result from the invariant check
type InvariantCheckResult struct {
	IssueID       int
	InvariantType string
	Reason        string
	Metadata      []byte
}

// InvariantRootCauseResult is the root cause for the issues identified in the invariant check
type InvariantRootCauseResult struct {
	IssueID   int
	RootCause RootCause
	Metadata  []byte
}

type InvariantCheckInput struct {
	WorkflowExecutionHistory *types.GetWorkflowExecutionHistoryResponse
	Domain                   string
}

type InvariantRootCauseInput struct {
	Domain string
	Issues []InvariantCheckResult
}
type RootCause string

const (
	RootCauseTypeMissingPollers                        RootCause = "There are no pollers for the tasklist"
	RootCauseTypePollersStatus                         RootCause = "There are pollers for the tasklist. Check backlog status"
	RootCauseTypeNoHeartBeatTimeoutNoRetryPolicy       RootCause = "Heartbeat timeout and retry policy are not configured"
	RootCauseTypeHeartBeatingNotEnabledWithRetryPolicy RootCause = "Heartbeat timeout not enabled for activity but there is a retry policy configured"
	RootCauseTypeHeartBeatingEnabledWithoutRetryPolicy RootCause = "Heartbeat timeout enabled for activity but there is no retry policy configured"
	RootCauseTypeHeartBeatingEnabledMissingHeartbeat   RootCause = "Heartbeat timeout enabled for activity but timed out due to missing heartbeat"
	RootCauseTypeServiceSideIssue                      RootCause = "There is an issue in the worker service that is causing a failure. Check identity for service logs"
	RootCauseTypeServiceSidePanic                      RootCause = "There is a panic in the activity/workflow that is causing a failure"
	RootCauseTypeServiceSideCustomError                RootCause = "Customised error returned by the activity/workflow"
	RootCauseTypeBlobSizeLimit                         RootCause = "Workflow has exceeded the blob size limits configured for the domain"
)

func (r RootCause) String() string {
	return string(r)
}

// Invariant represents a condition of a workflow execution.
type Invariant interface {
	Check(context.Context, InvariantCheckInput) ([]InvariantCheckResult, error)
	RootCause(context.Context, InvariantRootCauseInput) ([]InvariantRootCauseResult, error)
}

func MarshalData(rc any) []byte {
	data, _ := json.Marshal(rc)
	return data
}
