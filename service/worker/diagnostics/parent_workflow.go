package diagnostics

import (
	"fmt"
	"time"

	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/service/worker/diagnostics/analytics"
)

const (
	diagnosticsStarterWorkflow = "diagnostics-starter-workflow"
	emitUsageLogsActivity      = "emitUsageLogs"
	queryDiagnosticsReport     = "query-diagnostics-report"

	issueTypeTimeouts = "Timeout"
	issueTypeFailures = "Failure"
	issueTypeRetry    = "Retry"
)

type DiagnosticsStarterWorkflowInput struct {
	Domain     string
	Identity   string
	WorkflowID string
	RunID      string
}

type DiagnosticsStarterWorkflowResult struct {
	DiagnosticsResult    *DiagnosticsWorkflowResult
	DiagnosticsCompleted bool
}

func (w *dw) DiagnosticsStarterWorkflow(ctx workflow.Context, params DiagnosticsStarterWorkflowInput) (*DiagnosticsStarterWorkflowResult, error) {
	var diagWfResult DiagnosticsWorkflowResult
	workflowResult := DiagnosticsStarterWorkflowResult{
		DiagnosticsResult: &diagWfResult,
	}
	err := workflow.SetQueryHandler(ctx, queryDiagnosticsReport, func() (DiagnosticsStarterWorkflowResult, error) {
		return workflowResult, nil
	})
	if err != nil {
		return nil, err
	}

	future := workflow.ExecuteChildWorkflow(ctx, w.DiagnosticsWorkflow, DiagnosticsWorkflowInput{
		Domain:     params.Domain,
		WorkflowID: params.WorkflowID,
		RunID:      params.RunID,
	})

	var childWfExec workflow.Execution
	var childWfStart, childWfEnd time.Time
	if err = future.GetChildWorkflowExecution().Get(ctx, &childWfExec); err != nil {
		return nil, fmt.Errorf("Workflow Diagnostics start failed: %w", err)
	}
	childWfStart = workflow.Now(ctx)

	err = future.Get(ctx, &diagWfResult)
	if err != nil {
		return nil, fmt.Errorf("Workflow Diagnostics failed: %w", err)
	}
	workflowResult.DiagnosticsCompleted = true
	childWfEnd = workflow.Now(ctx)

	info := workflow.GetInfo(ctx)
	activityOptions := workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Second * 10,
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Second * 5,
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)
	err = workflow.ExecuteActivity(activityCtx, emitUsageLogsActivity, analytics.WfDiagnosticsUsageData{
		Domain:                params.Domain,
		WorkflowID:            params.WorkflowID,
		RunID:                 params.RunID,
		Identity:              params.Identity,
		IssueType:             getIssueType(diagWfResult),
		Environment:           w.clusterMetadata.GetCurrentClusterName(),
		DiagnosticsWorkflowID: childWfExec.ID,
		DiagnosticsRunID:      childWfExec.RunID,
		DiagnosticsStartTime:  childWfStart,
		DiagnosticsEndTime:    childWfEnd,
	}).Get(ctx, nil)
	if err != nil {
		w.logger.Error("wf-diagnostics usage logs emission failed",
			tag.Error(err),
			tag.WorkflowID(info.WorkflowExecution.ID),
			tag.WorkflowRunID(info.WorkflowExecution.RunID))
	}

	return &workflowResult, nil
}

func getIssueType(result DiagnosticsWorkflowResult) string {
	var issueType string
	if result.Timeouts != nil {
		issueType = fmt.Sprintf("%s-%s", issueType, issueTypeTimeouts)
	}
	if result.Failures != nil {
		issueType = fmt.Sprintf("%s-%s", issueType, issueTypeFailures)
	}
	if result.Retries != nil {
		issueType = fmt.Sprintf("%s-%s", issueType, issueTypeRetry)
	}
	return issueType
}
