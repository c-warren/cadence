package domaindeprecation

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/service/worker/batcher"
)

const (
	DomainDeprecationWorkflowTypeName = "domain-deprecation-workflow"
	DomainDeprecationTaskListName     = "domain-deprecation-tasklist"
	domainDeprecationBatchPrefix      = "domain-deprecation-batch"

	checkActivePollersActivity = "checkActivePollers"
	disableArchivalActivity    = "disableArchival"
	deprecateDomainActivity    = "deprecateDomain"
	checkOpenWorkflowsActivity = "checkOpenWorkflows"

	workflowStartToCloseTimeout     = time.Hour * 24 * 30
	workflowTaskStartToCloseTimeout = 5 * time.Minute

	// MaxBatchWorkflowAttempts is the maximum number of attempts to terminate open workflows
	MaxBatchWorkflowAttempts = 3

	// VisibilityRefreshWaitTime is the time to wait for visibility storage to refresh after workflow termination
	VisibilityRefreshWaitTime = 3 * time.Minute
)

var (
	retryPolicy = cadence.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
		ExpirationInterval: 24 * time.Hour,
		NonRetriableErrorReasons: []string{
			ErrDomainDoesNotExistNonRetryable,
			ErrAccessDeniedNonRetryable,
			ErrActivePollersNonRetryable,
		},
	}

	activityOptions = workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       30 * time.Second,
		RetryPolicy:            &retryPolicy,
	}
)

// DomainDeprecationWorkflow is the workflow that handles domain deprecation process
func (w *domainDeprecator) DomainDeprecationWorkflow(ctx workflow.Context, params DomainDeprecationParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting domain deprecation workflow", zap.String("domain", params.DomainName))

	// Step 1: Check for active pollers (skipped when Force is set)
	if !params.Force {
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, activityOptions),
			w.CheckActivePollersActivity,
			params,
		).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// Step 2: Activity disables archival
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, activityOptions),
		w.DisableArchivalActivity,
		params,
	).Get(ctx, nil)
	if err != nil {
		return err
	}

	// Step 3: Deprecate a domain
	err = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, activityOptions),
		w.DeprecateDomainActivity,
		params,
	).Get(ctx, nil)
	if err != nil {
		return err
	}

	// Step 4: Start child batch workflow to terminate open workflows of a domain
	batchParams := batcher.BatchParams{
		DomainName: params.DomainName,
		Query:      "CloseTime = missing",
		Reason:     "domain is deprecated",
		BatchType:  batcher.BatchTypeTerminate,
		TerminateParams: batcher.TerminateParams{
			TerminateChildren: common.BoolPtr(true),
		},
		RPS:                      DefaultRPS,
		Concurrency:              DefaultConcurrency,
		PageSize:                 DefaultPageSize,
		AttemptsOnRetryableError: DefaultAttemptsOnRetryableError,
		ActivityHeartBeatTimeout: DefaultActivityHeartBeatTimeout,
		MaxActivityRetries:       DefaultMaxActivityRetries,
		NonRetryableErrors: []string{
			ErrAccessDeniedNonRetryable,
			ErrWorkflowAlreadyCompletedNonRetryable,
		},
	}

	for attempt := 1; attempt <= MaxBatchWorkflowAttempts; attempt++ {
		logger.Info("Starting batch workflow",
			zap.String("domain", params.DomainName),
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", MaxBatchWorkflowAttempts))

		workflowID := uuid.New()
		childWorkflowOptions := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:                   fmt.Sprintf("%s-%s-%s-%d", domainDeprecationBatchPrefix, params.DomainName, workflowID, attempt),
			ExecutionStartToCloseTimeout: workflowStartToCloseTimeout,
			TaskStartToCloseTimeout:      workflowTaskStartToCloseTimeout,
		})

		var result batcher.HeartBeatDetails
		err = workflow.ExecuteChildWorkflow(childWorkflowOptions, batcher.BatchWorkflowV2, batchParams).Get(ctx, &result)
		if err != nil {
			return fmt.Errorf("batch workflow failed on attempt %d: %v", attempt, err)
		}

		// Wait for visibility storage to refresh
		err = workflow.Sleep(ctx, VisibilityRefreshWaitTime)
		if err != nil {
			return fmt.Errorf("workflow sleep failed on attempt %d: %v", attempt, err)
		}

		// Check if there are still open workflows
		var hasOpenWorkflows bool
		err = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, activityOptions),
			w.CheckOpenWorkflowsActivity,
			params,
		).Get(ctx, &hasOpenWorkflows)
		if err != nil {
			return fmt.Errorf("failed to check open workflows on attempt %d: %v", attempt, err)
		}

		if !hasOpenWorkflows {
			logger.Info("No open workflows found after batch workflow",
				zap.String("domain", params.DomainName),
				zap.Int("attempt", attempt))
			break
		}

		if attempt == MaxBatchWorkflowAttempts {
			return fmt.Errorf("failed to terminate all workflows after %d attempts", MaxBatchWorkflowAttempts)
		}

		logger.Info("Found open workflows after batch workflow, will retry",
			zap.String("domain", params.DomainName),
			zap.Int("attempt", attempt))
	}

	logger.Info("Domain deprecation workflow completed successfully", zap.String("domain", params.DomainName))
	return nil
}
