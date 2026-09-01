package workflow

import (
	"context"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
)

var ConditionalRetryCount = 5

type (
	UpdateAction struct {
		Noop           bool
		CreateDecision bool
	}

	UpdateActionFunc func(execution.Context, execution.MutableState) (*UpdateAction, error)
)

var (
	UpdateWithNewDecision = &UpdateAction{
		CreateDecision: true,
	}
	UpdateWithoutDecision = &UpdateAction{
		CreateDecision: false,
	}
)

func LoadOnce(
	ctx context.Context,
	cache execution.Cache,
	domainID string,
	workflowID string,
	runID string,
) (Context, error) {

	wfContext, release, err := cache.GetOrCreateWorkflowExecution(
		ctx,
		domainID,
		types.WorkflowExecution{
			WorkflowID: workflowID,
			RunID:      runID,
		},
	)
	if err != nil {
		return nil, err
	}

	mutableState, err := wfContext.LoadWorkflowExecution(ctx)
	if err != nil {
		release(err)
		return nil, err
	}

	return NewContext(wfContext, release, mutableState), nil
}

func Load(
	ctx context.Context,
	cache execution.Cache,
	executionManager persistence.ExecutionManager,
	shardID int,
	domainID string,
	domainName string,
	workflowID string,
	runID string,
) (Context, error) {

	if runID != "" {
		return LoadOnce(ctx, cache, domainID, workflowID, runID)
	}

	for attempt := 0; attempt < ConditionalRetryCount; attempt++ {

		workflowContext, err := LoadOnce(ctx, cache, domainID, workflowID, "")
		if err != nil {
			return nil, err
		}

		if workflowContext.GetMutableState().IsWorkflowExecutionRunning() {
			return workflowContext, nil
		}

		// workflow not running, need to check current record
		resp, err := executionManager.GetCurrentExecution(
			ctx,
			&persistence.GetCurrentExecutionRequest{
				ShardID:    common.Ptr(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				DomainName: domainName,
			},
		)
		if err != nil {
			workflowContext.GetReleaseFn()(err)
			return nil, err
		}

		if resp.RunID == workflowContext.GetRunID() {
			return workflowContext, nil
		}
		workflowContext.GetReleaseFn()(nil)
	}

	return nil, &types.InternalServiceError{Message: "unable to locate current workflow execution"}
}

// /////////////////  Util function for updating workflows ///////////////////

// UpdateWithActionFunc updates the given workflow execution.
// If runID is empty, it only tries to load the current workflow once.
// If the update should always be applied to the current run, use UpdateCurrentWithActionFunc instead.
func UpdateWithActionFunc(
	ctx context.Context,
	logger log.Logger,
	cache execution.Cache,
	domainID string,
	execution types.WorkflowExecution,
	now time.Time,
	action UpdateActionFunc,
) (retError error) {

	workflowContext, err := LoadOnce(ctx, cache, domainID, execution.GetWorkflowID(), execution.GetRunID())
	if err != nil {
		return err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	return updateHelper(ctx, logger, workflowContext, now, action)
}

// UpdateCurrentWithActionFunc updates the given workflow execution or current execution if runID is empty.
// It's the same as UpdateWithActionFunc if runID is not empty.
// This function is suitable for the case when the change should always be applied to the current execution.
func UpdateCurrentWithActionFunc(
	ctx context.Context,
	logger log.Logger,
	cache execution.Cache,
	executionManager persistence.ExecutionManager,
	shardID int,
	domainID string,
	domainCache cache.DomainCache,
	execution types.WorkflowExecution,
	now time.Time,
	action UpdateActionFunc,
) (retError error) {

	domainName, err := domainCache.GetDomainName(domainID)
	if err != nil {
		return nil
	}
	workflowContext, err := Load(ctx, cache, executionManager, shardID, domainID, domainName, execution.GetWorkflowID(), execution.GetRunID())
	if err != nil {
		return err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	return updateHelper(ctx, logger, workflowContext, now, action)
}

// TODO: deprecate and use UpdateWithActionFunc
func UpdateWithAction(
	ctx context.Context,
	logger log.Logger,
	cache execution.Cache,
	domainID string,
	execution types.WorkflowExecution,
	createDecisionTask bool,
	now time.Time,
	action func(wfContext execution.Context, mutableState execution.MutableState) error,
) error {

	return UpdateWithActionFunc(
		ctx,
		logger,
		cache,
		domainID,
		execution,
		now,
		getUpdateActionFunc(createDecisionTask, action),
	)
}

func getUpdateActionFunc(
	createDecisionTask bool,
	action func(wfContext execution.Context, mutableState execution.MutableState) error,
) UpdateActionFunc {

	return func(wfContext execution.Context, mutableState execution.MutableState) (*UpdateAction, error) {
		err := action(wfContext, mutableState)
		if err != nil {
			return nil, err
		}
		postActions := &UpdateAction{
			CreateDecision: createDecisionTask,
		}
		return postActions, nil
	}
}

func updateHelper(
	ctx context.Context,
	logger log.Logger,
	workflowContext Context,
	now time.Time,
	action UpdateActionFunc,
) (retError error) {

UpdateHistoryLoop:
	for attempt := 0; attempt < ConditionalRetryCount; attempt++ {
		wfContext := workflowContext.GetContext()
		mutableState := workflowContext.GetMutableState()

		// conduct caller action
		postActions, err := action(wfContext, mutableState)
		if err != nil {
			if err == ErrStaleState {
				// Handler detected that cached workflow mutable could potentially be stale
				// Reload workflow execution history
				workflowContext.GetContext().Clear()
				if attempt != ConditionalRetryCount-1 {
					_, err = workflowContext.ReloadMutableState(ctx)
					if err != nil {
						return err
					}
				}
				continue UpdateHistoryLoop
			}

			// Returned error back to the caller
			return err
		}
		if postActions.Noop {
			return nil
		}

		if postActions.CreateDecision {
			// Create a transfer task to schedule a decision task
			if !mutableState.HasPendingDecision() {
				_, err := mutableState.AddDecisionTaskScheduledEvent(false)
				if err != nil {
					return &types.InternalServiceError{Message: "Failed to add decision scheduled event."}
				}
			}
		}

		logger.Debug("updateHelper calling UpdateWorkflowExecutionAsActive", tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowID))
		err = workflowContext.GetContext().UpdateWorkflowExecutionAsActive(ctx, now)
		if _, ok := err.(*persistence.DuplicateRequestError); ok {
			return nil
		}
		if execution.IsConflictError(err) {
			if attempt != ConditionalRetryCount-1 {
				_, err = workflowContext.ReloadMutableState(ctx)
				if err != nil {
					return err
				}
			}
			continue UpdateHistoryLoop
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}
