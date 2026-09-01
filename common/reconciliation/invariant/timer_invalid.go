package invariant

import (
	"context"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/types"
)

type timerInvalid struct {
	pr    persistence.Retryer
	cache cache.DomainCache
}

// NewTimerInvalid returns a new timer invalid invariant
func NewTimerInvalid(
	pr persistence.Retryer, cache cache.DomainCache,
) Invariant {
	return &timerInvalid{
		pr:    pr,
		cache: cache,
	}
}

// Check checks if timer is scheduled for open execution
func (h *timerInvalid) Check(
	ctx context.Context,
	e interface{},
) CheckResult {
	if checkResult := validateCheckContext(ctx, h.Name()); checkResult != nil {
		return *checkResult
	}

	timer, ok := e.(*entity.Timer)

	if !ok {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   h.Name(),
			Info:            "failed to check: expected timer entity",
		}
	}
	domainID := timer.DomainID
	domainName, err := h.cache.GetDomainName(timer.DomainID)
	if err != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   h.Name(),
			Info:            "failed to check: expected Domain Name",
		}
	}
	req := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: types.WorkflowExecution{
			WorkflowID: timer.WorkflowID,
			RunID:      timer.RunID,
		},
		DomainName: domainName,
	}

	resp, err := h.pr.GetWorkflowExecution(ctx, req)

	if err != nil {
		switch err.(type) {
		case *types.EntityNotExistsError:
			return CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   h.Name(),
				Info:            "timer scheduled for non existing workflow",
			}
		default:
			return CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   h.Name(),
				Info:            "failed to get workflow for timer",
			}
		}
	}

	if !Open(resp.State.ExecutionInfo.State) {
		return CheckResult{
			CheckResultType: CheckResultTypeCorrupted,
			InvariantName:   h.Name(),
			Info:            "timer scheduled for closed workflow",
		}
	}

	return CheckResult{
		CheckResultType: CheckResultTypeHealthy,
		InvariantName:   h.Name(),
	}
}

// Fix will delete invalid timer
func (h *timerInvalid) Fix(
	ctx context.Context,
	e interface{},
) FixResult {

	if fixResult := validateFixContext(ctx, h.Name()); fixResult != nil {
		return *fixResult
	}

	fixResult, checkResult := checkBeforeFix(ctx, h, e)
	if fixResult != nil {
		return *fixResult
	}

	timer, _ := e.(*entity.Timer)

	if timer.TaskType != persistence.TaskTypeUserTimer {
		return FixResult{
			FixResultType: FixResultTypeSkipped,
			InvariantName: h.Name(),
			Info:          "timer is not a TaskTypeUserTimer",
		}
	}

	req := persistence.CompleteHistoryTaskRequest{
		TaskCategory: persistence.HistoryTaskCategoryTimer,
		TaskKeys: []persistence.HistoryTaskKey{
			persistence.NewHistoryTaskKey(
				timer.VisibilityTimestamp,
				timer.TaskID,
			),
		},
	}

	if err := h.pr.CompleteHistoryTask(ctx, &req); err != nil {
		return FixResult{
			FixResultType: FixResultTypeFailed,
			InvariantName: h.Name(),
			Info:          err.Error(),
		}
	}

	return FixResult{
		FixResultType: FixResultTypeFixed,
		InvariantName: h.Name(),
		CheckResult:   *checkResult,
	}
}

func (h *timerInvalid) Name() Name {
	return TimerInvalid
}
