package invariant

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/types"
)

type (
	openCurrentExecution struct {
		pr persistence.Retryer
		dc cache.DomainCache
	}
)

// NewOpenCurrentExecution returns a new invariant for checking open current execution
func NewOpenCurrentExecution(
	pr persistence.Retryer, dc cache.DomainCache,
) Invariant {
	return &openCurrentExecution{
		pr: pr,
		dc: dc,
	}
}

func (o *openCurrentExecution) Check(
	ctx context.Context,
	execution interface{},
) CheckResult {
	if checkResult := validateCheckContext(ctx, o.Name()); checkResult != nil {
		return *checkResult
	}

	concreteExecution, ok := execution.(*entity.ConcreteExecution)
	if !ok {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   o.Name(),
			Info:            "failed to check: expected concrete execution",
		}
	}
	if !Open(concreteExecution.State) {
		return CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   o.Name(),
		}
	}
	domainName, err := o.dc.GetDomainName(concreteExecution.DomainID)
	if err != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   o.Name(),
			Info:            "failed to fetch Domain Name",
			InfoDetails:     err.Error(),
		}
	}
	currentExecResp, currentExecErr := o.pr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		DomainID:   concreteExecution.DomainID,
		WorkflowID: concreteExecution.WorkflowID,
		DomainName: domainName,
	})

	stillOpen, stillOpenErr := ExecutionStillOpen(ctx, &concreteExecution.Execution, o.pr, o.dc)
	if stillOpenErr != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   o.Name(),
			Info:            "failed to check if concrete execution is still open",
			InfoDetails:     stillOpenErr.Error(),
		}
	}
	if !stillOpen {
		return CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   o.Name(),
		}
	}
	if currentExecErr != nil {
		switch currentExecErr.(type) {
		case *types.EntityNotExistsError:
			return CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   o.Name(),
				Info:            "execution is open without having current execution",
				InfoDetails:     currentExecErr.Error(),
			}
		default:
			return CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   o.Name(),
				Info:            "failed to check if current execution exists",
				InfoDetails:     currentExecErr.Error(),
			}
		}
	}
	if currentExecResp.RunID != concreteExecution.RunID {
		return CheckResult{
			CheckResultType: CheckResultTypeCorrupted,
			InvariantName:   o.Name(),
			Info:            "execution is open but current points at a different execution",
			InfoDetails:     fmt.Sprintf("current points at %v", currentExecResp.RunID),
		}
	}
	return CheckResult{
		CheckResultType: CheckResultTypeHealthy,
		InvariantName:   o.Name(),
	}
}

func (o *openCurrentExecution) Fix(
	ctx context.Context,
	execution interface{},
) FixResult {
	if fixResult := validateFixContext(ctx, o.Name()); fixResult != nil {
		return *fixResult
	}

	fixResult, checkResult := checkBeforeFix(ctx, o, execution)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = DeleteExecution(ctx, execution, o.pr, o.dc)
	fixResult.CheckResult = *checkResult
	fixResult.InvariantName = o.Name()
	return *fixResult
}

func (o *openCurrentExecution) Name() Name {
	return OpenCurrentExecution
}

// ExecutionStillOpen returns true if execution in persistence exists and is open, false otherwise.
// Returns error on failure to confirm.
func ExecutionStillOpen(
	ctx context.Context,
	exec *entity.Execution,
	pr persistence.Retryer,
	dc cache.DomainCache,
) (bool, error) {
	domainName, err := dc.GetDomainName(exec.DomainID)
	if err != nil {
		return false, err
	}
	req := &persistence.GetWorkflowExecutionRequest{
		DomainID: exec.DomainID,
		Execution: types.WorkflowExecution{
			WorkflowID: exec.WorkflowID,
			RunID:      exec.RunID,
		},
		DomainName: domainName,
	}
	resp, err := pr.GetWorkflowExecution(ctx, req)
	if err != nil {
		switch err.(type) {
		case *types.EntityNotExistsError:
			return false, nil
		default:
			return false, err
		}
	}
	return Open(resp.State.ExecutionInfo.State), nil
}
