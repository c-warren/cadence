package invariant

import (
	"context"

	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/types"
)

const (
	historyPageSize = 1
)

type (
	historyExists struct {
		pr persistence.Retryer
		dc cache.DomainCache
	}
)

func NewHistoryExists(
	pr persistence.Retryer, dc cache.DomainCache,
) Invariant {
	return &historyExists{
		pr: pr,
		dc: dc,
	}
}

func (h *historyExists) Check(
	ctx context.Context,
	execution interface{},
) CheckResult {
	if checkResult := validateCheckContext(ctx, h.Name()); checkResult != nil {
		return *checkResult
	}

	concreteExecution, ok := execution.(*entity.ConcreteExecution)
	if !ok {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   h.Name(),
			Info:            "failed to check: expected concrete execution",
		}
	}
	domainID := concreteExecution.GetDomainID()
	domainName, errorDomainName := h.dc.GetDomainName(domainID)
	if errorDomainName != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   h.Name(),
			Info:            "failed to check: expected DomainName",
			InfoDetails:     errorDomainName.Error(),
		}
	}
	readHistoryBranchReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   concreteExecution.BranchToken,
		MinEventID:    constants.FirstEventID,
		MaxEventID:    constants.FirstEventID + 1,
		PageSize:      historyPageSize,
		NextPageToken: nil,
		ShardID:       c.IntPtr(concreteExecution.ShardID),
		DomainName:    domainName,
	}
	readHistoryBranchResp, readHistoryBranchErr := h.pr.ReadHistoryBranch(ctx, readHistoryBranchReq)
	stillExists, existsCheckError := ExecutionStillExists(ctx, &concreteExecution.Execution, h.pr, h.dc)
	if existsCheckError != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   h.Name(),
			Info:            "failed to check if concrete execution still exists",
			InfoDetails:     existsCheckError.Error(),
		}
	}
	if !stillExists {
		return CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   h.Name(),
			Info:            "determined execution was healthy because concrete execution no longer exists",
		}
	}
	if readHistoryBranchErr != nil {
		switch readHistoryBranchErr.(type) {
		case *types.EntityNotExistsError:
			return CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   h.Name(),
				Info:            "concrete execution exists but history does not exist",
				InfoDetails:     readHistoryBranchErr.Error(),
			}
		default:
			return CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   h.Name(),
				Info:            "failed to verify if history exists",
				InfoDetails:     readHistoryBranchErr.Error(),
			}
		}
	}
	if readHistoryBranchResp == nil || len(readHistoryBranchResp.HistoryEvents) == 0 {
		return CheckResult{
			CheckResultType: CheckResultTypeCorrupted,
			InvariantName:   h.Name(),
			Info:            "concrete execution exists but got empty history",
		}
	}
	return CheckResult{
		CheckResultType: CheckResultTypeHealthy,
		InvariantName:   h.Name(),
	}
}

func (h *historyExists) Fix(
	ctx context.Context,
	execution interface{},
) FixResult {
	if fixResult := validateFixContext(ctx, h.Name()); fixResult != nil {
		return *fixResult
	}

	fixResult, checkResult := checkBeforeFix(ctx, h, execution)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = DeleteExecution(ctx, execution, h.pr, h.dc)
	fixResult.CheckResult = *checkResult
	fixResult.InvariantName = h.Name()
	return *fixResult
}

func (h *historyExists) Name() Name {
	return HistoryExists
}

// ExecutionStillExists returns true if execution still exists in persistence, false otherwise.
// Returns error on failure to confirm.
func ExecutionStillExists(
	ctx context.Context,
	exec *entity.Execution,
	pr persistence.Retryer,
	dc cache.DomainCache,
) (bool, error) {
	domainName, errorDomainName := dc.GetDomainName(exec.DomainID)
	if errorDomainName != nil {
		return false, errorDomainName
	}
	req := &persistence.GetWorkflowExecutionRequest{
		DomainID: exec.DomainID,
		Execution: types.WorkflowExecution{
			WorkflowID: exec.WorkflowID,
			RunID:      exec.RunID,
		},
		DomainName: domainName,
	}
	_, err := pr.GetWorkflowExecution(ctx, req)
	if err == nil {
		return true, nil
	}
	switch err.(type) {
	case *types.EntityNotExistsError:
		return false, nil
	default:
		return false, err
	}
}
