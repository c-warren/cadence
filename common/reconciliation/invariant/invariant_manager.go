package invariant

import "context"

type (
	invariantManager struct {
		invariants []Invariant
	}
)

// NewInvariantManager handles running a collection of invariants according to the invariant collection provided.
func NewInvariantManager(
	invariants []Invariant,
) Manager {
	return &invariantManager{
		invariants: invariants,
	}
}

// RunChecks runs all enabled checks.
func (i *invariantManager) RunChecks(
	ctx context.Context,
	execution interface{},
) ManagerCheckResult {
	result := ManagerCheckResult{
		CheckResultType:          CheckResultTypeHealthy,
		DeterminingInvariantType: nil,
		CheckResults:             nil,
	}
	for _, iv := range i.invariants {
		checkResult := iv.Check(ctx, execution)
		result.CheckResults = append(result.CheckResults, checkResult)
		checkResultType, updated := i.nextCheckResultType(result.CheckResultType, checkResult.CheckResultType)
		result.CheckResultType = checkResultType
		if updated {
			result.DeterminingInvariantType = &checkResult.InvariantName
		}
	}
	return result
}

// RunFixes runs all enabled fixes.
func (i *invariantManager) RunFixes(
	ctx context.Context,
	execution interface{}) ManagerFixResult {
	result := ManagerFixResult{
		FixResultType:            FixResultTypeSkipped,
		DeterminingInvariantName: nil,
		FixResults:               nil,
	}
	for _, iv := range i.invariants {
		fixResult := iv.Fix(ctx, execution)
		result.FixResults = append(result.FixResults, fixResult)
		fixResultType, updated := i.nextFixResultType(result.FixResultType, fixResult.FixResultType)
		result.FixResultType = fixResultType
		if updated {
			result.DeterminingInvariantName = &fixResult.InvariantName
		}
	}
	return result
}

func (i *invariantManager) nextFixResultType(
	currentState FixResultType,
	event FixResultType,
) (FixResultType, bool) {
	switch currentState {
	case FixResultTypeSkipped:
		return event, event != FixResultTypeSkipped
	case FixResultTypeFixed:
		if event == FixResultTypeFailed {
			return event, true
		}
		return currentState, false
	case FixResultTypeFailed:
		return currentState, false
	default:
		panic("unknown FixResultType")
	}
}

func (i *invariantManager) nextCheckResultType(
	currentState CheckResultType,
	event CheckResultType,
) (CheckResultType, bool) {
	switch currentState {
	case CheckResultTypeHealthy:
		return event, event != CheckResultTypeHealthy
	case CheckResultTypeCorrupted:
		if event == CheckResultTypeFailed {
			return event, true
		}
		return currentState, false
	case CheckResultTypeFailed:
		return currentState, false
	default:
		panic("unknown CheckResultType")
	}
}
