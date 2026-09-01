package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
)

// RecordDecisionTaskStarted starts a decision
func (e *historyEngineImpl) RecordDecisionTaskStarted(ctx context.Context, request *types.RecordDecisionTaskStartedRequest) (*types.RecordDecisionTaskStartedResponse, error) {
	return e.decisionHandler.HandleDecisionTaskStarted(ctx, request)
}
