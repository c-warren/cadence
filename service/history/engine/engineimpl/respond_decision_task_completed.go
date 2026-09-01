package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
)

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(ctx context.Context, req *types.HistoryRespondDecisionTaskCompletedRequest) (*types.HistoryRespondDecisionTaskCompletedResponse, error) {
	return e.decisionHandler.HandleDecisionTaskCompleted(ctx, req)
}
