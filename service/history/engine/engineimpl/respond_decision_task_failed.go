package engineimpl

import (
	"context"

	"github.com/uber/cadence/common/types"
)

// RespondDecisionTaskFailed fails a decision
func (e *historyEngineImpl) RespondDecisionTaskFailed(ctx context.Context, req *types.HistoryRespondDecisionTaskFailedRequest) error {
	return e.decisionHandler.HandleDecisionTaskFailed(ctx, req)
}
