package workflow

import (
	"context"

	"github.com/uber/cadence/service/history/execution"
)

type (
	// Context is an helper interface on top of execution.Context
	Context interface {
		GetContext() execution.Context
		GetMutableState() execution.MutableState
		ReloadMutableState(ctx context.Context) (execution.MutableState, error)
		GetReleaseFn() execution.ReleaseFunc
		GetWorkflowID() string
		GetRunID() string
	}

	contextImpl struct {
		context      execution.Context
		mutableState execution.MutableState
		releaseFn    execution.ReleaseFunc
	}
)

// NewContext creates a new helper instance on top of execution.Context
func NewContext(
	context execution.Context,
	releaseFn execution.ReleaseFunc,
	mutableState execution.MutableState,
) Context {

	return &contextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func (w *contextImpl) GetContext() execution.Context {
	return w.context
}

func (w *contextImpl) GetMutableState() execution.MutableState {
	return w.mutableState
}

func (w *contextImpl) ReloadMutableState(ctx context.Context) (execution.MutableState, error) {
	mutableState, err := w.GetContext().LoadWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}
	w.mutableState = mutableState
	return mutableState, nil
}

func (w *contextImpl) GetReleaseFn() execution.ReleaseFunc {
	return w.releaseFn
}

func (w *contextImpl) GetWorkflowID() string {
	return w.GetContext().GetExecution().GetWorkflowID()
}

func (w *contextImpl) GetRunID() string {
	return w.GetContext().GetExecution().GetRunID()
}
