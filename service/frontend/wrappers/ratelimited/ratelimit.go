package ratelimited

import (
	"context"
	"time"

	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/types"
)

// ratelimitType differentiates between the three categories of ratelimiters
type ratelimitType int

const (
	ratelimitTypeUser ratelimitType = iota + 1
	ratelimitTypeWorker
	ratelimitTypeWorkerPoll
	ratelimitTypeVisibility
	ratelimitTypeAsync
)

var errRateLimited = types.ServiceBusyError{Message: "Too many outstanding requests to the cadence service"}

func newErrRateLimited() error {
	return &errRateLimited
}

func (h *apiHandler) allowDomain(ctx context.Context, requestType ratelimitType, info quotas.Info) error {
	var policy quotas.Policy
	switch requestType {
	case ratelimitTypeUser:
		policy = h.userRateLimiter
	case ratelimitTypeWorker, ratelimitTypeWorkerPoll:
		policy = h.workerRateLimiter
	case ratelimitTypeVisibility:
		policy = h.visibilityRateLimiter
	case ratelimitTypeAsync:
		policy = h.asyncRateLimiter
	default:
		panic("coding error, unrecognized request ratelimit type value")
	}

	// If it is a poll request and there is a maxWorkerPollDelay configured, use Wait()
	// to potentially wait for a token. Otherwise, use Allow() for an immediate check
	if requestType == ratelimitTypeWorkerPoll {
		waitTime := h.maxWorkerPollDelay(info.Domain)
		if waitTime > 0 {
			return h.waitForPolicy(ctx, waitTime, policy, info)
		}
	}
	if !h.callerBypass.AllowPolicy(ctx, policy, info) {
		return newErrRateLimited()
	}
	return nil
}

func (h *apiHandler) waitForPolicy(ctx context.Context, waitTime time.Duration, policy quotas.Policy, info quotas.Info) error {
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	err := policy.Wait(waitCtx, info)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		waitCtxErr := waitCtx.Err()
		switch waitCtxErr {
		case nil:
			if h.callerBypass.ShouldBypass(ctx) {
				return nil
			}
			return newErrRateLimited()
		case context.DeadlineExceeded:
			// Race condition: context deadline hit right around wait completion
			if !h.callerBypass.AllowPolicy(ctx, policy, info) {
				return newErrRateLimited()
			}
			return nil
		default:
			return waitCtxErr
		}
	}
	return nil
}

func getTaskListName(t *types.TaskList) string {
	if t.GetKind() == types.TaskListKindNormal {
		return t.GetName()
	}
	return t.GetBaseName()
}
