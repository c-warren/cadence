package accesscontrolled

import (
	"context"
	"time"

	"github.com/uber/cadence/common/authorization"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

var errUnauthorized = &types.AccessDeniedError{Message: "Request unauthorized."}

func (a *adminHandler) isAuthorized(ctx context.Context, attr *authorization.Attributes) (bool, error) {
	result, err := a.authorizer.Authorize(ctx, attr)
	if err != nil {
		return false, err
	}
	isAuth := result.Decision == authorization.DecisionAllow
	return isAuth, nil
}

func (a *apiHandler) isAuthorized(
	ctx context.Context,
	attr *authorization.Attributes,
	scope metrics.Scope,
) (bool, error) {
	authStart := time.Now()
	sw := scope.StartTimer(metrics.CadenceAuthorizationLatency)
	defer func() {
		sw.Stop()
		scope.ExponentialHistogram(metrics.CadenceAuthorizationLatencyHistogram, time.Since(authStart))
	}()

	result, err := a.authorizer.Authorize(ctx, attr)
	if err != nil {
		scope.IncCounter(metrics.CadenceErrAuthorizeFailedCounter)
		return false, err
	}
	isAuth := result.Decision == authorization.DecisionAllow
	if !isAuth {
		scope.IncCounter(metrics.CadenceErrUnauthorizedCounter)
	}
	return isAuth, nil
}

// getMetricsScopeWithDomain return metrics scope with domain tag
func (a *apiHandler) getMetricsScopeWithDomain(
	scope metrics.ScopeIdx,
	domain string,
) metrics.Scope {
	if domain != "" {
		return a.GetMetricsClient().Scope(scope).Tagged(metrics.DomainTag(domain))
	}
	return a.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
}
