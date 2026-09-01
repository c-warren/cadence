package authorization

import "context"

type nopAuthority struct{}

// NewNopAuthorizer creates a no-op authority
func NewNopAuthorizer() (Authorizer, error) {
	return &nopAuthority{}, nil
}

func (a *nopAuthority) Authorize(
	ctx context.Context,
	attributes *Attributes,
) (Result, error) {
	return Result{Decision: DecisionAllow}, nil
}
