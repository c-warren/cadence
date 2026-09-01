package metered

import (
	"context"

	"github.com/uber/cadence/common/backoff"
)

func getRetryCountFromContext(ctx context.Context) int {
	return backoff.GetRetryCountFromContext(ctx)
}
