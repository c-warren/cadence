package ctxutils

import (
	"context"
	"sync"
)

// WithPropagatedContextCancel returns a copy of parent which is cancelled whenever
// it is cancelled itself (with the returned cancel func) or cancelCtx is cancelled
// you need to call cancel func to avoid potential goroutine leak
func WithPropagatedContextCancel(parent context.Context, cancelCtx context.Context) (context.Context, context.CancelFunc) {
	done := cancelCtx.Done()
	if done == nil {
		return parent, func() {}
	}

	childWithCancel, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case <-done:
			cancel()
		case <-childWithCancel.Done():
		}
	}()

	return childWithCancel, func() {
		cancel()
		wg.Wait()
	}
}
