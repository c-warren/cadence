package ctxutils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestWithPropagatedContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	cancelCtx, cancelFn := context.WithCancel(context.Background())

	ctx, stopper := WithPropagatedContextCancel(context.Background(), cancelCtx)
	defer stopper()

	cancelFn() // cancel should be propagated to "chained" ctx

	testTimeout := time.NewTimer(10 * time.Second)
	cancelledOriginalContext := false
	select {
	case <-ctx.Done():
		cancelledOriginalContext = true
	case <-testTimeout.C:
		t.Error("test timed out")
	}

	require.True(t, cancelledOriginalContext)
}

func TestWithPropagatedContextCancelWorksWhenContextHasNoCancellation(t *testing.T) {
	assert.NotPanics(t, func() {
		WithPropagatedContextCancel(context.Background(), context.Background())
	})

	goleak.VerifyNone(t)
}

func TestWithPropagatedContextCancelWorksWhenParentContextIsCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	cancelCtx1, cancelFn1 := context.WithCancel(context.Background())
	cancelCtx2, _ := context.WithCancel(context.Background())

	ctx, stopper := WithPropagatedContextCancel(cancelCtx1, cancelCtx2)
	defer stopper()

	// in this case there is no need to wait for the cancelCtx to cancel
	// since the parent context is closed before
	cancelFn1()

	testTimeout := time.NewTimer(10 * time.Second)
	select {
	case <-ctx.Done():
	case <-testTimeout.C:
		t.Error("sanity check #1 - we expect ctx to be cancelled")
	}

	select {
	case <-cancelCtx1.Done():
	case <-testTimeout.C:
		t.Error("sanity check #2 - we expect ctx to be cancelled")
	}
}

func TestWithPropagatedContextSanityCheckOriginalContextNeverAffected(t *testing.T) {
	defer goleak.VerifyNone(t)
	originalCtx, _ := context.WithCancel(context.Background())
	cancelCtx, cancelFn := context.WithCancel(context.Background())

	ctx, stopper := WithPropagatedContextCancel(originalCtx, cancelCtx)

	cancelFn() // cancel should be propagated to "chained" ctx
	stopper()

	testTimeout := time.NewTimer(10 * time.Second)

	// cancellation of ctx is expected
	select {
	case <-ctx.Done():
		break
	case <-testTimeout.C:
		t.Error("test timed out")
	}

	// ... but cancellation of originalCtx is not
	// actually this is guaranteed by Go (contexts are immutable), but let's check anyway
	select {
	case <-originalCtx.Done():
		t.Error("originalCtx should not be cancelled")
	default:
	}
}
