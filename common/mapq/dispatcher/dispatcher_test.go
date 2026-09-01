package dispatcher

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	d := New(nil)
	err := d.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = d.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
}
