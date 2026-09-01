package common

import (
	"context"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
)

type (
	// EchoActivityParams is the paramer for echoActivity
	EchoActivityParams struct {
		Payload []byte
	}
)

const (
	// EchoActivityName is the name of echoActivity
	EchoActivityName = "echoActivity"
)

// RegisterWorker registers common activities
func RegisterWorker(w worker.Worker) {
	w.RegisterActivityWithOptions(echoActivity, activity.RegisterOptions{Name: EchoActivityName})
}

func echoActivity(ctx context.Context, activityParams EchoActivityParams) ([]byte, error) {
	return activityParams.Payload, nil
}
