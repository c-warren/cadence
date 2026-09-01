package query

import (
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/simulation/replication/types"
)

const (
	signalName       = "custom-signal"
	clusterNameQuery = "cluster-name"
	signalCountQuery = "signal-count"
)

type Runner struct {
	ClusterName string
}

func (r *Runner) Workflow(ctx workflow.Context, input types.WorkflowInput) (types.WorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Sugar().Infof("query workflow started with input: %+v", input)

	err := workflow.SetQueryHandler(ctx, clusterNameQuery, func() (string, error) {
		logger.Sugar().Infof("query handler called. returning cluster name: %s", r.ClusterName)
		return r.ClusterName, nil
	})
	if err != nil {
		logger.Sugar().Errorf("failed to set query handler: %v", err)
		return types.WorkflowOutput{}, err
	}

	signalCount := 0
	err = workflow.SetQueryHandler(ctx, signalCountQuery, func() (int, error) {
		logger.Sugar().Infof("query handler called. returning signal count: %d", signalCount)
		return signalCount, nil
	})
	if err != nil {
		logger.Sugar().Errorf("failed to set query handler: %v", err)
		return types.WorkflowOutput{}, err
	}

	endTime := workflow.Now(ctx).Add(input.Duration)
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	done := false
	for {
		selector := workflow.NewSelector(ctx)

		// timer
		timerCtx, timerCancel := workflow.WithCancel(ctx)
		waitTimer := workflow.NewTimer(timerCtx, endTime.Sub(workflow.Now(ctx)))
		selector.AddFuture(waitTimer, func(f workflow.Future) {
			done = true
		})

		// signal
		selector.AddReceive(signalCh, func(c workflow.Channel, more bool) {
			var signal string
			for c.ReceiveAsync(&signal) {
				signalCount++
				logger.Sugar().Infof("signal received: %s", signal)
			}
		})

		selector.Select(ctx)
		timerCancel()
		if done {
			break
		}
	}

	logger.Info("query workflow completed")
	return types.WorkflowOutput{}, nil
}
