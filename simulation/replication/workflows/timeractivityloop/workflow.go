package timeractivityloop

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/simulation/replication/types"
)

const (
	latestSignalContentQuery = "latest-signal-content"
	signalName               = "custom-signal"
)

func Workflow(ctx workflow.Context, input types.WorkflowInput) (types.WorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Sugar().Infof("timer-activity-loop-workflow started with input: %+v", input)

	signalContent := make([]string, 0)
	err := workflow.SetQueryHandler(ctx, latestSignalContentQuery, func() ([]string, error) {
		logger.Sugar().Infof("query handler called. returning all signal content: %s", signalContent)
		return signalContent, nil
	})
	if err != nil {
		logger.Sugar().Errorf("failed to set query handler: %v", err)
		return types.WorkflowOutput{}, err
	}

	signalCh := workflow.GetSignalChannel(ctx, signalName)
	endTime := workflow.Now(ctx).Add(input.Duration)
	count := 0
	for {
		logger.Sugar().Infof("timer-activity-loop-workflow iteration %d", count)
		selector := workflow.NewSelector(ctx)
		activityFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskList:               types.TasklistName,
			ScheduleToStartTimeout: 10 * time.Second,
			StartToCloseTimeout:    10 * time.Second,
		}), FormatStringActivity, "World")
		selector.AddFuture(activityFuture, func(f workflow.Future) {
			logger.Info("timer-activity-loop-workflow completed activity")
		})

		// use timer future to send notification email if processing takes too long
		timerFuture := workflow.NewTimer(ctx, types.TimerInterval)
		selector.AddFuture(timerFuture, func(f workflow.Future) {
			logger.Info("timer-activity-loop-workflow timer fired")
		})

		selector.AddReceive(signalCh, func(c workflow.Channel, more bool) {
			var signal string
			for c.ReceiveAsync(&signal) {
				logger.Sugar().Infof("signal received: %s", signal)
				signalContent = append(signalContent, signal)
			}
		})

		// wait for both activity and timer to complete
		selector.Select(ctx)
		selector.Select(ctx)
		count++

		now := workflow.Now(ctx)
		if now.Before(endTime) {
			logger.Sugar().Infof("timer-activity-loop-workflow will continue iteration because [now %v] < [endTime %v]", now, endTime)
		} else {
			logger.Sugar().Infof("timer-activity-loop-workflow will exit because [now %v] >= [endTime %v]", now, endTime)
			break
		}
	}

	logger.Info("timer-activity-loop-workflow completed")
	return types.WorkflowOutput{Count: count}, nil
}

func FormatStringActivity(ctx context.Context, input string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("timer-activity-loop-workflow format-string-activity started")
	return fmt.Sprintf("Hello, %s!", input), nil
}
