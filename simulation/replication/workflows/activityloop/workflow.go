package activityloop

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/simulation/replication/types"
)

func Workflow(ctx workflow.Context, input types.WorkflowInput) (types.WorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Sugar().Infof("single-serial-activity-workflow started with input: %+v", input)

	count := 0

	for count < input.ActivityCount {
		logger.Sugar().Infof("single-serial-activity-workflow iteration %d", count)
		selector := workflow.NewSelector(ctx)
		activityFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskList:               types.TasklistName,
			ScheduleToStartTimeout: 10 * time.Second,
			StartToCloseTimeout:    10 * time.Second,
		}), FormatStringActivity, "World")
		selector.AddFuture(activityFuture, func(f workflow.Future) {
			logger.Info("single-serial-activity-workflow completed activity")
		})

		selector.Select(ctx)
		count++
	}

	logger.Info("single-serial-activity-workflow completed")
	return types.WorkflowOutput{Count: count}, nil
}

func FormatStringActivity(ctx context.Context, input string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("single-serial-activity-workflow format-string-activity started")

	time.Sleep(3 * time.Second)

	return fmt.Sprintf("Hello, %s!", input), nil
}
