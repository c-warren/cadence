package basic

import (
	"fmt"
	"time"

	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/uber/cadence/bench/load/common"
)

const (
	stressWorkflowName = "basicStressWorkflow"
)

type (
	// WorkflowParams inputs to workflow.
	WorkflowParams struct {
		ChainSequence    int
		ConcurrentCount  int
		TaskListNumber   int
		PayloadSizeBytes int
		CadenceSleep     time.Duration
		PanicWorkflow    bool
	}
)

// RegisterWorker registers workflows and activities for basic load
func RegisterWorker(w worker.Worker) {
	w.RegisterWorkflowWithOptions(stressWorkflowExecute, workflow.RegisterOptions{Name: stressWorkflowName})
}

func stressWorkflowExecute(ctx workflow.Context, workflowInput WorkflowParams) error {

	if workflowInput.PanicWorkflow {
		panic("panic workflow load test.")
	}

	activityParams := common.EchoActivityParams{
		Payload: make([]byte, workflowInput.PayloadSizeBytes),
	}

	ao := workflow.ActivityOptions{
		TaskList:               common.GetTaskListName(workflowInput.TaskListNumber),
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       20 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	for i := 0; i < workflowInput.ChainSequence; i++ {
		selector := workflow.NewSelector(ctx)
		var activityErr error
		for j := 0; j < workflowInput.ConcurrentCount; j++ {
			selector.AddFuture(workflow.ExecuteActivity(ctx, common.EchoActivityName, activityParams), func(f workflow.Future) {
				err := f.Get(ctx, nil)
				if err != nil {
					workflow.GetLogger(ctx).Error("basic test stress workflow echo activity execution failed", zap.Error(err))
					activityErr = err
				}
			})
		}

		for i := 0; i < workflowInput.ConcurrentCount; i++ {
			selector.Select(ctx) // this will wait for one branch
			if activityErr != nil {
				return fmt.Errorf("echo activity execution failed: %v", activityErr)
			}
		}

		if workflowInput.CadenceSleep > 0 {
			if err := workflow.Sleep(ctx, workflowInput.CadenceSleep); err != nil {
				workflow.GetLogger(ctx).Error("cadence.Sleep() returned error", zap.Error(err))
				return fmt.Errorf("stress workflow sleep failed: %v", err)
			}
		}
	}
	return nil
}
