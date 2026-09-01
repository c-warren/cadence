package worker

import (
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
)

//go:generate mockgen -source $GOFILE -destination worker_mock.go -package worker github.com/uber/cadence/service/worker/worker Worker

type Worker interface {
	RegisterActivity(activity interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
	GetRegisteredActivities() []activity.RegistryInfo
	RegisterWorkflow(workflow interface{})
	RegisterWorkflowWithOptions(workflow interface{}, options workflow.RegisterOptions)
	GetRegisteredWorkflows() []workflow.RegistryInfo
	Start() error
	Stop()
	Run() error
}
