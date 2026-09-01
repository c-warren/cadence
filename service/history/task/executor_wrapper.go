package task

import (
	"context"
	"encoding/json"
	"runtime/debug"

	"github.com/uber/cadence/common/activecluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

type (
	executorWrapper struct {
		currentClusterName string
		activeClusterMgr   activecluster.Manager
		activeExecutor     Executor
		standbyExecutor    Executor
		logger             log.Logger
	}
)

func NewExecutorWrapper(
	currentClusterName string,
	activeClusterMgr activecluster.Manager,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
) Executor {
	return &executorWrapper{
		currentClusterName: currentClusterName,
		activeClusterMgr:   activeClusterMgr,
		activeExecutor:     activeExecutor,
		standbyExecutor:    standbyExecutor,
		logger:             logger,
	}
}

func (e *executorWrapper) Stop() {
	e.activeExecutor.Stop()
	e.standbyExecutor.Stop()
}

func (e *executorWrapper) Execute(task Task) (ExecuteResponse, error) {
	if e.isActiveTask(task) {
		return e.activeExecutor.Execute(task)
	}

	return e.standbyExecutor.Execute(task)
}

func (e *executorWrapper) isActiveTask(
	task Task,
) bool {
	domainID := task.GetDomainID()
	wfID := task.GetWorkflowID()
	rID := task.GetRunID()

	activeClusterInfo, err := e.activeClusterMgr.GetActiveClusterInfoByWorkflow(context.Background(), domainID, wfID, rID)
	if err != nil {
		e.logger.Warn("Failed to get active cluster info, process task as active.", tag.WorkflowDomainID(domainID), tag.WorkflowID(wfID), tag.WorkflowRunID(rID), tag.Error(err))
		return true
	}

	if activeClusterInfo.ActiveClusterName != e.currentClusterName {
		if e.logger.DebugOn() {
			taskJSON, _ := json.Marshal(task)
			e.logger.Debug("Process task as standby.",
				tag.WorkflowDomainID(domainID),
				tag.Dynamic("task", string(taskJSON)),
				tag.Dynamic("taskType", task.GetTaskType()),
				tag.ClusterName(activeClusterInfo.ActiveClusterName),
				tag.Dynamic("stack", string(debug.Stack())),
			)
		}
		return false
	}
	if e.logger.DebugOn() {
		taskJSON, _ := json.Marshal(task)
		e.logger.Debug("Process task as active.",
			tag.WorkflowDomainID(domainID),
			tag.Dynamic("task", string(taskJSON)),
			tag.Dynamic("taskType", task.GetTaskType()),
			tag.ClusterName(activeClusterInfo.ActiveClusterName),
			tag.Dynamic("stack", string(debug.Stack())),
		)
	}
	return true
}
