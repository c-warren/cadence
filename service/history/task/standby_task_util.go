// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/activecluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
)

type (
	standbyActionFn     func(context.Context, execution.Context, execution.MutableState) (interface{}, error)
	standbyPostActionFn func(context.Context, persistence.Task, interface{}, log.Logger) error

	standbyCurrentTimeFn func(persistence.Task) (time.Time, error)
)

var (
	errDomainBecomesActive = errors.New("domain becomes active when processing task as standby")
)

func standbyTaskPostActionNoOp(
	ctx context.Context,
	taskInfo persistence.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	// return error so task processing logic will retry
	logger.Debug("standbyTaskPostActionNoOp return redispatch error so task processing logic will retry",
		tag.WorkflowID(taskInfo.GetWorkflowID()),
		tag.WorkflowRunID(taskInfo.GetRunID()),
		tag.WorkflowDomainID(taskInfo.GetDomainID()),
		tag.TaskID(taskInfo.GetTaskID()),
		tag.TaskType(taskInfo.GetTaskType()),
		tag.FailoverVersion(taskInfo.GetVersion()),
		tag.Timestamp(taskInfo.GetVisibilityTimestamp()))
	return &redispatchError{Reason: fmt.Sprintf("post action is %T", postActionInfo)}
}

func standbyTaskPostActionTaskDiscarded(
	ctx context.Context,
	task persistence.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	logger.Error("Discarding standby task due to task being pending for too long.",
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
		tag.WorkflowDomainID(task.GetDomainID()),
		tag.TaskID(task.GetTaskID()),
		tag.TaskType(task.GetTaskType()),
		tag.FailoverVersion(task.GetVersion()),
		tag.Timestamp(task.GetVisibilityTimestamp()))
	return ErrTaskDiscarded
}

// standbyTaskPostActionEnqueueToDLQ enqueues the task to DLQ instead of discarding
func standbyTaskPostActionEnqueueToDLQ(
	dlqManager persistence.StandbyTaskDLQManager,
	shardID int,
	clusterAttributeScope string,
	clusterAttributeName string,
) standbyPostActionFn {
	return func(
		ctx context.Context,
		task persistence.Task,
		postActionInfo interface{},
		logger log.Logger,
	) error {
		if postActionInfo == nil {
			return nil
		}

		logger.Warn("Enqueuing standby task to DLQ due to task being pending for too long",
			tag.WorkflowID(task.GetWorkflowID()),
			tag.WorkflowRunID(task.GetRunID()),
			tag.WorkflowDomainID(task.GetDomainID()),
			tag.TaskID(task.GetTaskID()),
			tag.TaskType(task.GetTaskType()),
			tag.FailoverVersion(task.GetVersion()),
			tag.Timestamp(task.GetVisibilityTimestamp()))

		// Serialize the task
		taskPayload, err := serializeTask(task)
		if err != nil {
			logger.Error("Failed to serialize task for DLQ", tag.Error(err))
			return ErrTaskDiscarded
		}

		err = dlqManager.EnqueueStandbyTask(ctx, &persistence.EnqueueStandbyTaskRequest{
			ShardID:              shardID,
			DomainID:             task.GetDomainID(),
			ClusterAttributeScope: clusterAttributeScope,
			ClusterAttributeName:  clusterAttributeName,
			WorkflowID:           task.GetWorkflowID(),
			RunID:                task.GetRunID(),
			TaskID:               task.GetTaskID(),
			VisibilityTimestamp:  task.GetVisibilityTimestamp().UnixNano(),
			TaskType:             task.GetTaskType(),
			TaskPayload:          taskPayload,
			Version:              task.GetVersion(),
		})

		if err != nil {
			// If duplicate, treat as success
			if _, ok := err.(*persistence.ConditionFailedError); ok {
				logger.Info("Task already exists in DLQ, treating as success",
					tag.TaskID(task.GetTaskID()))
				return ErrTaskDiscarded
			}
			logger.Error("Failed to enqueue task to DLQ", tag.Error(err))
			return err
		}

		// Return ErrTaskDiscarded to remove from execution queue
		return ErrTaskDiscarded
	}
}

// serializeTask converts a task to bytes for storage
func serializeTask(task persistence.Task) ([]byte, error) {
	// For POC, we'll store the task info as JSON
	// In production, this would use the same serialization as the persistence layer

	// Try to convert to TransferTaskInfo first
	if transferInfo, err := task.ToTransferTaskInfo(); err == nil {
		return json.Marshal(transferInfo)
	}

	// Try to convert to TimerTaskInfo
	if timerInfo, err := task.ToTimerTaskInfo(); err == nil {
		return json.Marshal(timerInfo)
	}

	return nil, fmt.Errorf("unsupported task type: %T", task)
}

type (
	historyResendInfo struct {
		// used by NDC
		lastEventID      *int64
		lastEventVersion *int64
	}

	pushActivityToMatchingInfo struct {
		activityScheduleToStartTimeout int32
		tasklist                       types.TaskList
		partitionConfig                map[string]string
	}

	pushDecisionToMatchingInfo struct {
		decisionScheduleToStartTimeout int32
		tasklist                       types.TaskList
		partitionConfig                map[string]string
	}
)

func newPushActivityToMatchingInfo(
	activityScheduleToStartTimeout int32,
	tasklist types.TaskList,
	partitionConfig map[string]string,
) *pushActivityToMatchingInfo {

	return &pushActivityToMatchingInfo{
		activityScheduleToStartTimeout: activityScheduleToStartTimeout,
		tasklist:                       tasklist,
		partitionConfig:                partitionConfig,
	}
}

func newPushDecisionToMatchingInfo(
	decisionScheduleToStartTimeout int32,
	tasklist types.TaskList,
	partitionConfig map[string]string,
) *pushDecisionToMatchingInfo {

	return &pushDecisionToMatchingInfo{
		decisionScheduleToStartTimeout: decisionScheduleToStartTimeout,
		tasklist:                       tasklist,
		partitionConfig:                partitionConfig,
	}
}

func getHistoryResendInfo(
	mutableState execution.MutableState,
) (*historyResendInfo, error) {

	versionHistories := mutableState.GetVersionHistories()
	if versionHistories == nil {
		return nil, execution.ErrMissingVersionHistories
	}
	currentBranch, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	lastItem, err := currentBranch.GetLastItem()
	if err != nil {
		return nil, err
	}
	return &historyResendInfo{
		lastEventID:      common.Int64Ptr(lastItem.EventID),
		lastEventVersion: common.Int64Ptr(lastItem.Version),
	}, nil
}

func getStandbyPostActionFn(
	logger log.Logger,
	taskInfo persistence.Task,
	standbyNow standbyCurrentTimeFn,
	standbyTaskMissingEventsResendDelay time.Duration,
	standbyTaskMissingEventsDiscardDelay time.Duration,
	fetchHistoryStandbyPostActionFn standbyPostActionFn,
	discardTaskStandbyPostActionFn standbyPostActionFn,
) standbyPostActionFn {

	taskTime := taskInfo.GetVisibilityTimestamp()
	resendTime := taskTime.Add(standbyTaskMissingEventsResendDelay)
	discardTime := taskTime.Add(standbyTaskMissingEventsDiscardDelay)

	tags := []tag.Tag{
		tag.WorkflowID(taskInfo.GetWorkflowID()),
		tag.WorkflowRunID(taskInfo.GetRunID()),
		tag.WorkflowDomainID(taskInfo.GetDomainID()),
		tag.TaskID(taskInfo.GetTaskID()),
		tag.TaskType(int(taskInfo.GetTaskType())),
		tag.Timestamp(taskInfo.GetVisibilityTimestamp()),
	}

	now, err := standbyNow(taskInfo)
	if err != nil {
		tags = append(tags, tag.Error(err))
		logger.Error("getStandbyPostActionFn error getting current time, fallback to standbyTaskPostActionNoOp", tags...)
		return standbyTaskPostActionNoOp
	}

	// now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(resendTime) {
		logger.Debug("getStandbyPostActionFn returning standbyTaskPostActionNoOp because now < task start time + StandbyTaskMissingEventsResendDelay", tags...)
		return standbyTaskPostActionNoOp
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(discardTime) {
		logger.Debug("getStandbyPostActionFn returning fetchHistoryStandbyPostActionFn because task start time + StandbyTaskMissingEventsResendDelay <= now < task start time + StandbyTaskMissingEventsResendDelay", tags...)
		return fetchHistoryStandbyPostActionFn
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now
	logger.Debug("getStandbyPostActionFn returning discardTaskStandbyPostActionFn because task start time + StandbyTaskMissingEventsResendDelay <= now", tags...)
	return discardTaskStandbyPostActionFn
}

func getRemoteClusterName(
	ctx context.Context,
	currentCluster string,
	activeClusterMgr activecluster.Manager,
	taskInfo persistence.Task,
) (string, error) {
	activeClusterInfo, err := activeClusterMgr.GetActiveClusterInfoByWorkflow(ctx, taskInfo.GetDomainID(), taskInfo.GetWorkflowID(), taskInfo.GetRunID())
	if err != nil {
		return "", err
	}
	if activeClusterInfo.ActiveClusterName == currentCluster {
		return "", errDomainBecomesActive
	}
	return activeClusterInfo.ActiveClusterName, nil
}
