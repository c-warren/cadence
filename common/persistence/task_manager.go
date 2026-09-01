package persistence

import (
	"context"

	"github.com/uber/cadence/common/clock"
)

type (
	taskManager struct {
		persistence TaskStore
		timeSrc     clock.TimeSource
	}
)

var _ TaskManager = (*taskManager)(nil)

// NewTaskManager returns a new TaskManager
func NewTaskManager(
	persistence TaskStore,
) TaskManager {
	return &taskManager{
		persistence: persistence,
		timeSrc:     clock.NewRealTimeSource(),
	}
}

func (t *taskManager) GetName() string {
	return t.persistence.GetName()
}

func (t *taskManager) Close() {
	t.persistence.Close()
}

func (t *taskManager) LeaseTaskList(ctx context.Context, request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	request.CurrentTimeStamp = t.timeSrc.Now()
	return t.persistence.LeaseTaskList(ctx, request)
}

func (t *taskManager) GetTaskList(ctx context.Context, request *GetTaskListRequest) (*GetTaskListResponse, error) {
	return t.persistence.GetTaskList(ctx, request)
}

func (t *taskManager) UpdateTaskList(ctx context.Context, request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	request.CurrentTimeStamp = t.timeSrc.Now()
	return t.persistence.UpdateTaskList(ctx, request)
}

func (t *taskManager) ListTaskList(ctx context.Context, request *ListTaskListRequest) (*ListTaskListResponse, error) {
	return t.persistence.ListTaskList(ctx, request)
}

func (t *taskManager) DeleteTaskList(ctx context.Context, request *DeleteTaskListRequest) error {
	return t.persistence.DeleteTaskList(ctx, request)
}

func (t *taskManager) GetTaskListSize(ctx context.Context, request *GetTaskListSizeRequest) (*GetTaskListSizeResponse, error) {
	return t.persistence.GetTaskListSize(ctx, request)
}

func (t *taskManager) CreateTasks(ctx context.Context, request *CreateTasksRequest) (*CreateTasksResponse, error) {
	request.CurrentTimeStamp = t.timeSrc.Now()
	return t.persistence.CreateTasks(ctx, request)
}

func (t *taskManager) GetTasks(ctx context.Context, request *GetTasksRequest) (*GetTasksResponse, error) {
	return t.persistence.GetTasks(ctx, request)
}

func (t *taskManager) CompleteTask(ctx context.Context, request *CompleteTaskRequest) error {
	return t.persistence.CompleteTask(ctx, request)
}

func (t *taskManager) CompleteTasksLessThan(ctx context.Context, request *CompleteTasksLessThanRequest) (*CompleteTasksLessThanResponse, error) {
	return t.persistence.CompleteTasksLessThan(ctx, request)
}

func (t *taskManager) GetOrphanTasks(ctx context.Context, request *GetOrphanTasksRequest) (*GetOrphanTasksResponse, error) {
	return t.persistence.GetOrphanTasks(ctx, request)
}
