package errors

import "fmt"

var _ error = &TaskListNotOwnedByHostError{}

type TaskListNotOwnedByHostError struct {
	OwnedByIdentity string
	MyIdentity      string
	TasklistName    string
}

func (m *TaskListNotOwnedByHostError) Error() string {
	return fmt.Sprintf("task list is not owned by this host: OwnedBy: %s, Me: %s, Tasklist: %s",
		m.OwnedByIdentity, m.MyIdentity, m.TasklistName)
}

func NewTaskListNotOwnedByHostError(ownedByIdentity string, myIdentity string, tasklistName string) *TaskListNotOwnedByHostError {
	return &TaskListNotOwnedByHostError{
		OwnedByIdentity: ownedByIdentity,
		MyIdentity:      myIdentity,
		TasklistName:    tasklistName,
	}
}
