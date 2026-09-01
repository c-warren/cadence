//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination taskTokenSerializerInterfaces_mock.go -self_package github.com/uber/cadence/common

package common

type (
	// TaskTokenSerializer serializes task tokens
	TaskTokenSerializer interface {
		Serialize(token *TaskToken) ([]byte, error)
		Deserialize(data []byte) (*TaskToken, error)
		SerializeQueryTaskToken(token *QueryTaskToken) ([]byte, error)
		DeserializeQueryTaskToken(data []byte) (*QueryTaskToken, error)
	}

	// TaskToken identifies a task
	TaskToken struct {
		DomainID        string `json:"domainId"`
		WorkflowID      string `json:"workflowId"`
		WorkflowType    string `json:"workflowType"`
		RunID           string `json:"runId"`
		ScheduleID      int64  `json:"scheduleId"`
		ScheduleAttempt int64  `json:"scheduleAttempt"`
		ActivityID      string `json:"activityId"`
		ActivityType    string `json:"activityType"`
	}

	// QueryTaskToken identifies a query task
	QueryTaskToken struct {
		DomainID   string `json:"domainId"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		TaskList   string `json:"taskList"`
		TaskID     string `json:"taskId"`
	}
)

func (t TaskToken) GetDomainID() string {
	return t.DomainID
}

func (t QueryTaskToken) GetDomainID() string {
	return t.DomainID
}
