package testdata

import (
	"log"
	"time"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

const (
	DomainID      = "test-domain-id"
	WorkflowType  = "test-workflow-type"
	WorkflowID    = "test-workflow-id"
	RunID         = "test-run-id"
	TypeName      = "test-type-name"
	HistoryLenght = int64(1)
	TaskList      = "test-task-list"
	NumClusters   = int16(1)
	ShardID       = int16(1)
)

func NewVisibilityRow() persistence.InternalVisibilityWorkflowExecutionInfo {
	ts, err := time.Parse(time.RFC3339, "2024-04-01T22:08:41Z")
	if err != nil {
		log.Fatalf("Failed to parse time: %v", err)
	}
	return persistence.InternalVisibilityWorkflowExecutionInfo{
		DomainID:      DomainID,
		WorkflowType:  WorkflowType,
		WorkflowID:    WorkflowID,
		RunID:         RunID,
		TypeName:      TypeName,
		StartTime:     ts,
		ExecutionTime: ts,
		CloseTime:     ts,
		Status:        types.WorkflowExecutionCloseStatusCompleted.Ptr(),
		HistoryLength: HistoryLenght,
		Memo: &persistence.DataBlob{
			Encoding: constants.EncodingTypeJSON,
			Data:     []byte{},
		},
		TaskList:               TaskList,
		IsCron:                 false,
		NumClusters:            NumClusters,
		UpdateTime:             ts,
		SearchAttributes:       map[string]interface{}{},
		ShardID:                ShardID,
		ExecutionStatus:        types.WorkflowExecutionStatusPending,
		CronSchedule:           "",
		ScheduledExecutionTime: time.Time{},
	}
}

func NewVisibilityRowForInsert() *nosqlplugin.VisibilityRowForInsert {
	return &nosqlplugin.VisibilityRowForInsert{
		VisibilityRow: NewVisibilityRow(),
		DomainID:      DomainID,
	}
}

func NewVisibilityRowForUpdate(updateCloseToOpen, updateOpenToClose bool) *nosqlplugin.VisibilityRowForUpdate {
	visibilityRow := NewVisibilityRow()
	visibilityRow.CloseTime = visibilityRow.StartTime.Add(-1 * time.Minute)
	return &nosqlplugin.VisibilityRowForUpdate{
		VisibilityRow:     visibilityRow,
		DomainID:          DomainID,
		UpdateCloseToOpen: updateCloseToOpen,
		UpdateOpenToClose: updateOpenToClose,
	}
}

func NewSelectVisibilityRequestFilter(filterType nosqlplugin.VisibilityFilterType, sortType nosqlplugin.VisibilitySortType) *nosqlplugin.VisibilityFilter {
	ts, err := time.Parse(time.RFC3339, "2024-04-01T22:08:41Z")
	if err != nil {
		log.Fatalf("Failed to parse time: %v", err)
	}
	return &nosqlplugin.VisibilityFilter{
		ListRequest:  persistence.InternalListWorkflowExecutionsRequest{DomainUUID: DomainID, EarliestTime: ts, LatestTime: ts},
		FilterType:   filterType,
		SortType:     sortType,
		WorkflowType: WorkflowType,
		WorkflowID:   WorkflowID,
		CloseStatus:  0,
	}
}
