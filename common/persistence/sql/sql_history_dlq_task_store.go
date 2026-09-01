package sql

import (
	"context"
	"errors"

	p "github.com/uber/cadence/common/persistence"
)

var errHistoryDLQNotImplemented = errors.New("history task DLQ not implemented for SQL")

type sqlHistoryDLQTaskStore struct{}

func (s *sqlHistoryDLQTaskStore) GetName() string { return "sql" }
func (s *sqlHistoryDLQTaskStore) Close()          {}

func (s *sqlHistoryDLQTaskStore) CreateHistoryDLQTask(_ context.Context, _ p.InternalCreateHistoryDLQTaskRequest) error {
	return errHistoryDLQNotImplemented
}

func (s *sqlHistoryDLQTaskStore) GetHistoryDLQTasks(_ context.Context, _ p.HistoryDLQGetTasksRequest) (p.InternalGetHistoryDLQTasksResponse, error) {
	return p.InternalGetHistoryDLQTasksResponse{}, errHistoryDLQNotImplemented
}

func (s *sqlHistoryDLQTaskStore) RangeDeleteHistoryDLQTasks(_ context.Context, _ p.HistoryDLQDeleteTasksRequest) error {
	return errHistoryDLQNotImplemented
}

func (s *sqlHistoryDLQTaskStore) GetHistoryDLQAckLevels(_ context.Context, _ p.HistoryDLQGetAckLevelsRequest) (p.InternalGetHistoryDLQAckLevelsResponse, error) {
	return p.InternalGetHistoryDLQAckLevelsResponse{}, errHistoryDLQNotImplemented
}

func (s *sqlHistoryDLQTaskStore) UpdateHistoryDLQAckLevel(_ context.Context, _ p.InternalUpdateHistoryDLQAckLevelRequest) error {
	return errHistoryDLQNotImplemented
}

func (s *sqlHistoryDLQTaskStore) CreateHistoryDLQAckLevelIfNotExists(_ context.Context, _ p.InternalHistoryDLQAckLevel) error {
	return errHistoryDLQNotImplemented
}
