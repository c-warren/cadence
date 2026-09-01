package query

import (
	"sync/atomic"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common/types"
)

const (
	// TerminationTypeCompleted means a query reaches its termination state because it has been completed
	TerminationTypeCompleted TerminationType = iota
	// TerminationTypeUnblocked means a query reaches its termination state because it has been unblocked
	TerminationTypeUnblocked
	// TerminationTypeFailed means a query reaches its termination state because it has failed
	TerminationTypeFailed
)

var (
	errTerminationStateInvalid = &types.InternalServiceError{Message: "query termination state invalid"}
	errAlreadyInTerminalState  = &types.InternalServiceError{Message: "query already in terminal state"}
	errQueryNotInTerminalState = &types.InternalServiceError{Message: "query not in terminal state"}
)

type (
	// TerminationType is the type of a query's termination state
	TerminationType int

	// TerminationState describes a query's termination state
	TerminationState struct {
		TerminationType TerminationType
		QueryResult     *types.WorkflowQueryResult
		Failure         error
	}

	query interface {
		getQueryID() string
		getQueryTermCh() <-chan struct{}
		getQueryInput() *types.WorkflowQuery
		getTerminationState() (*TerminationState, error)
		setTerminationState(*TerminationState) error
	}

	queryImpl struct {
		id         string
		queryInput *types.WorkflowQuery
		termCh     chan struct{}

		terminationState atomic.Value
	}
)

func newQuery(queryInput *types.WorkflowQuery) query {
	return &queryImpl{
		id:         uuid.New(),
		queryInput: queryInput,
		termCh:     make(chan struct{}),
	}
}

func (q *queryImpl) getQueryID() string {
	return q.id
}

func (q *queryImpl) getQueryTermCh() <-chan struct{} {
	return q.termCh
}

func (q *queryImpl) getQueryInput() *types.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) getTerminationState() (*TerminationState, error) {
	ts := q.terminationState.Load()
	if ts == nil {
		return nil, errQueryNotInTerminalState
	}
	return ts.(*TerminationState), nil
}

func (q *queryImpl) setTerminationState(terminationState *TerminationState) error {
	if err := q.validateTerminationState(terminationState); err != nil {
		return err
	}
	currTerminationState, _ := q.getTerminationState()
	if currTerminationState != nil {
		return errAlreadyInTerminalState
	}
	q.terminationState.Store(terminationState)
	close(q.termCh)
	return nil
}

func (q *queryImpl) validateTerminationState(
	terminationState *TerminationState,
) error {
	if terminationState == nil {
		return errTerminationStateInvalid
	}
	switch terminationState.TerminationType {
	case TerminationTypeCompleted:
		if terminationState.QueryResult == nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		queryResult := terminationState.QueryResult
		validAnswered := queryResult.GetResultType() == types.QueryResultTypeAnswered &&
			queryResult.Answer != nil &&
			queryResult.ErrorMessage == ""
		validFailed := queryResult.GetResultType() == types.QueryResultTypeFailed &&
			queryResult.Answer == nil &&
			queryResult.ErrorMessage != ""
		if !validAnswered && !validFailed {
			return errTerminationStateInvalid
		}
		return nil
	case TerminationTypeUnblocked:
		if terminationState.QueryResult != nil || terminationState.Failure != nil {
			return errTerminationStateInvalid
		}
		return nil
	case TerminationTypeFailed:
		if terminationState.QueryResult != nil || terminationState.Failure == nil {
			return errTerminationStateInvalid
		}
		return nil
	default:
		return errTerminationStateInvalid
	}
}
