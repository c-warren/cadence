//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination registry_mock.go -self_package github.com/uber/cadence/service/history/query

package query

import (
	"sync"

	"github.com/uber/cadence/common/types"
)

var (
	errQueryNotExists = &types.InternalServiceError{Message: "query does not exist"}
)

type (
	// Registry manages all the queries for a workflow
	Registry interface {
		HasBufferedQuery() bool
		GetBufferedIDs() []string
		HasCompletedQuery() bool
		GetCompletedIDs() []string
		HasUnblockedQuery() bool
		GetUnblockedIDs() []string
		HasFailedQuery() bool
		GetFailedIDs() []string

		GetQueryTermCh(string) (<-chan struct{}, error)
		GetQueryInput(string) (*types.WorkflowQuery, error)
		GetTerminationState(string) (*TerminationState, error)

		BufferQuery(queryInput *types.WorkflowQuery) (string, <-chan struct{})
		SetTerminationState(string, *TerminationState) error
		RemoveQuery(id string)
	}

	registryImpl struct {
		sync.RWMutex

		buffered  map[string]query
		completed map[string]query
		unblocked map[string]query
		failed    map[string]query
	}
)

// NewRegistry creates a new query registry
func NewRegistry() Registry {
	return &registryImpl{
		buffered:  make(map[string]query),
		completed: make(map[string]query),
		unblocked: make(map[string]query),
		failed:    make(map[string]query),
	}
}

func (r *registryImpl) HasBufferedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.buffered) > 0
}

func (r *registryImpl) GetBufferedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.buffered)
}

func (r *registryImpl) HasCompletedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.completed) > 0
}

func (r *registryImpl) GetCompletedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.completed)
}

func (r *registryImpl) HasUnblockedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.unblocked) > 0
}

func (r *registryImpl) GetUnblockedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.unblocked)
}

func (r *registryImpl) HasFailedQuery() bool {
	r.RLock()
	defer r.RUnlock()
	return len(r.failed) > 0
}

func (r *registryImpl) GetFailedIDs() []string {
	r.RLock()
	defer r.RUnlock()
	return r.getIDs(r.failed)
}

func (r *registryImpl) GetQueryTermCh(id string) (<-chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryTermCh(), nil
}

func (r *registryImpl) GetQueryInput(id string) (*types.WorkflowQuery, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getQueryInput(), nil
}

func (r *registryImpl) GetTerminationState(id string) (*TerminationState, error) {
	r.RLock()
	defer r.RUnlock()
	q, err := r.getQueryNoLock(id)
	if err != nil {
		return nil, err
	}
	return q.getTerminationState()
}

func (r *registryImpl) BufferQuery(queryInput *types.WorkflowQuery) (string, <-chan struct{}) {
	r.Lock()
	defer r.Unlock()
	q := newQuery(queryInput)
	id := q.getQueryID()
	r.buffered[id] = q
	return id, q.getQueryTermCh()
}

func (r *registryImpl) SetTerminationState(id string, TerminationState *TerminationState) error {
	r.Lock()
	defer r.Unlock()
	q, ok := r.buffered[id]
	if !ok {
		return errQueryNotExists
	}
	if err := q.setTerminationState(TerminationState); err != nil {
		return err
	}
	delete(r.buffered, id)
	switch TerminationState.TerminationType {
	case TerminationTypeCompleted:
		r.completed[id] = q
	case TerminationTypeUnblocked:
		r.unblocked[id] = q
	case TerminationTypeFailed:
		r.failed[id] = q
	}
	return nil
}

func (r *registryImpl) RemoveQuery(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.buffered, id)
	delete(r.completed, id)
	delete(r.unblocked, id)
	delete(r.failed, id)
}

func (r *registryImpl) getQueryNoLock(id string) (query, error) {
	if q, ok := r.buffered[id]; ok {
		return q, nil
	}
	if q, ok := r.completed[id]; ok {
		return q, nil
	}
	if q, ok := r.unblocked[id]; ok {
		return q, nil
	}
	if q, ok := r.failed[id]; ok {
		return q, nil
	}
	return nil, errQueryNotExists
}

func (r *registryImpl) getIDs(m map[string]query) []string {
	result := make([]string, len(m))
	index := 0
	for id := range m {
		result[index] = id
		index++
	}
	return result
}
