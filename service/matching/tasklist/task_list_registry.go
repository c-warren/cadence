package tasklist

import (
	"sync"

	"github.com/uber/cadence/common/metrics"
)

type taskListRegistryImpl struct {
	sync.RWMutex
	taskLists map[Identifier]Manager

	// task lists indexed by domain ID and task list name
	// to quickly get all task lists for a domain or a task list name
	// they are always in sync with the taskLists map
	taskListsByDomainID     map[string]map[Identifier]Manager
	taskListsByTaskListName map[string]map[Identifier]Manager

	metricsClient metrics.Client
}

func NewTaskListRegistry(metricsClient metrics.Client) TaskListRegistry {
	return &taskListRegistryImpl{
		taskLists:               make(map[Identifier]Manager),
		taskListsByDomainID:     make(map[string]map[Identifier]Manager),
		taskListsByTaskListName: make(map[string]map[Identifier]Manager),
		metricsClient:           metricsClient,
	}
}

func (r *taskListRegistryImpl) Register(id Identifier, mgr Manager) {
	r.Lock()
	defer r.Unlock()

	// we can override the manager for the same identifier if it is already registered
	// this case should be handled by the caller
	r.taskLists[id] = mgr

	if _, ok := r.taskListsByDomainID[id.GetDomainID()]; !ok {
		r.taskListsByDomainID[id.GetDomainID()] = make(map[Identifier]Manager)
	}
	if _, ok := r.taskListsByTaskListName[id.GetName()]; !ok {
		r.taskListsByTaskListName[id.GetName()] = make(map[Identifier]Manager)
	}

	r.taskListsByDomainID[id.GetDomainID()][id] = mgr
	r.taskListsByTaskListName[id.GetName()][id] = mgr
	r.updateMetricsLocked()
}

func (r *taskListRegistryImpl) Unregister(mgr Manager) bool {
	id := mgr.TaskListID()
	r.Lock()
	defer r.Unlock()

	// we need to make sure we still hold the given `mgr` or we already replaced with a new one.
	currentTlMgr, ok := r.taskLists[*id]
	if ok && currentTlMgr == mgr {
		delete(r.taskLists, *id)

		delete(r.taskListsByDomainID[id.GetDomainID()], *id)
		if len(r.taskListsByDomainID[id.GetDomainID()]) == 0 {
			delete(r.taskListsByDomainID, id.GetDomainID())
		}

		delete(r.taskListsByTaskListName[id.GetName()], *id)
		if len(r.taskListsByTaskListName[id.GetName()]) == 0 {
			delete(r.taskListsByTaskListName, id.GetName())
		}

		r.updateMetricsLocked()
		return true
	}

	return false
}

func (r *taskListRegistryImpl) ManagerByTaskListIdentifier(id Identifier) (Manager, bool) {
	r.RLock()
	defer r.RUnlock()

	tlMgr, ok := r.taskLists[id]
	return tlMgr, ok
}

func (r *taskListRegistryImpl) AllManagers() []Manager {
	r.RLock()
	defer r.RUnlock()

	res := make([]Manager, 0, len(r.taskLists))
	for _, tlMgr := range r.taskLists {
		res = append(res, tlMgr)
	}
	return res
}

func (r *taskListRegistryImpl) ManagersByDomainID(domainID string) []Manager {
	r.RLock()
	defer r.RUnlock()

	var res []Manager
	for _, tlm := range r.taskListsByDomainID[domainID] {
		res = append(res, tlm)
	}
	return res
}

func (r *taskListRegistryImpl) ManagersByTaskListName(name string) []Manager {
	r.RLock()
	defer r.RUnlock()

	var res []Manager
	for _, tlm := range r.taskListsByTaskListName[name] {
		res = append(res, tlm)
	}
	return res
}

func (r *taskListRegistryImpl) updateMetricsLocked() {
	r.metricsClient.Scope(metrics.MatchingTaskListMgrScope).UpdateGauge(
		metrics.TaskListManagersGauge,
		float64(len(r.taskLists)),
	)
}
