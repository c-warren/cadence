package tasklist

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/pborman/uuid"

	p "github.com/uber/cadence/common/persistence"
)

type (
	mockTaskTable struct {
		domainID   string
		workflowID string
		runID      string
		nextTaskID int64
		tasks      []*p.TaskInfo
	}
	mockTaskListTable struct {
		sync.Mutex
		info []p.TaskListInfo
	}
)

func newMockTaskTable() *mockTaskTable {
	return &mockTaskTable{
		domainID:   uuid.New(),
		workflowID: uuid.New(),
		runID:      uuid.New(),
	}
}

func (tbl *mockTaskListTable) generate(name string, idle bool) {
	tl := p.TaskListInfo{
		DomainID:    uuid.New(),
		Name:        name,
		RangeID:     22,
		LastUpdated: time.Now(),
	}
	if idle {
		tl.LastUpdated = time.Unix(1000, 1000)
	}
	tbl.info = append(tbl.info, tl)
}

func (tbl *mockTaskListTable) list(token []byte, count int) ([]p.TaskListInfo, []byte) {
	tbl.Lock()
	defer tbl.Unlock()
	if tbl.info == nil {
		return nil, nil
	}
	var off int
	if token != nil {
		off = int(binary.BigEndian.Uint32(token))
	}
	rem := len(tbl.info) - count + 1
	if rem < count {
		return tbl.info[off:], nil
	}
	token = make([]byte, 4)
	binary.BigEndian.PutUint32(token, uint32(off+count))
	return tbl.info[off : off+count], token
}

func (tbl *mockTaskListTable) delete(name string) {
	tbl.Lock()
	defer tbl.Unlock()
	var newInfo []p.TaskListInfo
	for _, tl := range tbl.info {
		if tl.Name != name {
			newInfo = append(newInfo, tl)
		}
	}
	tbl.info = newInfo
}

func (tbl *mockTaskListTable) get(name string) *p.TaskListInfo {
	tbl.Lock()
	defer tbl.Unlock()
	for _, tl := range tbl.info {
		if tl.Name == name {
			return &tl
		}
	}
	return nil
}

func (tbl *mockTaskTable) generate(count int, expired bool) {
	for i := 0; i < count; i++ {
		ti := &p.TaskInfo{
			DomainID:                      tbl.domainID,
			WorkflowID:                    tbl.workflowID,
			RunID:                         tbl.runID,
			TaskID:                        tbl.nextTaskID,
			ScheduleID:                    3,
			ScheduleToStartTimeoutSeconds: 30,
			Expiry:                        time.Now().Add(time.Hour),
		}
		if expired {
			ti.ScheduleToStartTimeoutSeconds = -33
			ti.Expiry = time.Unix(0, time.Now().UnixNano()-int64(time.Second*33))
		}
		tbl.tasks = append(tbl.tasks, ti)
		tbl.nextTaskID++
	}
}

func (tbl *mockTaskTable) get(count int) []*p.TaskInfo {
	if len(tbl.tasks) >= count {
		return tbl.tasks[:count]
	}
	return tbl.tasks[:]
}

func (tbl *mockTaskTable) deleteLessThan(id int64, limit int) int {
	count := 0
	for _, t := range tbl.tasks {
		if t.TaskID <= id && count < limit {
			count++
			continue
		}
		break
	}
	switch {
	case count == 0:
	case count == len(tbl.tasks):
		tbl.tasks = nil
	default:
		tbl.tasks = tbl.tasks[count:]
	}
	return count
}
