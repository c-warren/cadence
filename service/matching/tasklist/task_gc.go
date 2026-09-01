package tasklist

import (
	"sync/atomic"
	"time"

	"github.com/uber/cadence/service/matching/config"
)

type taskGC struct {
	lock           int64
	db             *taskListDB
	ackLevel       int64
	lastDeleteTime time.Time
	config         *config.TaskListConfig
}

// newTaskGC returns an instance of a task garbage collector object
// taskGC internally maintains a delete cursor and attempts to delete
// a batch of tasks everytime Run() method is called.
//
// In order for the taskGC to actually delete tasks when Run() is called, one of
// two conditions must be met
//   - Size Threshold: More than MaxDeleteBatchSize tasks are waiting to be deleted (rough estimation)
//   - Time Threshold: Time since previous delete was attempted exceeds maxTimeBetweenTaskDeletes
//
// Finally, the Run() method is safe to be called from multiple threads. The underlying
// implementation will make sure only one caller executes Run() and others simply bail out
func newTaskGC(db *taskListDB, config *config.TaskListConfig) *taskGC {
	return &taskGC{db: db, config: config}
}

// Run deletes a batch of completed tasks, if its possible to do so
// Only attempts deletion if size or time thresholds are met
func (tgc *taskGC) Run(ackLevel int64) {
	tgc.tryDeleteNextBatch(ackLevel, false)
}

// RunNow deletes a batch of completed tasks if its possible to do so
// This method attempts deletions without waiting for size/time threshold to be met
func (tgc *taskGC) RunNow(ackLevel int64) {
	tgc.tryDeleteNextBatch(ackLevel, true)
}

func (tgc *taskGC) tryDeleteNextBatch(ackLevel int64, ignoreTimeCond bool) {
	if !tgc.tryLock() {
		return
	}
	defer tgc.unlock()
	batchSize := tgc.config.MaxTaskDeleteBatchSize()
	if !tgc.checkPrecond(ackLevel, batchSize, ignoreTimeCond) {
		return
	}
	tgc.lastDeleteTime = time.Now()
	for {
		n, err := tgc.db.CompleteTasksLessThan(ackLevel, batchSize)
		if err != nil {
			break
		}
		if n < batchSize {
			tgc.ackLevel = ackLevel
			break
		}
	}
}

func (tgc *taskGC) checkPrecond(ackLevel int64, batchSize int, ignoreTimeCond bool) bool {
	backlog := ackLevel - tgc.ackLevel
	if backlog >= int64(batchSize) {
		return true
	}
	return backlog > 0 && (ignoreTimeCond || time.Since(tgc.lastDeleteTime) > tgc.config.MaxTimeBetweenTaskDeletes)
}

func (tgc *taskGC) tryLock() bool {
	return atomic.CompareAndSwapInt64(&tgc.lock, 0, 1)
}

func (tgc *taskGC) unlock() {
	atomic.StoreInt64(&tgc.lock, 0)
}
