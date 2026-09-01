package liveness

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
)

type (
	Liveness struct {
		status     int32
		timeSource clock.TimeSource
		ttl        time.Duration

		// stopCh is used to signal the liveness to stop
		stopCh chan struct{}
		// wg is used to wait for the liveness to stop
		wg sync.WaitGroup

		// broadcast shutdown functions
		broadcastShutdownFn func()

		lastEventTimeNano int64
	}
)

var _ common.Daemon = (*Liveness)(nil)

// NewLiveness creates a Liveness daemon that calls the broadcastShutdownFn if it does not receive MarkAlive() within ttl
// NOTE: livesness needs to be stopped explicitly to avoid go routine leak
func NewLiveness(timeSource clock.TimeSource, ttl time.Duration, broadcastShutdownFn func()) *Liveness {
	return &Liveness{
		status:              common.DaemonStatusInitialized,
		timeSource:          timeSource,
		ttl:                 ttl,
		stopCh:              make(chan struct{}),
		broadcastShutdownFn: broadcastShutdownFn,
		lastEventTimeNano:   timeSource.Now().UnixNano(),
	}
}

func (l *Liveness) Start() {
	if !atomic.CompareAndSwapInt32(&l.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	l.wg.Add(1)
	checkTimer := l.timeSource.NewTicker(l.ttl / 2)
	go l.eventLoop(checkTimer)
}

// Stop ONLY shuts down liveness does not block on broadcastShutdownFn
func (l *Liveness) Stop() {
	if !atomic.CompareAndSwapInt32(&l.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(l.stopCh)
	l.wg.Wait()
}

func (l *Liveness) eventLoop(ticker clock.Ticker) {
	defer l.wg.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if !l.IsAlive() {
				go l.broadcastShutdownFn() // do not block shutdown
				return
			}

		case <-l.stopCh:
			return
		}
	}
}

func (l *Liveness) IsAlive() bool {
	now := l.timeSource.Now().UnixNano()
	lastUpdate := atomic.LoadInt64(&l.lastEventTimeNano)
	return now-lastUpdate < l.ttl.Nanoseconds()
}

func (l *Liveness) MarkAlive() {
	now := l.timeSource.Now().UnixNano()
	atomic.StoreInt64(&l.lastEventTimeNano, now)
}
