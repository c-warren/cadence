package clock

import (
	"sync"
	"time"
)

type (
	// EventTimerGate is a TimerGate in which the time is retrieved from external events via SetCurrentTime method.
	EventTimerGate interface {
		TimerGate
		// SetCurrentTime set the current time, and additionally fire the fire chan
		// if new "current" time is after the fire time, return true if
		// "current" is actually updated
		SetCurrentTime(t time.Time) bool
	}

	eventTimerGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireChan chan time.Time

		// lock for timer and fire time
		sync.RWMutex
		// view of current time
		currentTime time.Time
		// time which timer will fire
		fireTime time.Time
	}
)

// NewEventTimerGate create a new event timer gate instance
func NewEventTimerGate(currentTime time.Time) EventTimerGate {
	timer := &eventTimerGateImpl{
		currentTime: currentTime,
		fireTime:    time.Time{},
		fireChan:    make(chan time.Time, 1),
	}
	return timer
}

func (t *eventTimerGateImpl) Chan() <-chan time.Time {
	return t.fireChan
}

func (t *eventTimerGateImpl) FireAfter(now time.Time) bool {
	t.RLock()
	defer t.RUnlock()

	active := t.currentTime.Before(t.fireTime)
	return active && t.fireTime.After(now)
}

func (t *eventTimerGateImpl) Update(nextTime time.Time) bool {
	t.Lock()
	defer t.Unlock()

	active := t.currentTime.Before(t.fireTime)
	if active {
		if t.fireTime.Before(nextTime) {
			// current time < next wake up time < next time
			return false
		}

		if t.currentTime.Before(nextTime) {
			// current time < next time <= next wake up time
			t.fireTime = nextTime
			return true
		}

		// next time <= current time < next wake up time
		t.fireTime = nextTime
		t.fire(t.currentTime)
		return true
	}

	// this means the timer, before stopped, has already fired / never active
	if !t.currentTime.Before(nextTime) {
		// next time is <= current time, need to fire immediately
		// whether to update next wake up time or not is irrelevent
		t.fire(t.currentTime)
	} else {
		// next time > current time
		t.fireTime = nextTime
	}
	return true
}

func (t *eventTimerGateImpl) Stop() {
	t.Lock()
	defer t.Unlock()
	t.fireTime = time.Time{}
}

func (t *eventTimerGateImpl) SetCurrentTime(currentTime time.Time) bool {
	t.Lock()
	defer t.Unlock()

	if !t.currentTime.Before(currentTime) {
		// new current time is <= current time
		return false
	}

	// NOTE: do not update the current time now
	if !t.currentTime.Before(t.fireTime) {
		// current time already >= next wakeup time
		// avoid duplicate fire
		t.currentTime = currentTime
		return true
	}

	t.currentTime = currentTime
	if !t.currentTime.Before(t.fireTime) {
		t.fire(t.currentTime)
	}
	return true
}

func (t *eventTimerGateImpl) fire(fireTime time.Time) {
	select {
	case t.fireChan <- fireTime:
		// timer successfully triggered
	default:
		// timer already triggered, pass
	}
}
