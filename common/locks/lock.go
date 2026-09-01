package locks

import (
	"context"
	"sync"
)

type (
	// Mutex accepts a context in its Lock method.
	// It blocks the goroutine until either the lock is acquired or the context
	// is closed.
	Mutex interface {
		Lock(context.Context) error
		Unlock()
	}
)

type impl struct {
	ch chan struct{}
}

func NewMutex() Mutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &impl{
		ch: ch,
	}
}

func (m *impl) Lock(ctx context.Context) error {
	select {
	case <-m.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *impl) Unlock() {
	select {
	case m.ch <- struct{}{}:
	default:
		// reaching this branch means the mutex was not locked.
		// this will intentionally crash the process, and print stack traces.
		//
		// it's not really possible to do this by hand (`fatal` is private),
		// and other common options like `log.Fatal` don't print stacks / don't
		// print all stacks / have loads of minor inconsistencies.
		//
		// regardless, what we want is to mimic mutexes when wrongly unlocked,
		// so just use the mutex implementation to guarantee it's the same.
		var m sync.Mutex
		m.Unlock()
	}
}
