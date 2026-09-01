package task

import (
	"sync/atomic"
	"time"
)

// TTLChannel is a channel that can expire if it is not written to for a given amount of time.
type TTLChannel[V any] struct {
	c             chan V
	lastWriteTime atomic.Int64
	refCount      atomic.Int32
}

func NewTTLChannel[V any](bufferSize int) *TTLChannel[V] {
	return &TTLChannel[V]{
		c: make(chan V, bufferSize),
	}
}

func (c *TTLChannel[V]) IncRef() {
	c.refCount.Add(1)
}

func (c *TTLChannel[V]) DecRef() {
	c.refCount.Add(-1)
}

func (c *TTLChannel[V]) RefCount() int32 {
	return c.refCount.Load()
}

func (c *TTLChannel[V]) LastWriteTime() time.Time {
	return time.Unix(c.lastWriteTime.Load(), 0)
}

func (c *TTLChannel[V]) UpdateLastWriteTime(now time.Time) {
	c.lastWriteTime.Store(now.Unix())
}

func (c *TTLChannel[V]) Chan() chan V {
	return c.c
}

func (c *TTLChannel[V]) Len() int {
	return len(c.c)
}

func (c *TTLChannel[V]) Cap() int {
	return cap(c.c)
}

func (c *TTLChannel[V]) ShouldCleanup(now time.Time, ttl time.Duration) bool {
	return now.Sub(c.LastWriteTime()) > ttl && c.Len() == 0 && c.RefCount() == 0
}
