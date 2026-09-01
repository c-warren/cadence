package collection

import (
	"errors"
	"sync"
)

type (
	concurrentQueueImpl[T any] struct {
		sync.RWMutex
		items []T
	}
)

// NewConcurrentQueue creates a new concurrent queue
func NewConcurrentQueue[T any]() Queue[T] {
	return &concurrentQueueImpl[T]{
		items: make([]T, 0, 1000),
	}
}

func (q *concurrentQueueImpl[T]) Peek() (T, error) {
	q.RLock()
	defer q.RUnlock()

	var item T
	if q.isEmptyLocked() {
		return item, errors.New("queue is empty")
	}
	return q.items[0], nil
}

func (q *concurrentQueueImpl[T]) Add(item T) {
	q.Lock()
	defer q.Unlock()

	q.items = append(q.items, item)
}

func (q *concurrentQueueImpl[T]) Remove() (T, error) {
	q.Lock()
	defer q.Unlock()
	var item T
	if q.isEmptyLocked() {
		return item, errors.New("queue is empty")
	}

	item = q.items[0]
	q.items = q.items[1:]

	return item, nil
}

func (q *concurrentQueueImpl[T]) IsEmpty() bool {
	q.RLock()
	defer q.RUnlock()

	return q.isEmptyLocked()
}

func (q *concurrentQueueImpl[T]) Len() int {
	q.RLock()
	defer q.RUnlock()

	return len(q.items)
}

func (q *concurrentQueueImpl[T]) isEmptyLocked() bool {
	return len(q.items) == 0
}
