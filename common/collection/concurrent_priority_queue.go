package collection

import "sync"

type (
	concurrentPriorityQueueImpl[T any] struct {
		sync.RWMutex
		priorityQueue Queue[T]
	}
)

// NewConcurrentPriorityQueue create a new concurrent priority queue
func NewConcurrentPriorityQueue[T any](compareLess func(this T, other T) bool) Queue[T] {
	return &concurrentPriorityQueueImpl[T]{
		priorityQueue: NewPriorityQueue(compareLess),
	}
}

// Peek returns the top item of the priority queue
func (pq *concurrentPriorityQueueImpl[T]) Peek() (T, error) {
	pq.RLock()
	defer pq.RUnlock()

	return pq.priorityQueue.Peek()
}

// Add push an item to priority queue
func (pq *concurrentPriorityQueueImpl[T]) Add(item T) {
	pq.Lock()
	defer pq.Unlock()

	pq.priorityQueue.Add(item)
}

// Remove pop an item from priority queue
func (pq *concurrentPriorityQueueImpl[T]) Remove() (T, error) {
	pq.Lock()
	defer pq.Unlock()

	return pq.priorityQueue.Remove()
}

// IsEmpty indicate if the priority queue is empty
func (pq *concurrentPriorityQueueImpl[T]) IsEmpty() bool {
	pq.RLock()
	defer pq.RUnlock()

	return pq.priorityQueue.IsEmpty()
}

// Len return the size of the queue
func (pq *concurrentPriorityQueueImpl[T]) Len() int {
	pq.RLock()
	defer pq.RUnlock()

	return pq.priorityQueue.Len()
}
