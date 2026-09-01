//go:generate mockgen -package=$GOPACKAGE -destination=collection_mock.go github.com/uber/cadence/common/quotas ICollection
//go:generate mockgen -package=$GOPACKAGE -destination=limiterfactory_mock.go github.com/uber/cadence/common/quotas LimiterFactory

package quotas

import (
	"sync"
)

// LimiterFactory is used to create a Limiter for a given domain
type LimiterFactory[K comparable] interface {
	// GetLimiter returns a new Limiter for the given domain
	GetLimiter(key K) Limiter
}

// Collection stores a map of limiters by key
type Collection[K comparable] struct {
	mu       sync.RWMutex
	factory  LimiterFactory[K]
	limiters map[K]Limiter
}

type ICollection[K comparable] interface {
	For(key K) Limiter
}

var _ ICollection[string] = (*Collection[string])(nil)

// NewCollection create a new limiter collection.
// Given factory is called to create new individual limiter.
func NewCollection[K comparable](factory LimiterFactory[K]) *Collection[K] {
	return &Collection[K]{
		factory:  factory,
		limiters: make(map[K]Limiter),
	}
}

// For retrieves limiter by a given key.
// If limiter for such key does not exists, it creates new one with via factory.
func (c *Collection[K]) For(key K) Limiter {
	c.mu.RLock()
	limiter, ok := c.limiters[key]
	c.mu.RUnlock()

	if !ok {
		// create a new limiter
		newLimiter := c.factory.GetLimiter(key)

		// verify that it is needed and add to map
		c.mu.Lock()
		limiter, ok = c.limiters[key]
		if !ok {
			c.limiters[key] = newLimiter
			limiter = newLimiter
		}
		c.mu.Unlock()
	}

	return limiter
}
