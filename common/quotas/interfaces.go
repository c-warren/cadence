//go:generate mockgen -package=$GOPACKAGE -destination=limiter_mock.go github.com/uber/cadence/common/quotas Limiter
//go:generate mockgen -package=$GOPACKAGE -destination=policy_mock.go github.com/uber/cadence/common/quotas Policy

package quotas

import (
	"context"

	"golang.org/x/time/rate"

	"github.com/uber/cadence/common/clock"
)

// RPSFunc returns a float64 as the RPS
type RPSFunc func() float64

// RPSKeyFunc returns a float64 as the RPS for the given key
type RPSKeyFunc func(key string) float64

// Info corresponds to information required to determine rate limits
type Info struct {
	Domain   string
	TaskList string
}

// Limiter corresponds to basic rate limiting functionality.
//
// TODO: This can likely be replaced with clock.Ratelimiter, now that it exists,
// but it is being left as a read-only mirror for now as only these methods are
// currently needed in areas that currently use this Limiter.
type Limiter interface {
	// Allow attempts to allow a request to go through. The method returns
	// immediately with a true or false indicating if the request can make
	// progress
	Allow() bool

	// Wait waits till the deadline for a rate limit token to allow the request
	// to go through.
	Wait(ctx context.Context) error

	// Reserve reserves a rate limit token
	Reserve() clock.Reservation

	// Limit returns the current configured ratelimit.
	//
	// If this Limiter wraps multiple values, this is generally the "most relevant" one,
	// i.e. the one that is most likely to apply to the next request
	Limit() rate.Limit
}

// Policy corresponds to a quota policy. A policy allows implementing layered
// and more complex rate limiting functionality.
type Policy interface {
	// Allow attempts to allow a request to go through. The method returns
	// immediately with a true or false indicating if the request can make
	// progress
	Allow(info Info) bool

	// Wait waits up till the context deadline for a rate limit token to allow
	// the request to go through. Returns nil if request is allowed.
	Wait(ctx context.Context, info Info) error
}
