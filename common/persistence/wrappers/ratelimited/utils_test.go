package ratelimited

import (
	"context"

	"golang.org/x/time/rate"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/quotas"
)

type limiterAlwaysAllow struct{}

func (l limiterAlwaysAllow) Allow() bool {
	return true
}

func (l limiterAlwaysAllow) Wait(ctx context.Context) error {
	return nil
}

func (l limiterAlwaysAllow) Reserve() clock.Reservation {
	return &reservationAlwaysAllow{}
}

func (l limiterAlwaysAllow) Limit() rate.Limit {
	return rate.Inf
}

type limiterNeverAllow struct{}

func (l limiterNeverAllow) Allow() bool {
	return false
}

func (l limiterNeverAllow) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (l limiterNeverAllow) Reserve() clock.Reservation {
	return &reservationNeverAllow{}
}

func (l limiterNeverAllow) Limit() rate.Limit {
	return 0
}

type reservationAlwaysAllow struct{}
type reservationNeverAllow struct{}

func (r reservationAlwaysAllow) Allow() bool { return true }
func (r reservationAlwaysAllow) Used(bool)   {}
func (r reservationNeverAllow) Allow() bool  { return false }
func (r reservationNeverAllow) Used(bool)    {}

var _ quotas.Limiter = (*limiterAlwaysAllow)(nil)
var _ quotas.Limiter = (*limiterNeverAllow)(nil)
var _ clock.Reservation = (*reservationAlwaysAllow)(nil)
var _ clock.Reservation = (*reservationNeverAllow)(nil)
