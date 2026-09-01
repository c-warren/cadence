package log

import (
	"math/rand"
	"sync/atomic"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/quotas"
)

type throttledLogger struct {
	rps     int32
	limiter quotas.Limiter
	log     Logger
}

const skipForThrottleLogger = 6

// NewThrottledLogger returns an implementation of logger that throttles the
// log messages being emitted. The underlying implementation uses a token bucket
// ratelimiter and stops emitting logs once the bucket runs out of tokens
//
// Fatal/Panic logs are always emitted without any throttling
func NewThrottledLogger(logger Logger, rps dynamicproperties.IntPropertyFn) Logger {
	var log Logger
	lg, ok := logger.(*loggerImpl)
	if ok {
		log = &loggerImpl{
			zapLogger:     lg.zapLogger,
			skip:          skipForThrottleLogger,
			sampleLocalFn: lg.sampleLocalFn,
		}
	} else {
		logger.Warn("ReplayLogger may not emit callat tag correctly because the logger passed in is not loggerImpl")
		log = logger
	}

	rate := rps()
	limiter := quotas.NewDynamicRateLimiter(func() float64 {
		return float64(rps())
	})
	tl := &throttledLogger{
		limiter: limiter,
		rps:     int32(rate),
		log:     log,
	}
	return tl
}

func (tl *throttledLogger) Debugf(msg string, args ...any) {
	tl.rateLimit(func() {
		tl.log.Debugf(msg, args...)
	})
}

func (tl *throttledLogger) Debug(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Debug(msg, tags...)
	})
}

func (tl *throttledLogger) Info(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Info(msg, tags...)
	})
}

func (tl *throttledLogger) Warn(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Warn(msg, tags...)
	})
}

func (tl *throttledLogger) Error(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Error(msg, tags...)
	})
}

func (tl *throttledLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Fatal(msg, tags...)
	})
}

func (tl *throttledLogger) SampleInfo(msg string, sampleRate int, tags ...tag.Tag) {
	if rand.Intn(sampleRate) == 0 {
		tl.rateLimit(func() {
			tl.log.Info(msg, tags...)
		})
	}
}

func (tl *throttledLogger) DebugOn() bool {
	return tl.log.DebugOn()
}

// Return a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (tl *throttledLogger) WithTags(tags ...tag.Tag) Logger {
	result := &throttledLogger{
		rps:     atomic.LoadInt32(&tl.rps),
		limiter: tl.limiter,
		log:     tl.log.WithTags(tags...),
	}
	return result
}

func (tl *throttledLogger) rateLimit(f func()) {
	if ok := tl.limiter.Allow(); ok {
		f()
	}
}

func (tl *throttledLogger) Helper() Logger {
	result := &throttledLogger{
		rps:     atomic.LoadInt32(&tl.rps),
		limiter: tl.limiter,
		log:     tl.log.Helper(),
	}
	return result
}
