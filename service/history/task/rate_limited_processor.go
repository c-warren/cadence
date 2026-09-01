package task

import (
	"context"
	"sync/atomic"

	"github.com/uber/cadence/common"
)

type rateLimitedProcessor struct {
	baseProcessor Processor
	rateLimiter   RateLimiter
	cancelCtx     context.Context
	cancelFn      context.CancelFunc
	status        int32
}

func NewRateLimitedProcessor(
	baseProcessor Processor,
	rateLimiter RateLimiter,
) Processor {
	ctx, cancel := context.WithCancel(context.Background())
	return &rateLimitedProcessor{
		baseProcessor: baseProcessor,
		rateLimiter:   rateLimiter,
		cancelCtx:     ctx,
		cancelFn:      cancel,
		status:        common.DaemonStatusInitialized,
	}
}

func (p *rateLimitedProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.baseProcessor.Start()
}

func (p *rateLimitedProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.cancelFn()
	p.baseProcessor.Stop()
}

func (p *rateLimitedProcessor) Submit(t Task) error {
	if err := p.rateLimiter.Wait(p.cancelCtx, t); err != nil {
		return err
	}
	return p.baseProcessor.Submit(t)
}

func (p *rateLimitedProcessor) TrySubmit(t Task) (bool, error) {
	if ok := p.rateLimiter.Allow(t); !ok {
		return false, nil
	}
	return p.baseProcessor.TrySubmit(t)
}
