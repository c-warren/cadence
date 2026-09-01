package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mapq/types"
)

type Dispatcher struct {
	consumer  types.Consumer
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
}

func New(c types.Consumer) *Dispatcher {
	ctx, cancelCtx := context.WithCancel(context.Background())
	return &Dispatcher{
		consumer:  c,
		ctx:       ctx,
		cancelCtx: cancelCtx,
	}
}

func (d *Dispatcher) Start(ctx context.Context) error {
	d.wg.Add(1)
	go d.run()
	return nil
}

func (d *Dispatcher) Stop(ctx context.Context) error {
	d.cancelCtx()
	timeout := 10 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}
	if !common.AwaitWaitGroup(&d.wg, timeout) {
		return fmt.Errorf("failed to stop dispatcher in %v", timeout)
	}
	return nil
}

func (d *Dispatcher) run() {
	defer d.wg.Done()
	// TODO: implement
}
