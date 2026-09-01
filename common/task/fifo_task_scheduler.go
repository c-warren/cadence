package task

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

type fifoTaskSchedulerImpl[T Task] struct {
	status       int32
	logger       log.Logger
	metricsScope metrics.Scope
	options      *FIFOTaskSchedulerOptions
	dispatcherWG sync.WaitGroup
	taskCh       chan T
	ctx          context.Context
	cancel       context.CancelFunc

	processor Processor
}

// NewFIFOTaskScheduler creates a new FIFO task scheduler
// it's an no-op implementation as it simply copy tasks from
// one task channel to another task channel.
// This scheduler is only for development purpose.
func NewFIFOTaskScheduler[T Task](
	logger log.Logger,
	metricsClient metrics.Client,
	options *FIFOTaskSchedulerOptions,
) Scheduler[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &fifoTaskSchedulerImpl[T]{
		status:       common.DaemonStatusInitialized,
		logger:       logger,
		metricsScope: metricsClient.Scope(metrics.TaskSchedulerScope),
		options:      options,
		taskCh:       make(chan T, options.QueueSize),
		ctx:          ctx,
		cancel:       cancel,
		processor: NewParallelTaskProcessor(
			logger,
			metricsClient,
			&ParallelTaskProcessorOptions{
				QueueSize:   options.QueueSize,
				WorkerCount: options.WorkerCount,
				RetryPolicy: options.RetryPolicy,
			},
		),
	}
}

func (f *fifoTaskSchedulerImpl[T]) Start() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	f.processor.Start()

	f.dispatcherWG.Add(f.options.DispatcherCount)
	for i := 0; i != f.options.DispatcherCount; i++ {
		go f.dispatcher()
	}

	f.logger.Info("FIFO task scheduler started.")
}

func (f *fifoTaskSchedulerImpl[T]) Stop() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	f.cancel()

	f.processor.Stop()

	f.drainAndNackTasks()

	if success := common.AwaitWaitGroup(&f.dispatcherWG, time.Minute); !success {
		f.logger.Warn("FIFO task scheduler timedout on shutdown.")
	}

	f.logger.Info("FIFO task scheduler shutdown.")
}

func (f *fifoTaskSchedulerImpl[T]) Submit(task T) error {
	f.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
	submitStart := time.Now()
	sw := f.metricsScope.StartTimer(metrics.ParallelTaskSubmitLatency)
	defer func() {
		sw.Stop()
		f.metricsScope.ExponentialHistogram(metrics.ParallelTaskSubmitLatencyHistogram, time.Since(submitStart))
	}()

	if f.isStopped() {
		return ErrTaskSchedulerClosed
	}

	select {
	case f.taskCh <- task:
		if f.isStopped() {
			f.drainAndNackTasks()
		}
		return nil
	case <-f.ctx.Done():
		return ErrTaskSchedulerClosed
	}
}

func (f *fifoTaskSchedulerImpl[T]) TrySubmit(task T) (bool, error) {
	if f.isStopped() {
		return false, ErrTaskSchedulerClosed
	}

	select {
	case f.taskCh <- task:
		f.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
		if f.isStopped() {
			f.drainAndNackTasks()
		}
		return true, nil
	case <-f.ctx.Done():
		return false, ErrTaskSchedulerClosed
	default:
		return false, nil
	}
}

func (f *fifoTaskSchedulerImpl[T]) dispatcher() {
	defer f.dispatcherWG.Done()

	for {
		select {
		case task := <-f.taskCh:
			if err := f.processor.Submit(task); err != nil {
				f.logger.Error("failed to submit task to processor", tag.Error(err))
				task.Nack(err)
			}
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *fifoTaskSchedulerImpl[T]) isStopped() bool {
	return atomic.LoadInt32(&f.status) == common.DaemonStatusStopped
}

func (f *fifoTaskSchedulerImpl[T]) drainAndNackTasks() {
	for {
		select {
		case task := <-f.taskCh:
			task.Nack(nil)
		default:
			return
		}
	}
}
