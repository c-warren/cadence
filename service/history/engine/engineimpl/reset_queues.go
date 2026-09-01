package engineimpl

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/queue"
)

func (e *historyEngineImpl) ResetTransferQueue(
	ctx context.Context,
	clusterName string,
) error {
	transferProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTransfer]
	if !ok {
		return fmt.Errorf("transfer processor not found")
	}
	_, err := transferProcessor.HandleAction(ctx, clusterName, queue.NewResetAction())
	return err
}

func (e *historyEngineImpl) ResetTimerQueue(
	ctx context.Context,
	clusterName string,
) error {
	timerProcessor, ok := e.queueProcessors[persistence.HistoryTaskCategoryTimer]
	if !ok {
		return fmt.Errorf("timer processor not found")
	}
	_, err := timerProcessor.HandleAction(ctx, clusterName, queue.NewResetAction())
	return err
}
