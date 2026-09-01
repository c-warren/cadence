package engineimpl

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/queue"
)

func (e *historyEngineImpl) DescribeTransferQueue(
	ctx context.Context,
	clusterName string,
) (*types.DescribeQueueResponse, error) {
	return e.describeQueue(ctx, persistence.HistoryTaskCategoryTransfer, clusterName)
}

func (e *historyEngineImpl) DescribeTimerQueue(
	ctx context.Context,
	clusterName string,
) (*types.DescribeQueueResponse, error) {
	return e.describeQueue(ctx, persistence.HistoryTaskCategoryTimer, clusterName)
}

func (e *historyEngineImpl) describeQueue(
	ctx context.Context,
	category persistence.HistoryTaskCategory,
	clusterName string,
) (*types.DescribeQueueResponse, error) {
	queueProcessor, ok := e.queueProcessors[category]
	if !ok {
		return nil, fmt.Errorf("queue processor not found for category %v", category)
	}
	resp, err := queueProcessor.HandleAction(ctx, clusterName, queue.NewGetStateAction())
	if err != nil {
		return nil, err
	}

	serializedStates := make([]string, 0, len(resp.GetStateActionResult.States))
	for _, state := range resp.GetStateActionResult.States {
		serializedStates = append(serializedStates, e.serializeQueueState(state))
	}
	return &types.DescribeQueueResponse{
		ProcessingQueueStates: serializedStates,
	}, nil
}

func (e *historyEngineImpl) serializeQueueState(
	state queue.ProcessingQueueState,
) string {
	return fmt.Sprintf("%v", state)
}
