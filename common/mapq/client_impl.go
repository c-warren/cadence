package mapq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/mapq/tree"
	"github.com/uber/cadence/common/mapq/types"
	"github.com/uber/cadence/common/metrics"
)

type clientImpl struct {
	logger          log.Logger
	scope           metrics.Scope
	persister       types.Persister
	consumerFactory types.ConsumerFactory
	tree            *tree.QueueTree
	partitions      []string
	policies        []types.NodePolicy
}

func (c *clientImpl) Start(ctx context.Context) error {
	c.logger.Info("Starting MAPQ client")
	err := c.tree.Start(ctx)
	if err != nil {
		return err
	}

	c.logger.Info("Started MAPQ client")
	return nil
}

func (c *clientImpl) Stop(ctx context.Context) error {
	c.logger.Info("Stopping MAPQ client")

	// Stop the tree which will stop the dispatchers
	if err := c.tree.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop tree: %w", err)
	}

	// stop the consumer factory which will stop the consumers
	err := c.consumerFactory.Stop(ctx)
	if err != nil {
		return fmt.Errorf("failed to stop consumer factory: %w", err)
	}

	c.logger.Info("Stopped MAPQ client")
	return nil
}

func (c *clientImpl) Enqueue(ctx context.Context, items []types.Item) ([]types.ItemToPersist, error) {
	return c.tree.Enqueue(ctx, items)
}

func (c *clientImpl) Ack(context.Context, types.Item) error {
	return errors.New("not implemented")
}

func (c *clientImpl) Nack(context.Context, types.Item) error {
	return errors.New("not implemented")
}
