package mapq

import (
	"fmt"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/mapq/tree"
	"github.com/uber/cadence/common/mapq/types"
	"github.com/uber/cadence/common/metrics"
)

type Options func(*clientImpl)

func WithPersister(p types.Persister) Options {
	return func(c *clientImpl) {
		c.persister = p
	}
}

func WithConsumerFactory(cf types.ConsumerFactory) Options {
	return func(c *clientImpl) {
		c.consumerFactory = cf
	}
}

// WithPartitions sets the partition keys for each level.
// MAPQ creates a tree with depth = len(partitions)
func WithPartitions(partitions []string) Options {
	return func(c *clientImpl) {
		c.partitions = partitions
	}
}

// WithPolicies sets the policies for the MAPQ instance.
// Policies can be defined for nodes at a specific level or nodes with specific path.
//
// Path conventions:
// - "*" -> represents the root node at level 0
// - "*/." matches with all nodes at level 1
// - "*/*" represents the catch-all node at level 1
// - "*/xyz" represents a specific node at level 1 whose partition value is xyz
// - "*/./." matches with all nodes at level 2
// - "*/xyz/." matches with all nodes at level 2 whose parent is xyz node
// - "*/xyz/*" represents the catch-all node at level 2 whose parent is xyz node
// - "*/xyz/abc" represents a specific node at level 2 whose level 2 attribute value is abc and parent is xyz node
func WithPolicies(policies []types.NodePolicy) Options {
	return func(c *clientImpl) {
		c.policies = policies
	}
}

func New(logger log.Logger, scope metrics.Scope, opts ...Options) (types.Client, error) {
	c := &clientImpl{
		logger: logger.WithTags(tag.ComponentMapQ),
		scope:  scope,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.persister == nil {
		return nil, fmt.Errorf("persister is required. Use WithPersister option to set it")
	}

	if c.consumerFactory == nil {
		return nil, fmt.Errorf("consumer factory is required. Use WithConsumerFactory option to set it")
	}

	tree, err := tree.New(logger, scope, c.partitions, c.policies, c.persister, c.consumerFactory)
	if err != nil {
		return nil, err
	}

	c.tree = tree
	c.logger.Info("MAPQ client created",
		tag.Dynamic("partitions", c.partitions),
		tag.Dynamic("policies", c.policies),
		tag.Dynamic("tree", c.tree.String()),
	)

	return c, nil
}
