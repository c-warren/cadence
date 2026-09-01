package nosql

import (
	"fmt"

	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

// shardingPolicy holds the configuration for the sharding logic
type shardingPolicy struct {
	logger       log.Logger
	defaultShard string
	config       config.ShardedNoSQL

	hasShardedHistory  bool
	hasShardedTasklist bool
}

func newShardingPolicy(logger log.Logger, cfg config.ShardedNoSQL) (shardingPolicy, error) {
	sp := shardingPolicy{
		logger:       logger,
		defaultShard: cfg.DefaultShard,
		config:       cfg,
	}

	err := sp.parse()
	if err != nil {
		return sp, err
	}

	return sp, nil
}

func (sp *shardingPolicy) parse() error {
	sp.parseHistoryShardMapping()
	sp.parseTaskListShardingPolicy()
	return nil
}

func (sp *shardingPolicy) parseHistoryShardMapping() {
	historyShardMapping := sp.config.ShardingPolicy.HistoryShardMapping
	if len(historyShardMapping) == 0 {
		return
	}
	sp.hasShardedHistory = true
}

func (sp *shardingPolicy) parseTaskListShardingPolicy() {
	tlShards := sp.config.ShardingPolicy.TaskListHashing.ShardOrder
	if len(tlShards) == 0 {
		return
	}
	sp.hasShardedTasklist = true
}

func (sp *shardingPolicy) getHistoryShardName(shardID int) (string, error) {
	if !sp.hasShardedHistory {
		sp.logger.Debug("Selected default store shard for history shard", tag.StoreShard(sp.defaultShard), tag.ShardID(shardID))
		return sp.defaultShard, nil
	}

	for _, r := range sp.config.ShardingPolicy.HistoryShardMapping {
		if shardID >= r.Start && shardID < r.End {
			sp.logger.Debug("Selected store shard history shard", tag.StoreShard(r.Shard), tag.ShardID(shardID))
			return r.Shard, nil
		}
	}

	return "", &ShardingError{
		Message: fmt.Sprintf("Failed to identify store shard for shardID %v", shardID),
	}
}

func (sp *shardingPolicy) getTaskListShardName(domainID string, taskListName string, taskType int) string {
	if !sp.hasShardedTasklist {
		sp.logger.Debug("Selected default store shard for tasklist", tag.StoreShard(sp.defaultShard), tag.WorkflowTaskListName(taskListName))
		return sp.defaultShard
	}
	tlShards := sp.config.ShardingPolicy.TaskListHashing.ShardOrder
	tlShardCount := len(tlShards)
	hash := farm.Hash32([]byte(domainID+"_"+taskListName)) % uint32(tlShardCount)
	shardIndex := int(hash) % tlShardCount

	sp.logger.Debug("Selected store shard tasklist", tag.StoreShard(tlShards[shardIndex]), tag.WorkflowTaskListName(taskListName))
	return tlShards[shardIndex]
}
