package nosql

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	. "github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
)

func TestSimplePolicyWithSharding(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()

	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)
	require.True(t, sp.hasShardedHistory, "sp.hasShardedHistory must be true")
	require.True(t, sp.hasShardedTasklist, "sp.hasShardedTasklist must be true")
}

func TestSimplePolicyWithoutSharding(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	cfg.ShardingPolicy = ShardingPolicy{} // remove the sharding policy

	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)
	require.False(t, sp.hasShardedHistory, "sp.hasShardedHistory must be false")
	require.False(t, sp.hasShardedTasklist, "sp.hasShardedTasklist must be false")
}

func TestSimplePolicyWithoutHistorySharding(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	cfg.ShardingPolicy.HistoryShardMapping = []HistoryShardRange{} // remove only the history sharding

	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)
	require.False(t, sp.hasShardedHistory, "sp.hasShardedHistory must be false")
	require.True(t, sp.hasShardedTasklist, "sp.hasShardedTasklist must be true")
}

func TestSimplePolicyWithoutTasklistSharding(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	cfg.ShardingPolicy.TaskListHashing = TasklistHashing{} // remove only the tasklist sharding

	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)
	require.True(t, sp.hasShardedHistory, "sp.hasShardedHistory must be false")
	require.False(t, sp.hasShardedTasklist, "sp.hasShardedTasklist must be true")
}

func TestHistorySharding(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)

	shardName0, err1 := sp.getHistoryShardName(0)
	shardName1, err2 := sp.getHistoryShardName(1)

	require.Nil(t, err1)
	require.Nil(t, err2)
	require.Equal(t, "shard-1", shardName0, "shard name must be correct for shard 0")
	require.Equal(t, "shard-2", shardName1, "shard name must be correct for shard 1")
}

func TestHistorySharding_UnexpectedGapInHistoryRanges(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	cfg.ShardingPolicy.HistoryShardMapping = []HistoryShardRange{
		HistoryShardRange{
			Start: 0,
			End:   1,
			Shard: "shard-1",
		},
		HistoryShardRange{
			Start: 2,
			End:   3,
			Shard: "shard-2",
		},
	}
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)

	shardName0, err1 := sp.getHistoryShardName(0)
	shardName2, err2 := sp.getHistoryShardName(2)

	require.Nil(t, err1)
	require.Nil(t, err2)
	require.Equal(t, "shard-1", shardName0, "shard name must be correct for shard 0")
	require.Equal(t, "shard-2", shardName2, "shard name must be correct for shard 2")

	unknownShard, err := sp.getHistoryShardName(1)
	require.ErrorContains(t, err, "Failed to identify store shard")
	require.Empty(t, unknownShard)
}

func TestHistorySharding_OutOfBounds(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)

	unknownShard, err := sp.getHistoryShardName(-1)
	require.ErrorContains(t, err, "Failed to identify store shard")
	require.Empty(t, unknownShard)

	unknownShard, err = sp.getHistoryShardName(2)
	require.ErrorContains(t, err, "Failed to identify store shard")
	require.Empty(t, unknownShard)
}

func TestTaskListSharding_Simple(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)

	// Note that shardName0 and shardName1 can't really be predicted due to hashing inside the function. The values
	// below were picked manually to yield the outcome needed. The test is useful to ensure the logic is deterministic.
	shardName1 := sp.getTaskListShardName("domain1", "tl1", 0)
	shardName2 := sp.getTaskListShardName("domain1", "tl2", 0)

	require.Equal(t, "shard-1", shardName1, "shard name must be correct for tl1")
	require.Equal(t, "shard-2", shardName2, "shard name must be correct for tl2")
}

func TestTaskListSharding_SingleShard(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	cfg.ShardingPolicy.TaskListHashing.ShardOrder = []string{"only-shard"}
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)
	require.True(t, sp.hasShardedTasklist, "sp.hasShardedTasklist must be true")

	countExpected := 0
	countUnexpected := 0

	for i := 0; i < 1000000; i++ {
		s := sp.getTaskListShardName(uuid.New(), uuid.New(), 0)
		if s == "only-shard" {
			countExpected++
		} else {
			countUnexpected++
		}
	}

	require.Equal(t, 0, countUnexpected, "Unexpected shard names must not be returned")
	require.Equal(t, 1000000, countExpected, "Expected shard name must be returned always")
}

func TestTaskListSharding_Probabilistic(t *testing.T) {
	cfg := getValidShardedNoSQLConfig()
	sp, err := newShardingPolicy(log.NewNoop(), cfg)
	require.NoError(t, err)

	countShard1 := 0
	countShard2 := 0
	countUnexpected := 0

	for i := 0; i < 1000000; i++ {
		s := sp.getTaskListShardName(uuid.New(), uuid.New(), 0)
		if s == "shard-1" {
			countShard1++
		} else if s == "shard-2" {
			countShard2++
		} else {
			countUnexpected++
		}
	}

	ratio := float32(countShard1) / float32(countShard2)

	require.Equal(t, 0, countUnexpected, "Unexpected shard names must not be returned")
	require.True(t, ratio > 0.95 && ratio < 1.05, "Shards should have equal chance of being selected")
}
