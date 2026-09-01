package sqlplugin

import (
	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/common/persistence/serialization"
)

// This section defines the special dbShardID, they must all below 0
const (
	// this means the query need to execute in one shard but the shard should be fixed/static, e.g. for domain, queue storage are single shard
	DbDefaultShard = 0
	// this is should never being used in sharded SQL driver. It is used in admin/schema operation in singleton driver, which ignores all the shardID parameter
	DbShardUndefined = -1
	// this means the query needs to execute in all dbShards in sharded SQL driver (currently not supported)
	DbAllShards = -2
)

// GetDBShardIDFromHistoryShardID maps  historyShardID to a DBShardID
func GetDBShardIDFromHistoryShardID(historyShardID int, numDBShards int) int {
	return historyShardID % numDBShards
}

// GetDBShardIDFromDomainIDAndTasklist maps <domainID, tasklistName> to a DBShardID
func GetDBShardIDFromDomainIDAndTasklist(domainID, tasklistName string, numDBShards int) int {
	hash := farm.Hash32([]byte(domainID+"_"+tasklistName)) % uint32(numDBShards)
	return int(hash) % numDBShards
}

// GetDBShardIDFromDomainID maps domainID to a DBShardID
func GetDBShardIDFromDomainID(domainID string, numDBShards int) int {
	hash := farm.Hash32([]byte(domainID)) % uint32(numDBShards)
	return int(hash) % numDBShards
}

// GetDBShardIDFromTreeID maps treeID to a DBShardID
func GetDBShardIDFromTreeID(treeID serialization.UUID, numDBShards int) int {
	hash := farm.Hash32(treeID) % uint32(numDBShards)
	return int(hash) % numDBShards
}
