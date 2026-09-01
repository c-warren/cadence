package lookup

import (
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/service"
)

// HistoryServerByShardID calls resolver.Lookup with key based on provided shardID
func HistoryServerByShardID(resolver membership.Resolver, shardID int) (membership.HostInfo, error) {
	return resolver.Lookup(service.History, string(rune(shardID)))
}
