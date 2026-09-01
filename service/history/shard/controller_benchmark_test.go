package shard

import (
	"sync"
	"testing"
)

// BenchmarkController_ShardIDs-96         52588224               293.9 ns/op            66 B/op          0 allocs/op
// go test -bench=. --benchtime=10s --benchmem
// goos: linux
// goarch: amd64
// pkg: github.com/uber/cadence/service/history/shard
// cpu: AMD EPYC 7B13
// With the old approach, the benchmark result is:
// BenchmarkController_ShardIDs-96            39314            324629 ns/op          272333 B/op         19 allocs/op
func BenchmarkController_ShardIDs(b *testing.B) {
	numShards := 16384
	historyShards := make(map[int]*historyShardsItem)
	for i := 0; i < numShards; i++ {
		historyShards[i] = &historyShardsItem{shardID: i}
	}
	shardController := &controller{
		historyShards: historyShards,
	}
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		if i%1000 == 0 { // update is much much less frequent than read
			wg.Add(1)
			go func() {
				defer wg.Done()
				shardController.Lock()
				shardController.updateShardIDSnapshotLocked()
				shardController.Unlock()
			}()
		}
		shardController.ShardIDs()
		shardController.NumShards()
	}
	wg.Wait()
}
