package matching

import (
	"sync/atomic"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

type (
	key struct {
		domainID     string
		taskListName string
		taskListType int
	}

	roundRobinLoadBalancer struct {
		provider   PartitionConfigProvider
		readCache  cache.Cache
		writeCache cache.Cache

		pickPartitionFn func(domainName string, taskList types.TaskList, taskListType int, nPartitions int, partitionCache cache.Cache) string
	}
)

func NewRoundRobinLoadBalancer(
	provider PartitionConfigProvider,
) LoadBalancer {
	return &roundRobinLoadBalancer{
		provider: provider,
		readCache: cache.New(&cache.Options{
			TTL:             0,
			InitialCapacity: 100,
			Pin:             false,
			MaxCount:        3000,
			ActivelyEvict:   false,
			MetricsScope:    provider.GetMetricsClient().Scope(metrics.LoadBalancerScope).Tagged(metrics.NonShardTag()),
			Logger:          provider.GetLogger(),
		}),
		writeCache: cache.New(&cache.Options{
			TTL:             0,
			InitialCapacity: 100,
			Pin:             false,
			MaxCount:        3000,
			ActivelyEvict:   false,
			MetricsScope:    provider.GetMetricsClient().Scope(metrics.LoadBalancerScope).Tagged(metrics.NonShardTag()),
			Logger:          provider.GetLogger(),
		}),
		pickPartitionFn: pickPartition,
	}
}

func (lb *roundRobinLoadBalancer) PickWritePartition(
	taskListType int,
	req WriteRequest,
) string {
	nPartitions := lb.provider.GetNumberOfWritePartitions(req.GetDomainUUID(), *req.GetTaskList(), taskListType)
	return lb.pickPartitionFn(req.GetDomainUUID(), *req.GetTaskList(), taskListType, nPartitions, lb.writeCache)
}

func (lb *roundRobinLoadBalancer) PickReadPartition(
	taskListType int,
	req ReadRequest,
	_ string,
) string {
	n := lb.provider.GetNumberOfReadPartitions(req.GetDomainUUID(), *req.GetTaskList(), taskListType)
	return lb.pickPartitionFn(req.GetDomainUUID(), *req.GetTaskList(), taskListType, n, lb.readCache)
}

func pickPartition(
	domainID string,
	taskList types.TaskList,
	taskListType int,
	nPartitions int,
	partitionCache cache.Cache,
) string {
	taskListName := taskList.GetName()
	if nPartitions <= 1 {
		return taskListName
	}

	taskListKey := key{
		domainID:     domainID,
		taskListName: taskListName,
		taskListType: taskListType,
	}

	valI := partitionCache.Get(taskListKey)
	if valI == nil {
		val := int64(-1)
		var err error
		valI, err = partitionCache.PutIfNotExist(taskListKey, &val)
		if err != nil {
			return taskListName
		}
	}
	valAddr, ok := valI.(*int64)
	if !ok {
		return taskListName
	}

	p := int(atomic.AddInt64(valAddr, 1) % int64(nPartitions))
	return getPartitionTaskListName(taskList.GetName(), p)
}

func (lb *roundRobinLoadBalancer) UpdateWeight(
	taskListType int,
	req ReadRequest,
	partition string,
	info *types.LoadBalancerHints,
) {
}
