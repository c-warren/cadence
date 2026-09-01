package matching

import (
	"strings"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
)

type (
	multiLoadBalancer struct {
		defaultLoadBalancer  LoadBalancer
		loadBalancers        map[string]LoadBalancer
		domainIDToName       func(string) (string, error)
		loadbalancerStrategy dynamicproperties.StringPropertyFnWithTaskListInfoFilters
		logger               log.Logger
	}
)

func NewMultiLoadBalancer(
	defaultLoadBalancer LoadBalancer,
	loadBalancers map[string]LoadBalancer,
	domainIDToName func(string) (string, error),
	dc *dynamicconfig.Collection,
	logger log.Logger,
) LoadBalancer {
	return &multiLoadBalancer{
		defaultLoadBalancer:  defaultLoadBalancer,
		loadBalancers:        loadBalancers,
		domainIDToName:       domainIDToName,
		loadbalancerStrategy: dc.GetStringPropertyFilteredByTaskListInfo(dynamicproperties.TasklistLoadBalancerStrategy),
		logger:               logger,
	}
}

func (lb *multiLoadBalancer) PickWritePartition(
	taskListType int,
	req WriteRequest,
) string {
	if !lb.canRedirectToPartition(req) {
		return req.GetTaskList().GetName()
	}
	domainName, err := lb.domainIDToName(req.GetDomainUUID())
	if err != nil {
		return lb.defaultLoadBalancer.PickWritePartition(taskListType, req)
	}
	strategy := lb.loadbalancerStrategy(domainName, req.GetTaskList().GetName(), taskListType)
	loadBalancer, ok := lb.loadBalancers[strategy]
	if !ok {
		lb.logger.Warn("unsupported load balancer strategy", tag.Value(strategy))
		return lb.defaultLoadBalancer.PickWritePartition(taskListType, req)
	}
	return loadBalancer.PickWritePartition(taskListType, req)
}

func (lb *multiLoadBalancer) PickReadPartition(
	taskListType int,
	req ReadRequest,
	isolationGroup string,
) string {
	if !lb.canRedirectToPartition(req) {
		return req.GetTaskList().GetName()
	}
	domainName, err := lb.domainIDToName(req.GetDomainUUID())
	if err != nil {
		return lb.defaultLoadBalancer.PickReadPartition(taskListType, req, isolationGroup)
	}
	strategy := lb.loadbalancerStrategy(domainName, req.GetTaskList().GetName(), taskListType)
	loadBalancer, ok := lb.loadBalancers[strategy]
	if !ok {
		lb.logger.Warn("unsupported load balancer strategy", tag.Value(strategy))
		return lb.defaultLoadBalancer.PickReadPartition(taskListType, req, isolationGroup)
	}
	return loadBalancer.PickReadPartition(taskListType, req, isolationGroup)
}

func (lb *multiLoadBalancer) UpdateWeight(
	taskListType int,
	req ReadRequest,
	partition string,
	info *types.LoadBalancerHints,
) {
	if !lb.canRedirectToPartition(req) {
		return
	}
	domainName, err := lb.domainIDToName(req.GetDomainUUID())
	if err != nil {
		return
	}
	strategy := lb.loadbalancerStrategy(domainName, req.GetTaskList().GetName(), taskListType)
	loadBalancer, ok := lb.loadBalancers[strategy]
	if !ok {
		lb.logger.Warn("unsupported load balancer strategy", tag.Value(strategy))
		return
	}
	loadBalancer.UpdateWeight(taskListType, req, partition, info)
}

func (lb *multiLoadBalancer) canRedirectToPartition(req ReadRequest) bool {
	return req.GetForwardedFrom() == "" && req.GetTaskList().GetKind() == types.TaskListKindNormal && !strings.HasPrefix(req.GetTaskList().GetName(), constants.ReservedTaskListPrefix)
}
