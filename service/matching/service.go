package matching

import (
	"sync/atomic"
	"time"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/clientcommon"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/matching/config"
	"github.com/uber/cadence/service/matching/handler"
	"github.com/uber/cadence/service/matching/wrappers/grpc"
	"github.com/uber/cadence/service/matching/wrappers/thrift"
)

// Service represents the cadence-matching service
type Service struct {
	resource.Resource

	status                         int32
	handler                        handler.Handler
	stopC                          chan struct{}
	config                         *config.Config
	ShardDistributorMatchingConfig clientcommon.Config
	drainObserver                  clientcommon.DrainSignalObserver
	percentageOnboarded            membership.PercentageOnboarded
}

// NewService builds a new cadence-matching service
func NewService(
	params *resource.Params,
) (resource.Resource, error) {

	serviceConfig := config.NewConfig(
		params.DynamicCollection,
		params.OperationalDynamicConfig,
		params.HostName,
		params.RPCConfig,
		params.GetIsolationGroups,
	)

	serviceResource, err := resource.New(
		params,
		service.Matching,
		&service.Config{
			PersistenceMaxQPS:        serviceConfig.PersistenceMaxQPS,
			PersistenceGlobalMaxQPS:  serviceConfig.PersistenceGlobalMaxQPS,
			ThrottledLoggerMaxRPS:    serviceConfig.ThrottledLogRPS,
			IsErrorRetryableFunction: common.IsServiceTransientError,
			// matching doesn't need visibility config as it never read or write visibility
		},
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource:                       serviceResource,
		status:                         common.DaemonStatusInitialized,
		config:                         serviceConfig,
		stopC:                          make(chan struct{}),
		ShardDistributorMatchingConfig: params.ShardDistributorMatchingConfig,
		drainObserver:                  params.DrainObserver,
		percentageOnboarded:            params.PercentageOnboarded,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("matching starting")

	engine := handler.NewEngine(
		s.GetTaskManager(),
		s.GetClusterMetadata(),
		s.GetHistoryClient(),
		s.GetMatchingRawClient(), // Use non retry client inside matching
		s.config,
		s.GetLogger(),
		s.GetZapLogger(),
		s.GetMetricsClient(),
		s.GetMetricsScope(),
		s.GetDomainCache(),
		s.GetMembershipResolver(),
		s.GetIsolationGroupState(),
		s.GetTimeSource(),
		s.GetShardDistributorExecutorClient(),
		s.ShardDistributorMatchingConfig,
		s.drainObserver,
		s.percentageOnboarded,
	)

	s.handler = handler.NewHandler(engine, s.config, s.GetDomainCache(), s.GetMetricsClient(), s.GetLogger(), s.GetThrottledLogger())

	thriftHandler := thrift.NewThriftHandler(s.handler)
	thriftHandler.Register(s.GetDispatcher())

	grpcHandler := grpc.NewGRPCHandler(s.handler)
	grpcHandler.Register(s.GetDispatcher())

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	logger.Info("matching started")

	<-s.stopC
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// remove self from membership ring and wait for traffic to drain
	s.GetLogger().Info("ShutdownHandler: Evicting self from membership ring")
	s.GetMembershipResolver().EvictSelf()
	s.GetLogger().Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	close(s.stopC)

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("matching stopped")
}
