//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/service/frontend/admin
//go:generate gowrap gen -g -p . -i Handler -t ../templates/accesscontrolled.tmpl -o ../wrappers/accesscontrolled/admin_generated.go -v handler=Admin
//go:generate gowrap gen -g -p . -i Handler -t ../../templates/grpc.tmpl -o ../wrappers/grpc/admin_generated.go -v handler=Admin -v package=adminv1 -v path=github.com/uber/cadence-idl/go/proto/admin/v1 -v prefix=Admin
//go:generate gowrap gen -g -p ../../../.gen/go/admin/adminserviceserver -i Interface -t ../../templates/thrift.tmpl -o ../wrappers/thrift/admin_generated.go -v handler=Admin -v prefix=Admin

package admin

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

// Handler interface for admin service
type Handler interface {
	common.Daemon

	AddSearchAttribute(context.Context, *types.AddSearchAttributeRequest) error
	CloseShard(context.Context, *types.CloseShardRequest) error
	DescribeCluster(context.Context) (*types.DescribeClusterResponse, error)
	DescribeShardDistribution(context.Context, *types.DescribeShardDistributionRequest) (*types.DescribeShardDistributionResponse, error)
	DescribeHistoryHost(context.Context, *types.DescribeHistoryHostRequest) (*types.DescribeHistoryHostResponse, error)
	DescribeQueue(context.Context, *types.DescribeQueueRequest) (*types.DescribeQueueResponse, error)
	DescribeWorkflowExecution(context.Context, *types.AdminDescribeWorkflowExecutionRequest) (*types.AdminDescribeWorkflowExecutionResponse, error)
	GetDLQReplicationMessages(context.Context, *types.GetDLQReplicationMessagesRequest) (*types.GetDLQReplicationMessagesResponse, error)
	GetDomainReplicationMessages(context.Context, *types.GetDomainReplicationMessagesRequest) (*types.GetDomainReplicationMessagesResponse, error)
	GetReplicationMessages(context.Context, *types.GetReplicationMessagesRequest) (*types.GetReplicationMessagesResponse, error)
	GetWorkflowExecutionRawHistoryV2(context.Context, *types.GetWorkflowExecutionRawHistoryV2Request) (*types.GetWorkflowExecutionRawHistoryV2Response, error)
	CountDLQMessages(context.Context, *types.CountDLQMessagesRequest) (*types.CountDLQMessagesResponse, error)
	MergeDLQMessages(context.Context, *types.MergeDLQMessagesRequest) (*types.MergeDLQMessagesResponse, error)
	PurgeDLQMessages(context.Context, *types.PurgeDLQMessagesRequest) error
	ReadDLQMessages(context.Context, *types.ReadDLQMessagesRequest) (*types.ReadDLQMessagesResponse, error)
	ReapplyEvents(context.Context, *types.ReapplyEventsRequest) error
	RefreshWorkflowTasks(context.Context, *types.RefreshWorkflowTasksRequest) error
	RemoveTask(context.Context, *types.RemoveTaskRequest) error
	ResendReplicationTasks(context.Context, *types.ResendReplicationTasksRequest) error
	ResetQueue(context.Context, *types.ResetQueueRequest) error
	GetCrossClusterTasks(context.Context, *types.GetCrossClusterTasksRequest) (*types.GetCrossClusterTasksResponse, error)
	RespondCrossClusterTasksCompleted(context.Context, *types.RespondCrossClusterTasksCompletedRequest) (*types.RespondCrossClusterTasksCompletedResponse, error)
	GetDynamicConfig(context.Context, *types.GetDynamicConfigRequest) (*types.GetDynamicConfigResponse, error)
	UpdateDynamicConfig(context.Context, *types.UpdateDynamicConfigRequest) error
	RestoreDynamicConfig(context.Context, *types.RestoreDynamicConfigRequest) error
	ListDynamicConfig(context.Context, *types.ListDynamicConfigRequest) (*types.ListDynamicConfigResponse, error)
	GetOperationalDynamicConfig(context.Context, *types.GetOperationalDynamicConfigRequest) (*types.GetOperationalDynamicConfigResponse, error)
	UpdateOperationalDynamicConfig(context.Context, *types.UpdateOperationalDynamicConfigRequest) error
	RestoreOperationalDynamicConfig(context.Context, *types.RestoreOperationalDynamicConfigRequest) error
	ListOperationalDynamicConfig(context.Context, *types.ListOperationalDynamicConfigRequest) (*types.ListOperationalDynamicConfigResponse, error)
	DeleteWorkflow(context.Context, *types.AdminDeleteWorkflowRequest) (*types.AdminDeleteWorkflowResponse, error)
	MaintainCorruptWorkflow(context.Context, *types.AdminMaintainWorkflowRequest) (*types.AdminMaintainWorkflowResponse, error)
	GetGlobalIsolationGroups(ctx context.Context, request *types.GetGlobalIsolationGroupsRequest) (*types.GetGlobalIsolationGroupsResponse, error)
	UpdateGlobalIsolationGroups(ctx context.Context, request *types.UpdateGlobalIsolationGroupsRequest) (*types.UpdateGlobalIsolationGroupsResponse, error)
	GetDomainIsolationGroups(ctx context.Context, request *types.GetDomainIsolationGroupsRequest) (*types.GetDomainIsolationGroupsResponse, error)
	UpdateDomainIsolationGroups(ctx context.Context, request *types.UpdateDomainIsolationGroupsRequest) (*types.UpdateDomainIsolationGroupsResponse, error)
	GetDomainAsyncWorkflowConfiguraton(context.Context, *types.GetDomainAsyncWorkflowConfiguratonRequest) (*types.GetDomainAsyncWorkflowConfiguratonResponse, error)
	UpdateDomainAsyncWorkflowConfiguraton(context.Context, *types.UpdateDomainAsyncWorkflowConfiguratonRequest) (*types.UpdateDomainAsyncWorkflowConfiguratonResponse, error)
	UpdateTaskListPartitionConfig(context.Context, *types.UpdateTaskListPartitionConfigRequest) (*types.UpdateTaskListPartitionConfigResponse, error)
}
