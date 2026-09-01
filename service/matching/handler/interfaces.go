//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go -package handler github.com/uber/cadence/service/matching/handler Handler
//go:generate gowrap gen -g -p . -i Handler -t ../../templates/grpc.tmpl -o ../wrappers/grpc/grpc_handler_generated.go -v handler=GRPC -v package=matchingv1 -v path=github.com/uber/cadence/.gen/proto/matching/v1 -v prefix=Matching
//go:generate gowrap gen -g -p ../../../.gen/go/matching/matchingserviceserver -i Interface -t ../../templates/thrift.tmpl -o ../wrappers/thrift/thrift_handler_generated.go -v handler=Thrift -v prefix=Matching

package handler

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		common.Daemon

		AddDecisionTask(hCtx *handlerContext, request *types.AddDecisionTaskRequest) (*types.AddDecisionTaskResponse, error)
		AddActivityTask(hCtx *handlerContext, request *types.AddActivityTaskRequest) (*types.AddActivityTaskResponse, error)
		PollForDecisionTask(hCtx *handlerContext, request *types.MatchingPollForDecisionTaskRequest) (*types.MatchingPollForDecisionTaskResponse, error)
		PollForActivityTask(hCtx *handlerContext, request *types.MatchingPollForActivityTaskRequest) (*types.MatchingPollForActivityTaskResponse, error)
		QueryWorkflow(hCtx *handlerContext, request *types.MatchingQueryWorkflowRequest) (*types.MatchingQueryWorkflowResponse, error)
		RespondQueryTaskCompleted(hCtx *handlerContext, request *types.MatchingRespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(hCtx *handlerContext, request *types.CancelOutstandingPollRequest) error
		DescribeTaskList(hCtx *handlerContext, request *types.MatchingDescribeTaskListRequest) (*types.DescribeTaskListResponse, error)
		ListTaskListPartitions(hCtx *handlerContext, request *types.MatchingListTaskListPartitionsRequest) (*types.ListTaskListPartitionsResponse, error)
		GetTaskListsByDomain(hCtx *handlerContext, request *types.GetTaskListsByDomainRequest) (*types.GetTaskListsByDomainResponse, error)
		UpdateTaskListPartitionConfig(hCtx *handlerContext, request *types.MatchingUpdateTaskListPartitionConfigRequest) (*types.MatchingUpdateTaskListPartitionConfigResponse, error)
		RefreshTaskListPartitionConfig(hCtx *handlerContext, request *types.MatchingRefreshTaskListPartitionConfigRequest) (*types.MatchingRefreshTaskListPartitionConfigResponse, error)
	}

	// Handler interface for matching service
	Handler interface {
		common.Daemon

		Health(context.Context) (*types.HealthStatus, error)
		AddActivityTask(context.Context, *types.AddActivityTaskRequest) (*types.AddActivityTaskResponse, error)
		AddDecisionTask(context.Context, *types.AddDecisionTaskRequest) (*types.AddDecisionTaskResponse, error)
		CancelOutstandingPoll(context.Context, *types.CancelOutstandingPollRequest) error
		DescribeTaskList(context.Context, *types.MatchingDescribeTaskListRequest) (*types.DescribeTaskListResponse, error)
		ListTaskListPartitions(context.Context, *types.MatchingListTaskListPartitionsRequest) (*types.ListTaskListPartitionsResponse, error)
		GetTaskListsByDomain(context.Context, *types.GetTaskListsByDomainRequest) (*types.GetTaskListsByDomainResponse, error)
		PollForActivityTask(context.Context, *types.MatchingPollForActivityTaskRequest) (*types.MatchingPollForActivityTaskResponse, error)
		PollForDecisionTask(context.Context, *types.MatchingPollForDecisionTaskRequest) (*types.MatchingPollForDecisionTaskResponse, error)
		QueryWorkflow(context.Context, *types.MatchingQueryWorkflowRequest) (*types.MatchingQueryWorkflowResponse, error)
		RespondQueryTaskCompleted(context.Context, *types.MatchingRespondQueryTaskCompletedRequest) error
		UpdateTaskListPartitionConfig(context.Context, *types.MatchingUpdateTaskListPartitionConfigRequest) (*types.MatchingUpdateTaskListPartitionConfigResponse, error)
		RefreshTaskListPartitionConfig(context.Context, *types.MatchingRefreshTaskListPartitionConfigRequest) (*types.MatchingRefreshTaskListPartitionConfigResponse, error)
	}
)
