package grpc

import (
	"context"

	adminv1 "github.com/uber/cadence-idl/go/proto/admin/v1"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/common/types/mapper/proto"
)

func (g AdminHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(adminv1.BuildAdminAPIYARPCProcedures(g))
}

func (g APIHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(apiv1.BuildDomainAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildWorkflowAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildWorkerAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildVisibilityAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildMetaAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildScheduleAPIYARPCProcedures(g))
}

func (g APIHandler) Health(ctx context.Context, request *apiv1.HealthRequest) (*apiv1.HealthResponse, error) {
	response, err := g.h.Health(ctx)
	return proto.FromHealthResponse(response), proto.FromError(err)
}
