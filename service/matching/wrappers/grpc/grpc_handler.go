package grpc

import (
	"context"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/yarpc"

	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/common/types/mapper/proto"
)

func (g GRPCHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(matchingv1.BuildMatchingAPIYARPCProcedures(g))
	dispatcher.Register(apiv1.BuildMetaAPIYARPCProcedures(g))
}

func (g GRPCHandler) Health(ctx context.Context, _ *apiv1.HealthRequest) (*apiv1.HealthResponse, error) {
	response, err := g.h.Health(ctx)
	return proto.FromHealthResponse(response), proto.FromError(err)
}
