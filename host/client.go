package host

import (
	adminv1 "github.com/uber/cadence-idl/go/proto/admin/v1"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/yarpc"

	historyv1 "github.com/uber/cadence/.gen/proto/history/v1"
	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/client/wrappers/grpc"
	"github.com/uber/cadence/common/service"
)

// AdminClient is the interface exposed by admin service client
type AdminClient interface {
	admin.Client
}

// FrontendClient is the interface exposed by frontend service client
type FrontendClient interface {
	frontend.Client
}

// HistoryClient is the interface exposed by history service client
type HistoryClient interface {
	history.Client
}

type MatchingClient interface {
	matching.Client
}

// NewAdminClient creates a client to cadence admin client
func NewAdminClient(d *yarpc.Dispatcher) AdminClient {
	return grpc.NewAdminClient(adminv1.NewAdminAPIYARPCClient(d.ClientConfig(testOutboundName(service.Frontend))))
}

// NewFrontendClient creates a client to cadence frontend client
func NewFrontendClient(d *yarpc.Dispatcher) FrontendClient {
	config := d.ClientConfig(testOutboundName(service.Frontend))
	return grpc.NewFrontendClient(
		apiv1.NewDomainAPIYARPCClient(config),
		apiv1.NewWorkflowAPIYARPCClient(config),
		apiv1.NewWorkerAPIYARPCClient(config),
		apiv1.NewVisibilityAPIYARPCClient(config),
		apiv1.NewScheduleAPIYARPCClient(config),
	)
}

// NewHistoryClient creates a client to cadence history service client
func NewHistoryClient(d *yarpc.Dispatcher) HistoryClient {
	return grpc.NewHistoryClient(historyv1.NewHistoryAPIYARPCClient(d.ClientConfig(testOutboundName(service.History))))
}
