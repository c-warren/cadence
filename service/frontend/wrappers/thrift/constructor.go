package thrift

import (
	"context"

	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/admin/adminserviceserver"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	"github.com/uber/cadence/common/types/mapper/thrift"
	"github.com/uber/cadence/service/frontend/admin"
	"github.com/uber/cadence/service/frontend/api"
)

type (
	AdminHandler struct {
		h admin.Handler
	}
	APIHandler struct {
		h api.Handler
	}
)

func NewAdminHandler(h admin.Handler) AdminHandler {
	return AdminHandler{h}
}

func NewAPIHandler(h api.Handler) APIHandler {
	return APIHandler{h}
}

func (t AdminHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(adminserviceserver.New(t))
}

func (t APIHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(workflowserviceserver.New(t))
	dispatcher.Register(metaserver.New(t))
}

func (t APIHandler) Health(ctx context.Context) (*health.HealthStatus, error) {
	response, err := t.h.Health(ctx)
	return thrift.FromHealthStatus(response), thrift.FromError(err)
}
