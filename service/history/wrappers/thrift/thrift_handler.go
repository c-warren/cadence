package thrift

import (
	"context"

	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	"github.com/uber/cadence/.gen/go/history/historyserviceserver"
	"github.com/uber/cadence/common/types/mapper/thrift"
	"github.com/uber/cadence/service/history/handler"
)

type ThriftHandler struct {
	h handler.Handler
}

func NewThriftHandler(h handler.Handler) ThriftHandler {
	return ThriftHandler{h}
}

func (t ThriftHandler) Register(dispatcher *yarpc.Dispatcher) {
	dispatcher.Register(historyserviceserver.New(&t))
	dispatcher.Register(metaserver.New(&t))
}

func (t ThriftHandler) Health(ctx context.Context) (*health.HealthStatus, error) {
	response, err := t.h.Health(ctx)
	return thrift.FromHealthStatus(response), thrift.FromError(err)
}
