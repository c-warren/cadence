package isolationgroupapi

import (
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
)

type handlerImpl struct {
	log                        log.Logger
	globalIsolationGroupDrains dynamicconfig.Client
	domainHandler              domain.Handler
}

func New(log log.Logger, igConfigStore dynamicconfig.Client, dh domain.Handler) Handler {
	return &handlerImpl{
		log:                        log,
		globalIsolationGroupDrains: igConfigStore,
		domainHandler:              dh,
	}
}
