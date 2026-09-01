package pinotutils

import (
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	pnt "github.com/uber/cadence/common/pinot"
)

func CreatePinotClient(s *suite.Suite, pinotConfig *config.PinotVisibilityConfig, logger log.Logger) pnt.GenericClient {
	pinotRawClient, err := pinot.NewFromBrokerList([]string{pinotConfig.Broker})
	s.Require().NoError(err)
	pinotClient := pnt.NewPinotClient(pinotRawClient, logger, pinotConfig)
	return pinotClient
}
