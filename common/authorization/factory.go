package authorization

import (
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
)

func NewAuthorizer(authorization config.Authorization, logger log.Logger, domainCache cache.DomainCache) (Authorizer, error) {
	switch true {
	case authorization.OAuthAuthorizer.Enable:
		return NewOAuthAuthorizer(authorization.OAuthAuthorizer, logger, domainCache)
	default:
		return NewNopAuthorizer()
	}
}
