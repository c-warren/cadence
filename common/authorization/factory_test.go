package authorization

import (
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
)

type (
	factorySuite struct {
		suite.Suite
		logger log.Logger
	}
)

func TestFactorySuite(t *testing.T) {
	suite.Run(t, new(factorySuite))
}

func (s *factorySuite) SetupTest() {
	s.logger = testlogger.New(s.Suite.T())
}

func cfgNoop() config.Authorization {
	return config.Authorization{
		OAuthAuthorizer: config.OAuthAuthorizer{
			Enable: false,
		},
		NoopAuthorizer: config.NoopAuthorizer{
			Enable: true,
		},
	}
}

func cfgOAuth() config.Authorization {
	return config.Authorization{
		OAuthAuthorizer: config.OAuthAuthorizer{
			Enable: true,
			JwtCredentials: &config.JwtCredentials{
				Algorithm: jwt.SigningMethodRS256.Name,
				PublicKey: "../../config/credentials/keytest.pub",
			},
			MaxJwtTTL: 12345,
		},
	}
}

func (s *factorySuite) TestFactoryNoopAuthorizer() {
	cfgOAuthVar := cfgOAuth()

	publicKey, _ := common.LoadRSAPublicKey(cfgOAuthVar.OAuthAuthorizer.JwtCredentials.PublicKey)

	var tests = []struct {
		cfg      config.Authorization
		expected Authorizer
		err      error
	}{
		{cfgNoop(), &nopAuthority{}, nil},
		{cfgOAuthVar, &oauthAuthority{
			config:    cfgOAuthVar.OAuthAuthorizer,
			log:       s.logger,
			publicKey: publicKey,
			parser:    jwt.NewParser(jwt.WithValidMethods([]string{cfgOAuthVar.OAuthAuthorizer.JwtCredentials.Algorithm}), jwt.WithIssuedAt()),
		}, nil},
	}

	for _, test := range tests {
		authorizer, err := NewAuthorizer(test.cfg, s.logger, nil)
		s.Equal(authorizer, test.expected)
		s.Equal(err, test.err)
	}
}
