package sql

import (
	"net"
	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/tools/sql"
)

type (
	// HandlerTestSuite defines a test suite
	HandlerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		pluginName string
	}
)

// NewHandlerTestSuite returns a test suite
func NewHandlerTestSuite(pluginName string) *HandlerTestSuite {
	return &HandlerTestSuite{
		pluginName: pluginName,
	}
}

// SetupTest setups test
func (s *HandlerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// TestValidateConnectConfig test
func (s *HandlerTestSuite) TestValidateConnectConfig() {
	cfg := new(config.SQL)

	s.NotNil(sql.ValidateConnectConfig(cfg))
	port, err := environment.GetMySQLPort()
	s.NoError(err)
	cfg.ConnectAddr = net.JoinHostPort(
		environment.GetMySQLAddress(),
		strconv.Itoa(port),
	)
	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.DatabaseName = "foobar"
	s.Nil(sql.ValidateConnectConfig(cfg))

	cfg.TLS = &config.TLS{}
	cfg.TLS.Enabled = true
	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.TLS.CaFile = "ca.pem"
	s.Nil(sql.ValidateConnectConfig(cfg))

	cfg.TLS.KeyFile = "key_file"
	cfg.TLS.CertFile = ""
	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.TLS.KeyFile = ""
	cfg.TLS.CertFile = "cert_file"
	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.TLS.KeyFile = "key_file"
	cfg.TLS.CertFile = "cert_file"
	s.Nil(sql.ValidateConnectConfig(cfg))
}
