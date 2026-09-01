package rpc

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

func TestNewParams(t *testing.T) {
	serviceName := service.Frontend
	dc := dynamicconfig.NewNopCollection()
	makeConfig := func(svc config.Service) *config.Config {
		return &config.Config{
			PublicClient:           config.PublicClient{HostPort: "localhost:9999"},
			ShardDistributorClient: config.ShardDistributorClient{HostPort: "localhost:9998"},
			Services:               map[string]config.Service{"frontend": svc}}
	}
	logger := testlogger.New(t)
	metricsCl := metrics.NewNoopMetricsClient()

	_, err := NewParams(serviceName, &config.Config{}, dc, logger, metricsCl)
	assert.EqualError(t, err, "no config section for service: frontend")

	_, err = NewParams(serviceName, makeConfig(config.Service{RPC: config.RPC{BindOnLocalHost: true, BindOnIP: "1.2.3.4"}}), dc, logger, metricsCl)
	assert.EqualError(t, err, "get listen IP: bindOnLocalHost and bindOnIP are mutually exclusive")

	_, err = NewParams(serviceName, makeConfig(config.Service{RPC: config.RPC{BindOnIP: "invalidIP"}}), dc, logger, metricsCl)
	assert.EqualError(t, err, "get listen IP: unable to parse bindOnIP value or it is not an IPv4 or IPv6 address: invalidIP")

	_, err = NewParams(serviceName, &config.Config{Services: map[string]config.Service{"frontend": {}}}, dc, logger, metricsCl)
	assert.EqualError(t, err, "public client outbound: need to provide an endpoint config for PublicClient")

	cfg := makeConfig(config.Service{RPC: config.RPC{BindOnLocalHost: true, TLS: config.TLS{Enabled: true, CertFile: "invalid", KeyFile: "invalid"}}})
	_, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.EqualError(t, err, "inbound TLS config: open invalid: no such file or directory")

	cfg = &config.Config{Services: map[string]config.Service{
		"frontend": {RPC: config.RPC{BindOnLocalHost: true}},
		"history":  {RPC: config.RPC{TLS: config.TLS{Enabled: true, CaFile: "invalid"}}},
	}}
	_, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.EqualError(t, err, "outbound cadence-history TLS config: open invalid: no such file or directory")

	cfg = makeConfig(config.Service{RPC: config.RPC{BindOnLocalHost: true, Port: 1111, GRPCPort: 2222, GRPCMaxMsgSize: 3333}})
	params, err := NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1111", params.TChannelAddress)
	assert.Equal(t, "127.0.0.1:2222", params.GRPCAddress)
	assert.Equal(t, 3333, params.GRPCMaxMsgSize)
	assert.Nil(t, params.InboundTLS)

	cfg = makeConfig(config.Service{RPC: config.RPC{BindOnLocalHost: true, HTTP: &config.HTTP{Port: 8800}}})
	params, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8800", params.HTTP.Address)

	cfg = makeConfig(config.Service{RPC: config.RPC{BindOnLocalHost: true, HTTP: &config.HTTP{}}})
	params, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.Error(t, err)

	cfg = makeConfig(config.Service{RPC: config.RPC{BindOnIP: "1.2.3.4", GRPCPort: 2222}})
	params, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.NoError(t, err)
	assert.Equal(t, "1.2.3.4:2222", params.GRPCAddress)

	cfg = makeConfig(config.Service{RPC: config.RPC{GRPCPort: 2222, TLS: config.TLS{Enabled: true}}})
	params, err = NewParams(serviceName, cfg, dc, logger, metricsCl)
	assert.NoError(t, err)
	ip, port, err := net.SplitHostPort(params.GRPCAddress)
	assert.NoError(t, err)
	assert.Equal(t, "2222", port)
	assert.NotNil(t, net.ParseIP(ip))
	assert.NotNil(t, params.InboundTLS)
}
