package thrift

import (
	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/history/historyserviceclient"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
)

type (
	adminClient struct {
		c adminserviceclient.Interface
	}
	frontendClient struct {
		c workflowserviceclient.Interface
	}
	historyClient struct {
		c historyserviceclient.Interface
	}
	matchingClient struct {
		c matchingserviceclient.Interface
	}
)

func NewAdminClient(c adminserviceclient.Interface) admin.Client {
	return adminClient{c}
}

func NewFrontendClient(c workflowserviceclient.Interface) frontend.Client {
	return frontendClient{c}
}

func NewHistoryClient(c historyserviceclient.Interface) history.Client {
	return historyClient{c}
}

func NewMatchingClient(c matchingserviceclient.Interface) matching.Client {
	return matchingClient{c}
}
