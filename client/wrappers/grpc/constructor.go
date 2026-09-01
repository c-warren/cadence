package grpc

import (
	adminv1 "github.com/uber/cadence-idl/go/proto/admin/v1"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"

	historyv1 "github.com/uber/cadence/.gen/proto/history/v1"
	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
)

type (
	adminClient struct {
		c adminv1.AdminAPIYARPCClient
	}

	frontendGRPCClientWrapper struct {
		apiv1.DomainAPIYARPCClient
		apiv1.WorkflowAPIYARPCClient
		apiv1.WorkerAPIYARPCClient
		apiv1.VisibilityAPIYARPCClient
		apiv1.ScheduleAPIYARPCClient
	}
	frontendClient struct {
		c *frontendGRPCClientWrapper
	}
	historyClient struct {
		c historyv1.HistoryAPIYARPCClient
	}
	matchingClient struct {
		c matchingv1.MatchingAPIYARPCClient
	}
)

func NewAdminClient(c adminv1.AdminAPIYARPCClient) admin.Client {
	return adminClient{c}
}

func NewFrontendClient(
	domain apiv1.DomainAPIYARPCClient,
	workflow apiv1.WorkflowAPIYARPCClient,
	worker apiv1.WorkerAPIYARPCClient,
	visibility apiv1.VisibilityAPIYARPCClient,
	schedule apiv1.ScheduleAPIYARPCClient,
) frontend.Client {
	return frontendClient{&frontendGRPCClientWrapper{domain, workflow, worker, visibility, schedule}}
}

func NewHistoryClient(c historyv1.HistoryAPIYARPCClient) history.Client {
	return historyClient{c}
}

func NewMatchingClient(c matchingv1.MatchingAPIYARPCClient) matching.Client {
	return matchingClient{c}
}
