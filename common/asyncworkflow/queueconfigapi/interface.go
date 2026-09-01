package queueconfigapi

import (
	"context"

	"github.com/uber/cadence/common/types"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination handler_mock.go -self_package github.com/uber/cadence/common/asyncworkflow/queueconfigapi

type Handler interface {
	GetConfiguraton(context.Context, *types.GetDomainAsyncWorkflowConfiguratonRequest) (*types.GetDomainAsyncWorkflowConfiguratonResponse, error)
	UpdateConfiguration(context.Context, *types.UpdateDomainAsyncWorkflowConfiguratonRequest) (*types.UpdateDomainAsyncWorkflowConfiguratonResponse, error)
}
