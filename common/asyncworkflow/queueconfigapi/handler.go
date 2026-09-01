package queueconfigapi

import (
	"context"

	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
)

type handlerImpl struct {
	logger        log.Logger
	domainHandler domain.Handler
}

func New(logger log.Logger, dh domain.Handler) Handler {
	return &handlerImpl{
		logger:        logger,
		domainHandler: dh,
	}
}

func (h *handlerImpl) GetConfiguraton(ctx context.Context, req *types.GetDomainAsyncWorkflowConfiguratonRequest) (*types.GetDomainAsyncWorkflowConfiguratonResponse, error) {
	resp, err := h.domainHandler.DescribeDomain(ctx, &types.DescribeDomainRequest{
		Name: &req.Domain,
	})
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Configuration == nil || resp.Configuration.AsyncWorkflowConfig == nil {
		return &types.GetDomainAsyncWorkflowConfiguratonResponse{}, nil
	}

	return &types.GetDomainAsyncWorkflowConfiguratonResponse{
		Configuration: resp.Configuration.AsyncWorkflowConfig,
	}, nil
}

func (h *handlerImpl) UpdateConfiguration(ctx context.Context, req *types.UpdateDomainAsyncWorkflowConfiguratonRequest) (*types.UpdateDomainAsyncWorkflowConfiguratonResponse, error) {
	if req == nil {
		return nil, &types.BadRequestError{Message: "Request is nil."}
	}

	err := h.domainHandler.UpdateAsyncWorkflowConfiguraton(ctx, *req)
	if err != nil {
		return nil, err
	}

	return &types.UpdateDomainAsyncWorkflowConfiguratonResponse{}, nil
}
