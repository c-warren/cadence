package retryable

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

func TestFrontendClientRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	clientMock := frontend.NewMockClient(ctrl)
	// One failure, one success
	clientMock.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, &types.ServiceBusyError{
			Message: "error",
		}).Times(1)
	clientMock.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).Times(1)

	retryableClient := NewFrontendClient(
		clientMock,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsServiceBusyError)

	_, err := retryableClient.CountWorkflowExecutions(context.Background(), &types.CountWorkflowExecutionsRequest{})
	assert.NoError(t, err)
}

func TestFrontendClientNonRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	clientMock := frontend.NewMockClient(ctrl)
	// One failure, one success
	clientMock.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, &types.BadRequestError{
			Message: "error",
		}).Times(1)

	retryableClient := NewFrontendClient(
		clientMock,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsServiceBusyError)

	_, err := retryableClient.CountWorkflowExecutions(context.Background(), &types.CountWorkflowExecutionsRequest{})
	assert.Error(t, err)
}
