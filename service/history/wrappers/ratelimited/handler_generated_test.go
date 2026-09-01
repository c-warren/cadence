package ratelimited

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/handler"
)

const (
	testDomainID   = "test-domain-id"
	testWorkflowID = "test-workflow-id"
	testDomainName = "test-domain-name"
)

func TestRatelimitedEndpoints_Table(t *testing.T) {
	controller := gomock.NewController(t)

	handlerMock := handler.NewMockHandler(controller)

	wrapper := NewHistoryHandler(handlerMock, nil, log.NewNoop())

	// We define the calls that should be ratelimited
	limitedCalls := []struct {
		name string
		// Defines how to call the wrapper function (correct request type, and call)
		callWrapper func() (interface{}, error)
		// Defines the expected call to the wrapped handler (what to call if the call is not ratelimited)
		expectCallToEndpoint func()
	}{
		{
			name: "StartWorkflowExecution",
			callWrapper: func() (interface{}, error) {
				startRequest := &types.HistoryStartWorkflowExecutionRequest{
					DomainUUID:   testDomainID,
					StartRequest: &types.StartWorkflowExecutionRequest{WorkflowID: testWorkflowID},
				}
				return wrapper.StartWorkflowExecution(context.Background(), startRequest)
			},
			expectCallToEndpoint: func() {
				handlerMock.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
		},
		{
			name: "SignalWithStartWorkflowExecution",
			callWrapper: func() (interface{}, error) {
				signalWithStartRequest := &types.HistorySignalWithStartWorkflowExecutionRequest{
					DomainUUID:             testDomainID,
					SignalWithStartRequest: &types.SignalWithStartWorkflowExecutionRequest{WorkflowID: testWorkflowID},
				}

				return wrapper.SignalWithStartWorkflowExecution(context.Background(), signalWithStartRequest)
			},
			expectCallToEndpoint: func() {
				handlerMock.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
		},
		{
			name: "SignalWorkflowExecution",
			callWrapper: func() (interface{}, error) {
				signalRequest := &types.HistorySignalWorkflowExecutionRequest{
					DomainUUID: testDomainID,
					SignalRequest: &types.SignalWorkflowExecutionRequest{
						WorkflowExecution: &types.WorkflowExecution{WorkflowID: testWorkflowID},
					},
				}

				return nil, wrapper.SignalWorkflowExecution(context.Background(), signalRequest)
			},
			expectCallToEndpoint: func() {
				handlerMock.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
		},
		{
			name: "DescribeWorkflowExecution",
			callWrapper: func() (interface{}, error) {
				describeRequest := &types.HistoryDescribeWorkflowExecutionRequest{
					DomainUUID: testDomainID,
					Request: &types.DescribeWorkflowExecutionRequest{
						Execution: &types.WorkflowExecution{WorkflowID: testWorkflowID},
					},
				}

				return wrapper.DescribeWorkflowExecution(context.Background(), describeRequest)
			},
			expectCallToEndpoint: func() {
				handlerMock.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
		},
	}

	for _, endpoint := range limitedCalls {
		t.Run(fmt.Sprintf("%s, %s", endpoint.name, "not limited"), func(t *testing.T) {
			wrapper.(*historyHandler).allowFunc = func(string, string) bool { return true }
			endpoint.expectCallToEndpoint()
			_, err := endpoint.callWrapper()
			assert.NoError(t, err)
		})

		t.Run(fmt.Sprintf("%s, %s", endpoint.name, "limited"), func(t *testing.T) {
			wrapper.(*historyHandler).allowFunc = func(string, string) bool { return false }
			_, err := endpoint.callWrapper()
			var sbErr *types.ServiceBusyError
			assert.ErrorAs(t, err, &sbErr)
			assert.ErrorContains(t, err, "Too many requests for the workflow ID")
			assert.Equal(t, constants.WorkflowIDRateLimitReason, sbErr.Reason)
		})
	}
}
