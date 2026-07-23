// Copyright (c) 2026 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
	frontendcfg "github.com/uber/cadence/service/frontend/config"
)

func newTestAdminHandlerForDLQ(t *testing.T) (*adminHandlerImpl, *history.MockClient) {
	ctrl := gomock.NewController(t)
	mockResource := resource.NewTest(t, ctrl, metrics.Frontend)
	params := &resource.Params{
		Logger:          testlogger.New(t),
		ThrottledLogger: testlogger.New(t),
		MetricScope:     tally.NewTestScope(service.Frontend, make(map[string]string)),
		MetricsClient:   metrics.NewNoopMetricsClient(),
	}
	cfg := &frontendcfg.Config{
		EnableAdminProtection:  dynamicproperties.GetBoolPropertyFn(false),
		EnableGracefulFailover: dynamicproperties.GetBoolPropertyFn(false),
	}
	dh := domain.NewMockHandler(ctrl)
	handler := NewHandler(mockResource, params, cfg, dh).(*adminHandlerImpl)
	return handler, mockResource.HistoryClient
}

func TestReadAsyncWorkflowMessagesFromDLQ(t *testing.T) {
	historyErr := errors.New("history failure")
	tests := []struct {
		name      string
		request   *types.ReadAsyncWorkflowMessagesFromDLQRequest
		mockSetup func(*history.MockClient)
		wantResp  *types.ReadAsyncWorkflowMessagesFromDLQResponse
		wantErr   bool
	}{
		{
			name:    "nil request",
			request: nil,
			wantErr: true,
		},
		{
			name:    "empty queue name",
			request: &types.ReadAsyncWorkflowMessagesFromDLQRequest{ShardID: 1},
			wantErr: true,
		},
		{
			name:    "success with page size default",
			request: &types.ReadAsyncWorkflowMessagesFromDLQRequest{QueueName: "q", ShardID: 1},
			mockSetup: func(m *history.MockClient) {
				m.EXPECT().ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *types.ReadAsyncWorkflowMessagesFromDLQRequest, _ ...interface{}) (*types.ReadAsyncWorkflowMessagesFromDLQResponse, error) {
						assert.Equal(t, int32(constants.ReadDLQMessagesPageSize), req.PageSize)
						return &types.ReadAsyncWorkflowMessagesFromDLQResponse{LastMessageID: 7}, nil
					})
			},
			wantResp: &types.ReadAsyncWorkflowMessagesFromDLQResponse{LastMessageID: 7},
		},
		{
			name:    "history error",
			request: &types.ReadAsyncWorkflowMessagesFromDLQRequest{QueueName: "q", ShardID: 1, PageSize: 10},
			mockSetup: func(m *history.MockClient) {
				m.EXPECT().ReadAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).Return(nil, historyErr)
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler, mockHistory := newTestAdminHandlerForDLQ(t)
			if tc.mockSetup != nil {
				tc.mockSetup(mockHistory)
			}
			resp, err := handler.ReadAsyncWorkflowMessagesFromDLQ(context.Background(), tc.request)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantResp, resp)
		})
	}
}

func TestMergeAsyncWorkflowMessagesFromDLQ(t *testing.T) {
	tests := []struct {
		name      string
		request   *types.MergeAsyncWorkflowMessagesFromDLQRequest
		mockSetup func(*history.MockClient)
		wantResp  *types.MergeAsyncWorkflowMessagesFromDLQResponse
		wantErr   bool
	}{
		{
			name:    "nil request",
			request: nil,
			wantErr: true,
		},
		{
			name:    "empty queue name",
			request: &types.MergeAsyncWorkflowMessagesFromDLQRequest{ShardID: 1},
			wantErr: true,
		},
		{
			name:    "success with defaults applied",
			request: &types.MergeAsyncWorkflowMessagesFromDLQRequest{QueueName: "q", ShardID: 1},
			mockSetup: func(m *history.MockClient) {
				m.EXPECT().MergeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *types.MergeAsyncWorkflowMessagesFromDLQRequest, _ ...interface{}) (*types.MergeAsyncWorkflowMessagesFromDLQResponse, error) {
						assert.Equal(t, constants.InclusiveEndMessageID, req.InclusiveEndMessageID)
						assert.Equal(t, int32(constants.ReadDLQMessagesPageSize), req.PageSize)
						return &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 3, LastMessageID: 9}, nil
					})
			},
			wantResp: &types.MergeAsyncWorkflowMessagesFromDLQResponse{MessagesCount: 3, LastMessageID: 9},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler, mockHistory := newTestAdminHandlerForDLQ(t)
			if tc.mockSetup != nil {
				tc.mockSetup(mockHistory)
			}
			resp, err := handler.MergeAsyncWorkflowMessagesFromDLQ(context.Background(), tc.request)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantResp, resp)
		})
	}
}

func TestPurgeAsyncWorkflowMessagesFromDLQ(t *testing.T) {
	tests := []struct {
		name      string
		request   *types.PurgeAsyncWorkflowMessagesFromDLQRequest
		mockSetup func(*history.MockClient)
		wantErr   bool
	}{
		{
			name:    "nil request",
			request: nil,
			wantErr: true,
		},
		{
			name:    "empty queue name",
			request: &types.PurgeAsyncWorkflowMessagesFromDLQRequest{ShardID: 1},
			wantErr: true,
		},
		{
			name:    "success with default end message id",
			request: &types.PurgeAsyncWorkflowMessagesFromDLQRequest{QueueName: "q", ShardID: 1},
			mockSetup: func(m *history.MockClient) {
				m.EXPECT().PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *types.PurgeAsyncWorkflowMessagesFromDLQRequest, _ ...interface{}) (*types.PurgeAsyncWorkflowMessagesFromDLQResponse, error) {
						assert.Equal(t, constants.InclusiveEndMessageID, req.InclusiveEndMessageID)
						return &types.PurgeAsyncWorkflowMessagesFromDLQResponse{}, nil
					})
			},
		},
		{
			name:    "history error",
			request: &types.PurgeAsyncWorkflowMessagesFromDLQRequest{QueueName: "q", ShardID: 1, InclusiveEndMessageID: 5},
			mockSetup: func(m *history.MockClient) {
				m.EXPECT().PurgeAsyncWorkflowMessagesFromDLQ(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom"))
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler, mockHistory := newTestAdminHandlerForDLQ(t)
			if tc.mockSetup != nil {
				tc.mockSetup(mockHistory)
			}
			_, err := handler.PurgeAsyncWorkflowMessagesFromDLQ(context.Background(), tc.request)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
