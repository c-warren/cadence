package thrift

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	adminHandler "github.com/uber/cadence/service/frontend/admin"
)

func TestAdminThriftHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	h := adminHandler.NewMockHandler(ctrl)
	th := NewAdminHandler(h)
	ctx := context.Background()
	internalErr := &types.InternalServiceError{Message: "test"}
	expectedErr := &shared.InternalServiceError{Message: "test"}

	t.Run("AddSearchAttribute", func(t *testing.T) {
		h.EXPECT().AddSearchAttribute(ctx, &types.AddSearchAttributeRequest{}).Return(internalErr).Times(1)
		err := th.AddSearchAttribute(ctx, &admin.AddSearchAttributeRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("CloseShard", func(t *testing.T) {
		h.EXPECT().CloseShard(ctx, &types.CloseShardRequest{}).Return(internalErr).Times(1)
		err := th.CloseShard(ctx, &shared.CloseShardRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("DescribeCluster", func(t *testing.T) {
		h.EXPECT().DescribeCluster(ctx).Return(&types.DescribeClusterResponse{}, internalErr).Times(1)
		resp, err := th.DescribeCluster(ctx)
		assert.Equal(t, admin.DescribeClusterResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("DescribeHistoryHost", func(t *testing.T) {
		h.EXPECT().DescribeHistoryHost(ctx, &types.DescribeHistoryHostRequest{}).Return(&types.DescribeHistoryHostResponse{}, internalErr).Times(1)
		resp, err := th.DescribeHistoryHost(ctx, &shared.DescribeHistoryHostRequest{})
		assert.Equal(t, shared.DescribeHistoryHostResponse{NumberOfShards: common.Int32Ptr(0), ShardControllerStatus: common.StringPtr(""), Address: common.StringPtr("")}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("DescribeQueue", func(t *testing.T) {
		h.EXPECT().DescribeQueue(ctx, &types.DescribeQueueRequest{}).Return(&types.DescribeQueueResponse{}, internalErr).Times(1)
		resp, err := th.DescribeQueue(ctx, &shared.DescribeQueueRequest{})
		assert.Equal(t, shared.DescribeQueueResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("DescribeWorkflowExecution", func(t *testing.T) {
		h.EXPECT().DescribeWorkflowExecution(ctx, &types.AdminDescribeWorkflowExecutionRequest{}).Return(nil, internalErr).Times(1)
		resp, err := th.DescribeWorkflowExecution(ctx, &admin.DescribeWorkflowExecutionRequest{})
		assert.Nil(t, resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("GetDLQReplicationMessages", func(t *testing.T) {
		h.EXPECT().GetDLQReplicationMessages(ctx, &types.GetDLQReplicationMessagesRequest{}).Return(&types.GetDLQReplicationMessagesResponse{}, internalErr).Times(1)
		resp, err := th.GetDLQReplicationMessages(ctx, &replicator.GetDLQReplicationMessagesRequest{})
		assert.Equal(t, replicator.GetDLQReplicationMessagesResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("GetDomainReplicationMessages", func(t *testing.T) {
		h.EXPECT().GetDomainReplicationMessages(ctx, &types.GetDomainReplicationMessagesRequest{}).Return(&types.GetDomainReplicationMessagesResponse{}, internalErr).Times(1)
		resp, err := th.GetDomainReplicationMessages(ctx, &replicator.GetDomainReplicationMessagesRequest{})
		assert.Equal(t, replicator.GetDomainReplicationMessagesResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("GetReplicationMessages", func(t *testing.T) {
		h.EXPECT().GetReplicationMessages(ctx, &types.GetReplicationMessagesRequest{}).Return(&types.GetReplicationMessagesResponse{}, internalErr).Times(1)
		resp, err := th.GetReplicationMessages(ctx, &replicator.GetReplicationMessagesRequest{})
		assert.Equal(t, replicator.GetReplicationMessagesResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("GetWorkflowExecutionRawHistoryV2", func(t *testing.T) {
		h.EXPECT().GetWorkflowExecutionRawHistoryV2(ctx, &types.GetWorkflowExecutionRawHistoryV2Request{}).Return(&types.GetWorkflowExecutionRawHistoryV2Response{}, internalErr).Times(1)
		resp, err := th.GetWorkflowExecutionRawHistoryV2(ctx, &admin.GetWorkflowExecutionRawHistoryV2Request{})
		assert.Equal(t, admin.GetWorkflowExecutionRawHistoryV2Response{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("MergeDLQMessages", func(t *testing.T) {
		h.EXPECT().MergeDLQMessages(ctx, &types.MergeDLQMessagesRequest{}).Return(&types.MergeDLQMessagesResponse{}, internalErr).Times(1)
		resp, err := th.MergeDLQMessages(ctx, &replicator.MergeDLQMessagesRequest{})
		assert.Equal(t, replicator.MergeDLQMessagesResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("PurgeDLQMessages", func(t *testing.T) {
		h.EXPECT().PurgeDLQMessages(ctx, &types.PurgeDLQMessagesRequest{}).Return(internalErr).Times(1)
		err := th.PurgeDLQMessages(ctx, &replicator.PurgeDLQMessagesRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("ReadDLQMessages", func(t *testing.T) {
		h.EXPECT().ReadDLQMessages(ctx, &types.ReadDLQMessagesRequest{}).Return(&types.ReadDLQMessagesResponse{}, internalErr).Times(1)
		resp, err := th.ReadDLQMessages(ctx, &replicator.ReadDLQMessagesRequest{})
		assert.Equal(t, replicator.ReadDLQMessagesResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("ReapplyEvents", func(t *testing.T) {
		h.EXPECT().ReapplyEvents(ctx, &types.ReapplyEventsRequest{}).Return(internalErr).Times(1)
		err := th.ReapplyEvents(ctx, &shared.ReapplyEventsRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("RefreshWorkflowTasks", func(t *testing.T) {
		h.EXPECT().RefreshWorkflowTasks(ctx, &types.RefreshWorkflowTasksRequest{}).Return(internalErr).Times(1)
		err := th.RefreshWorkflowTasks(ctx, &shared.RefreshWorkflowTasksRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("RemoveTask", func(t *testing.T) {
		h.EXPECT().RemoveTask(ctx, &types.RemoveTaskRequest{}).Return(internalErr).Times(1)
		err := th.RemoveTask(ctx, &shared.RemoveTaskRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("ResendReplicationTasks", func(t *testing.T) {
		h.EXPECT().ResendReplicationTasks(ctx, &types.ResendReplicationTasksRequest{}).Return(internalErr).Times(1)
		err := th.ResendReplicationTasks(ctx, &admin.ResendReplicationTasksRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("ResetQueue", func(t *testing.T) {
		h.EXPECT().ResetQueue(ctx, &types.ResetQueueRequest{}).Return(internalErr).Times(1)
		err := th.ResetQueue(ctx, &shared.ResetQueueRequest{})
		assert.Equal(t, expectedErr, err)
	})
	t.Run("GetCrossClusterTasks", func(t *testing.T) {
		h.EXPECT().GetCrossClusterTasks(ctx, &types.GetCrossClusterTasksRequest{}).Return(&types.GetCrossClusterTasksResponse{}, internalErr).Times(1)
		resp, err := th.GetCrossClusterTasks(ctx, &shared.GetCrossClusterTasksRequest{})
		assert.Equal(t, shared.GetCrossClusterTasksResponse{}, *resp)
		assert.Equal(t, expectedErr, err)
	})
}
