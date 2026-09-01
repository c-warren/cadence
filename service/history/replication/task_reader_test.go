package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

const (
	testBatchSize = 50
)

var (
	testTime = time.Now()

	testReplicationTasks = []persistence.Task{
		&persistence.HistoryReplicationTask{TaskData: persistence.TaskData{TaskID: 50, VisibilityTimestamp: testTime.Add(-1 * time.Second)}},
		&persistence.HistoryReplicationTask{TaskData: persistence.TaskData{TaskID: 51, VisibilityTimestamp: testTime.Add(-2 * time.Second)}},
	}
)

func TestTaskReader(t *testing.T) {
	tests := []struct {
		name              string
		prepareExecutions func(m *persistence.MockExecutionManager)
		readLevel         int64
		maxReadLevel      int64
		expectResponse    []persistence.Task
		expectErr         string
	}{
		{
			name:         "read replication tasks - first read will use default batch size",
			readLevel:    50,
			maxReadLevel: 100,
			prepareExecutions: func(m *persistence.MockExecutionManager) {
				m.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
					TaskCategory:        persistence.HistoryTaskCategoryReplication,
					InclusiveMinTaskKey: persistence.NewImmediateTaskKey(51),
					ExclusiveMaxTaskKey: persistence.NewImmediateTaskKey(101),
					PageSize:            testBatchSize,
					ShardID:             common.Ptr(testShardID),
				}).Return(&persistence.GetHistoryTasksResponse{Tasks: testReplicationTasks}, nil)
			},
			expectResponse: testReplicationTasks,
		},
		{
			name:         "do not hit persistence when no task will be returned",
			readLevel:    50,
			maxReadLevel: 50,
			prepareExecutions: func(m *persistence.MockExecutionManager) {
				m.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Times(0)
			},
			expectResponse: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			em := persistence.NewMockExecutionManager(ctrl)
			tt.prepareExecutions(em)

			reader := NewTaskReader(testShardID, em)
			response, _, err := reader.Read(context.Background(), tt.readLevel, tt.maxReadLevel, testBatchSize)

			if tt.expectErr != "" {
				assert.EqualError(t, err, tt.expectErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResponse, response)
			}
		})
	}
}
