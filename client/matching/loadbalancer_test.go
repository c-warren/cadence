package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/types"
)

func setUpMocksForLoadBalancer(t *testing.T) (*defaultLoadBalancer, *MockPartitionConfigProvider) {
	ctrl := gomock.NewController(t)
	mockProvider := NewMockPartitionConfigProvider(ctrl)

	return &defaultLoadBalancer{
		provider: mockProvider,
	}, mockProvider
}

func Test_defaultLoadBalancer_PickWritePartition(t *testing.T) {
	testCases := []struct {
		name               string
		forwardedFrom      string
		taskListType       int
		nPartitions        int
		taskListKind       types.TaskListKind
		expectedPartitions []string
	}{
		{
			name:               "single write partition, forwarded",
			forwardedFrom:      "parent-task-list",
			taskListType:       0,
			nPartitions:        1,
			taskListKind:       types.TaskListKindNormal,
			expectedPartitions: []string{"test-task-list"},
		},
		{
			name:               "multiple write partitions, no forward",
			forwardedFrom:      "",
			taskListType:       0,
			nPartitions:        3,
			taskListKind:       types.TaskListKindNormal,
			expectedPartitions: []string{"test-task-list", "/__cadence_sys/test-task-list/1", "/__cadence_sys/test-task-list/2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up mocks
			loadBalancer, mockProvider := setUpMocksForLoadBalancer(t)

			mockProvider.EXPECT().
				GetNumberOfWritePartitions("test-domain-id", types.TaskList{Name: "test-task-list", Kind: &tc.taskListKind}, tc.taskListType).
				Return(tc.nPartitions).
				Times(1)

			// Pick write partition
			req := &types.AddDecisionTaskRequest{
				DomainUUID:    "test-domain-id",
				TaskList:      &types.TaskList{Name: "test-task-list", Kind: &tc.taskListKind},
				ForwardedFrom: tc.forwardedFrom,
			}
			partition := loadBalancer.PickWritePartition(tc.taskListType, req)

			// Validate result
			assert.Contains(t, tc.expectedPartitions, partition)
		})
	}
}

func Test_defaultLoadBalancer_PickReadPartition(t *testing.T) {
	testCases := []struct {
		name               string
		forwardedFrom      string
		taskListType       int
		nPartitions        int
		taskListKind       types.TaskListKind
		expectedPartitions []string
	}{
		{
			name:               "single read partition, forwarded",
			forwardedFrom:      "parent-task-list",
			taskListType:       0,
			nPartitions:        1,
			taskListKind:       types.TaskListKindNormal,
			expectedPartitions: []string{"test-task-list"},
		},
		{
			name:               "multiple read partitions, no forward",
			forwardedFrom:      "",
			taskListType:       0,
			nPartitions:        3,
			taskListKind:       types.TaskListKindNormal,
			expectedPartitions: []string{"test-task-list", "/__cadence_sys/test-task-list/1", "/__cadence_sys/test-task-list/2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up mocks
			loadBalancer, mockProvider := setUpMocksForLoadBalancer(t)

			mockProvider.EXPECT().
				GetNumberOfReadPartitions("test-domain-id", types.TaskList{Name: "test-task-list", Kind: &tc.taskListKind}, tc.taskListType).
				Return(tc.nPartitions).
				Times(1)

			// Pick read partition
			req := &types.AddDecisionTaskRequest{
				DomainUUID:    "test-domain-id",
				TaskList:      &types.TaskList{Name: "test-task-list", Kind: &tc.taskListKind},
				ForwardedFrom: tc.forwardedFrom,
			}
			partition := loadBalancer.PickReadPartition(tc.taskListType, req, "")

			// Validate result
			assert.Contains(t, tc.expectedPartitions, partition)
		})
	}
}

func Test_defaultLoadBalancer_UpdateWeight(t *testing.T) {
	t.Run("no-op for task list partitions", func(t *testing.T) {
		// Set up mocks
		loadBalancer, _ := setUpMocksForLoadBalancer(t)

		taskList := types.TaskList{Name: "test-task-list", Kind: types.TaskListKindNormal.Ptr()}

		// Call UpdateWeight, should do nothing
		req := &types.AddDecisionTaskRequest{
			DomainUUID: "test-domain-id",
			TaskList:   &taskList,
		}
		loadBalancer.UpdateWeight(0, req, "partition", nil)

		// No expectations, just ensure no-op
	})
}

func Test_defaultLoadBalancer_pickPartition(t *testing.T) {
	type args struct {
		taskList      types.TaskList
		forwardedFrom string
		nPartitions   int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test: nPartitions <= 0",
			args: args{
				taskList: types.TaskList{
					Name: "taskList4",
					Kind: types.TaskListKindNormal.Ptr(),
				},
				forwardedFrom: "",
				nPartitions:   0,
			},
			want: "taskList4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb := &defaultLoadBalancer{}
			got := lb.pickPartition(tt.args.taskList, tt.args.forwardedFrom, tt.args.nPartitions)
			assert.Equal(t, tt.want, got)
		})
	}
}
