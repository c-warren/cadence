package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence"
)

const (
	domainID   = "test-domain-id"
	workflowID = "test-workflow-id"
	runID      = "test-run-id"
	treeID     = "test-tree-id"
	branchID   = "test-branch-id"
)

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(TypeSuite))
}

type TypeSuite struct {
	*require.Assertions
	suite.Suite
}

func (t *TypeSuite) SetupTest() {
	t.Assertions = require.New(t.T())
}

func (t *TypeSuite) TestValidateExecution() {
	testCases := []struct {
		execution   *ConcreteExecution
		expectError bool
	}{
		{
			execution:   &ConcreteExecution{},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: -1,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: 0,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:  0,
					DomainID: domainID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated - 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCorrupted + 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		err := tc.execution.Validate()
		if tc.expectError {
			t.Error(err)
		} else {
			t.NoError(err)
		}
	}
}
