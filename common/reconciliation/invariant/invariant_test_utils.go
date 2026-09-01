package invariant

import (
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

const (
	domainID     = "test-domain-id"
	domainName   = "test-domain-name"
	workflowID   = "test-workflow-id"
	runID        = "test-run-id"
	shardID      = 0
	treeID       = "test-tree-id"
	branchID     = "test-branch-id"
	openState    = persistence.WorkflowStateCreated
	closedState  = persistence.WorkflowStateCompleted
	currentRunID = "test-current-run-id"
)

var (
	branchToken = []byte{1, 2, 3}
)

func getOpenConcreteExecution() *entity.ConcreteExecution {
	return &entity.ConcreteExecution{
		Execution: entity.Execution{
			ShardID:    shardID,
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			State:      openState,
		},
		BranchToken: branchToken,
		TreeID:      treeID,
		BranchID:    branchID,
	}
}

func getClosedConcreteExecution() *entity.ConcreteExecution {
	return &entity.ConcreteExecution{
		Execution: entity.Execution{
			ShardID:    shardID,
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			State:      closedState,
		},
		BranchToken: branchToken,
		TreeID:      treeID,
		BranchID:    branchID,
	}
}

func getOpenCurrentExecution() *entity.CurrentExecution {
	return &entity.CurrentExecution{
		Execution: entity.Execution{
			ShardID:    shardID,
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			State:      openState,
		},
		CurrentRunID: currentRunID,
	}
}

func getClosedCurrentExecution() *entity.CurrentExecution {
	return &entity.CurrentExecution{
		Execution: entity.Execution{
			ShardID:    shardID,
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			State:      closedState,
		},
		CurrentRunID: currentRunID,
	}
}
