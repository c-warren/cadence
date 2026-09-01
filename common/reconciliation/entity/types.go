package entity

import (
	"errors"
	"fmt"
	"time"

	"github.com/uber/cadence/common/persistence"
)

type (
	// Execution is a base type for executions which should be checked or fixed.
	Execution struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		State      int
	}

	// ConcreteExecution is a concrete execution.
	ConcreteExecution struct {
		BranchToken []byte
		TreeID      string
		BranchID    string
		Execution
	}

	// CurrentExecution is a current execution.
	CurrentExecution struct {
		CurrentRunID string
		Execution
	}

	// Timer is a timer scheduled to be fired
	Timer struct {
		ShardID             int
		WorkflowID          string
		DomainID            string
		RunID               string
		VisibilityTimestamp time.Time
		TaskID              int64
		TaskType            int
		TimeoutType         int
		EventID             int64
		ScheduleAttempt     int64
		Version             int64
	}
)

func (t *Timer) Validate() error {
	if t.ShardID < 0 {
		return fmt.Errorf("invalid ShardID: %v", t.ShardID)
	}
	if len(t.DomainID) == 0 {
		return errors.New("empty DomainID")
	}
	if len(t.WorkflowID) == 0 {
		return errors.New("empty WorkflowID")
	}
	if len(t.RunID) == 0 {
		return errors.New("empty RunID")
	}

	return nil
}

func (t *Timer) Clone() Entity {
	return &Timer{}
}

func (t *Timer) GetShardID() int {
	return t.ShardID
}

func (t *Timer) GetDomainID() string {
	return t.DomainID
}

// ValidateExecution returns an error if Execution is not valid, nil otherwise.
func validateExecution(execution *Execution) error {
	if execution.ShardID < 0 {
		return fmt.Errorf("invalid ShardID: %v", execution.ShardID)
	}
	if len(execution.DomainID) == 0 {
		return errors.New("empty DomainID")
	}
	if len(execution.WorkflowID) == 0 {
		return errors.New("empty WorkflowID")
	}
	if len(execution.RunID) == 0 {
		return errors.New("empty RunID")
	}
	if execution.State < persistence.WorkflowStateCreated || execution.State > persistence.WorkflowStateCorrupted {
		return fmt.Errorf("unknown workflow state: %v", execution.State)
	}
	return nil
}

// Validate returns an error if ConcreteExecution is not valid, nil otherwise.
func (ce *ConcreteExecution) Validate() error {
	err := validateExecution(&ce.Execution)
	if err != nil {
		return err
	}
	if len(ce.BranchToken) == 0 {
		return errors.New("empty BranchToken")
	}
	if len(ce.TreeID) == 0 {
		return errors.New("empty TreeID")
	}
	if len(ce.BranchID) == 0 {
		return errors.New("empty BranchID")
	}
	return nil

}

// Validate returns an error if CurrentExecution is not valid, nil otherwise.
func (curre *CurrentExecution) Validate() error {
	err := validateExecution(&curre.Execution)
	if err != nil {
		return err
	}
	if len(curre.CurrentRunID) == 0 {
		return errors.New("empty CurrentRunID")
	}
	return nil
}

// Clone will return a new copy of ConcreteExecution
func (ConcreteExecution) Clone() Entity {
	return &ConcreteExecution{}
}

// Clone will return a new copy of CurrentExecution
func (CurrentExecution) Clone() Entity {
	return &CurrentExecution{}
}

// GetShardID returns shard id
func (ce *ConcreteExecution) GetShardID() int {
	return ce.Execution.ShardID
}

// GetShardID returns shard id
func (curre *CurrentExecution) GetShardID() int {
	return curre.Execution.ShardID
}

// GetDomainID returns the domain id
func (ce *ConcreteExecution) GetDomainID() string {
	return ce.DomainID
}

// GetDomainID returns the domain id
func (curre *CurrentExecution) GetDomainID() string {
	return curre.DomainID
}

// Entity allows to deserialize and validate different type of executions
type Entity interface {
	Validate() error
	Clone() Entity
	GetShardID() int
	GetDomainID() string
}
