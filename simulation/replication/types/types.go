package types

import (
	"fmt"
	"testing"
	"time"
)

const (
	DefaultTestCase = "testdata/replication_simulation_default.yaml"
	TasklistName    = "test-tasklist"

	TimerInterval = 5 * time.Second
)

type OperationFunction func(t *testing.T, op *Operation, simCfg *ReplicationSimulationConfig) error

type WorkflowInput struct {
	Duration             time.Duration
	ActivityCount        int
	ChildWorkflowID      string
	ChildWorkflowTimeout time.Duration
}

type WorkflowOutput struct {
	Count int
}

type ReplicationSimulation struct {
	RunIDRegistry map[string]string
}

func NewReplicationSimulation() *ReplicationSimulation {
	return &ReplicationSimulation{
		RunIDRegistry: make(map[string]string),
	}
}

func (s *ReplicationSimulation) StoreRunID(key, runID string) error {
	if s.RunIDRegistry == nil {
		return fmt.Errorf("runIDRegistry is nil")
	}
	s.RunIDRegistry[key] = runID
	return nil
}

func (s *ReplicationSimulation) GetRunID(key string) (string, error) {
	if s.RunIDRegistry == nil {
		return "", fmt.Errorf("runIDRegistry is nil")
	}
	return s.RunIDRegistry[key], nil
}

func WorkerIdentityFor(clusterName string, domainName string) string {
	if domainName == "" {
		return fmt.Sprintf("worker-%s", clusterName)
	}
	return fmt.Sprintf("worker-%s-%s", domainName, clusterName)
}

func Logf(t *testing.T, msg string, args ...interface{}) {
	t.Helper()
	msg = time.Now().Format(time.RFC3339Nano) + "\t" + msg
	t.Logf(msg, args...)
}
