package persistence

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/types"
)

type (
	workflowStateCloseStatusSuite struct {
		suite.Suite
	}
)

func TestWorkflowStateCloseStatusSuite(t *testing.T) {
	s := new(workflowStateCloseStatusSuite)
	suite.Run(t, s)
}

func (s *workflowStateCloseStatusSuite) SetupSuite() {
}

func (s *workflowStateCloseStatusSuite) TearDownSuite() {

}

func (s *workflowStateCloseStatusSuite) SetupTest() {

}

func (s *workflowStateCloseStatusSuite) TearDownTest() {

}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateCreated() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateCreated, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateCreated, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateRunning() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateRunning, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateRunning, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateCompleted() {
	closeStatuses := []int{
		WorkflowCloseStatusNone,
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateCompleted, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateZombie() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateZombie, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateCloseStatus(WorkflowStateZombie, closeStatus))
	}
}

// TODO

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateCreated() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateCreated, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateCreated, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateRunning() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateRunning, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateRunning, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateCompleted() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.NotNil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateCompleted, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.Nil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateCompleted, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateZombie() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateZombie, WorkflowCloseStatusNone))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateCloseStatus(WorkflowStateZombie, closeStatus))
	}
}

func (s *workflowStateCloseStatusSuite) TestInternalMapping() {
	s.Nil(ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusNone))
	s.Equal(types.WorkflowExecutionCloseStatusCompleted.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusCompleted))
	s.Equal(types.WorkflowExecutionCloseStatusFailed.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusFailed))
	s.Equal(types.WorkflowExecutionCloseStatusCanceled.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusCanceled))
	s.Equal(types.WorkflowExecutionCloseStatusTerminated.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusTerminated))
	s.Equal(types.WorkflowExecutionCloseStatusContinuedAsNew.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusContinuedAsNew))
	s.Equal(types.WorkflowExecutionCloseStatusTimedOut.Ptr(), ToInternalWorkflowExecutionCloseStatus(WorkflowCloseStatusTimedOut))

	s.Equal(WorkflowCloseStatusNone, FromInternalWorkflowExecutionCloseStatus(nil))
	s.Equal(WorkflowCloseStatusCompleted, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusCompleted.Ptr()))
	s.Equal(WorkflowCloseStatusFailed, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusFailed.Ptr()))
	s.Equal(WorkflowCloseStatusCanceled, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusCanceled.Ptr()))
	s.Equal(WorkflowCloseStatusTerminated, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusTerminated.Ptr()))
	s.Equal(WorkflowCloseStatusContinuedAsNew, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusContinuedAsNew.Ptr()))
	s.Equal(WorkflowCloseStatusTimedOut, FromInternalWorkflowExecutionCloseStatus(types.WorkflowExecutionCloseStatusTimedOut.Ptr()))
}
