package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/types"
)

type QuerySuite struct {
	*require.Assertions
	suite.Suite
}

func TestQuerySuite(t *testing.T) {
	suite.Run(t, new(QuerySuite))
}

func (s *QuerySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *QuerySuite) TestValidateTerminationState() {
	testCases := []struct {
		ts        *TerminationState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult:     &types.WorkflowQueryResult{},
				Failure:         errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult: &types.WorkflowQueryResult{
					ResultType: types.QueryResultTypeAnswered.Ptr(),
				},
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult: &types.WorkflowQueryResult{
					ResultType:   types.QueryResultTypeAnswered.Ptr(),
					Answer:       []byte{1, 2, 3},
					ErrorMessage: "err",
				},
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult: &types.WorkflowQueryResult{
					ResultType: types.QueryResultTypeFailed.Ptr(),
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult: &types.WorkflowQueryResult{
					ResultType:   types.QueryResultTypeFailed.Ptr(),
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeCompleted,
				QueryResult: &types.WorkflowQueryResult{
					ResultType: types.QueryResultTypeAnswered.Ptr(),
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: false,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeUnblocked,
				QueryResult:     &types.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeUnblocked,
				Failure:         errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeFailed,
				QueryResult:     &types.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &TerminationState{
				TerminationType: TerminationTypeFailed,
				Failure:         errors.New("err"),
			},
			expectErr: false,
		},
	}

	queryImpl := &queryImpl{}
	for _, tc := range testCases {
		if tc.expectErr {
			s.Error(queryImpl.validateTerminationState(tc.ts))
		} else {
			s.NoError(queryImpl.validateTerminationState(tc.ts))
		}
	}
}

func (s *QuerySuite) TestTerminationState_Failed() {
	failedTerminationState := &TerminationState{
		TerminationType: TerminationTypeFailed,
		Failure:         errors.New("err"),
	}
	s.testSetTerminationState(failedTerminationState)
}

func (s *QuerySuite) TestTerminationState_Completed() {
	answeredTerminationState := &TerminationState{
		TerminationType: TerminationTypeCompleted,
		QueryResult: &types.WorkflowQueryResult{
			ResultType: types.QueryResultTypeAnswered.Ptr(),
			Answer:     []byte{1, 2, 3},
		},
	}
	s.testSetTerminationState(answeredTerminationState)
}

func (s *QuerySuite) TestTerminationState_Unblocked() {
	unblockedTerminationState := &TerminationState{
		TerminationType: TerminationTypeUnblocked,
	}
	s.testSetTerminationState(unblockedTerminationState)
}

func (s *QuerySuite) testSetTerminationState(terminationState *TerminationState) {
	query := newQuery(nil)
	ts, err := query.getTerminationState()
	s.Equal(errQueryNotInTerminalState, err)
	s.Nil(ts)
	s.False(closed(query.getQueryTermCh()))
	s.Equal(errTerminationStateInvalid, query.setTerminationState(nil))
	s.NoError(query.setTerminationState(terminationState))
	s.True(closed(query.getQueryTermCh()))
	actualTerminationState, err := query.getTerminationState()
	s.NoError(err)
	s.assertTerminationStateEqual(terminationState, actualTerminationState)
}

func (s *QuerySuite) assertTerminationStateEqual(expected *TerminationState, actual *TerminationState) {
	s.Equal(expected.TerminationType, actual.TerminationType)
	if expected.Failure != nil {
		s.Equal(expected.Failure.Error(), actual.Failure.Error())
	}
	if expected.QueryResult != nil {
		s.Equal(expected.QueryResult, actual.QueryResult)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
