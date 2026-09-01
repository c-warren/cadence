package filestore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

type queryParserSuite struct {
	*require.Assertions
	suite.Suite

	parser QueryParser
}

func TestQueryParserSuite(t *testing.T) {
	suite.Run(t, new(queryParserSuite))
}

func (s *queryParserSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.parser = NewQueryParser()
}

func (s *queryParserSuite) TestParseWorkflowID_RunID_WorkflowType() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "WorkflowID = \"random workflowID\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: common.StringPtr("random workflowID"),
			},
		},
		{
			query:     "WorkflowID = \"random workflowID\" and WorkflowID = \"random workflowID\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: common.StringPtr("random workflowID"),
			},
		},
		{
			query:     "RunID = \"random runID\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				runID: common.StringPtr("random runID"),
			},
		},
		{
			query:     "WorkflowType = \"random typeName\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowTypeName: common.StringPtr("random typeName"),
			},
		},
		{
			query:     "WorkflowID = 'random workflowID'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: common.StringPtr("random workflowID"),
			},
		},
		{
			query:     "WorkflowType = 'random typeName' and WorkflowType = \"another typeName\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				emptyResult: true,
			},
		},
		{
			query:     "WorkflowType = 'random typeName' and (WorkflowID = \"random workflowID\" and RunID='random runID')",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID:       common.StringPtr("random workflowID"),
				runID:            common.StringPtr("random runID"),
				workflowTypeName: common.StringPtr("random typeName"),
			},
		},
		{
			query:     "runID = random workflowID",
			expectErr: true,
		},
		{
			query:     "WorkflowID = \"random workflowID\" or WorkflowID = \"another workflowID\"",
			expectErr: true,
		},
		{
			query:     "WorkflowID = \"random workflowID\" or runID = \"random runID\"",
			expectErr: true,
		},
		{
			query:     "workflowid = \"random workflowID\"",
			expectErr: true,
		},
		{
			query:     "runID > \"random workflowID\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.emptyResult, parsedQuery.emptyResult)
		if !tc.parsedQuery.emptyResult {
			s.Equal(tc.parsedQuery.workflowID, parsedQuery.workflowID)
			s.Equal(tc.parsedQuery.runID, parsedQuery.runID)
			s.Equal(tc.parsedQuery.workflowTypeName, parsedQuery.workflowTypeName)
		}
	}
}

func (s *queryParserSuite) TestParseCloseStatus() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "CloseStatus = \"Completed\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeStatus: types.WorkflowExecutionCloseStatusCompleted.Ptr(),
			},
		},
		{
			query:     "CloseStatus = 'continuedasnew'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeStatus: types.WorkflowExecutionCloseStatusContinuedAsNew.Ptr(),
			},
		},
		{
			query:     "CloseStatus = 'TIMED_OUT'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeStatus: types.WorkflowExecutionCloseStatusTimedOut.Ptr(),
			},
		},
		{
			query:     "CloseStatus = 'Failed' and CloseStatus = \"Failed\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeStatus: types.WorkflowExecutionCloseStatusFailed.Ptr(),
			},
		},
		{
			query:     "(CloseStatus = 'Timedout' and CloseStatus = \"canceled\")",
			expectErr: false,
			parsedQuery: &parsedQuery{
				emptyResult: true,
			},
		},
		{
			query:     "closeStatus = \"Failed\"",
			expectErr: true,
		},
		{
			query:     "CloseStatus = \"Failed\" or CloseStatus = \"Failed\"",
			expectErr: true,
		},
		{
			query:     "CloseStatus = \"unknown\"",
			expectErr: true,
		},
		{
			query:     "CloseStatus > \"Failed\"",
			expectErr: true,
		},
		{
			query:     "CloseStatus = 1",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeStatus: types.WorkflowExecutionCloseStatusFailed.Ptr(),
			},
		},
		{
			query:     "CloseStatus = 10",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.emptyResult, parsedQuery.emptyResult)
		if !tc.parsedQuery.emptyResult {
			s.Equal(tc.parsedQuery.closeStatus, parsedQuery.closeStatus)
		}
	}
}

func (s *queryParserSuite) TestParseCloseTime() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "CloseTime <= 1000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 0,
				latestCloseTime:   1000,
			},
		},
		{
			query:     "CloseTime < 2000 and CloseTime <= 1000 and CloseTime > 300",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 301,
				latestCloseTime:   1000,
			},
		},
		{
			query:     "CloseTime = 2000 and (CloseTime > 1000 and CloseTime <= 9999)",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 2000,
				latestCloseTime:   2000,
			},
		},
		{
			query:     "CloseTime <= \"2019-01-01T11:11:11Z\" and CloseTime >= 1000000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 1000000,
				latestCloseTime:   1546341071000000000,
			},
		},
		{
			query:     "closeTime = 2000",
			expectErr: true,
		},
		{
			query:     "CloseTime > \"2019-01-01 00:00:00\"",
			expectErr: true,
		},
		{
			query:     "CloseStatus > 2000 or CloseStatus < 1000",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.emptyResult, parsedQuery.emptyResult)
		if !tc.parsedQuery.emptyResult {
			s.Equal(tc.parsedQuery.earliestCloseTime, parsedQuery.earliestCloseTime)
			s.Equal(tc.parsedQuery.latestCloseTime, parsedQuery.latestCloseTime)
		}
	}
}

func (s *queryParserSuite) TestParse() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "CloseTime <= \"2019-01-01T11:11:11Z\" and WorkflowID = 'random workflowID'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 0,
				latestCloseTime:   1546341071000000000,
				workflowID:        common.StringPtr("random workflowID"),
			},
		},
		{
			query:     "CloseTime > 1999 and CloseTime < 10000 and RunID = 'random runID' and CloseStatus = 'Failed'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				earliestCloseTime: 2000,
				latestCloseTime:   9999,
				runID:             common.StringPtr("random runID"),
				closeStatus:       types.WorkflowExecutionCloseStatusFailed.Ptr(),
			},
		},
		{
			query:     "CloseTime > 2001 and CloseTime < 10000 and (RunID = 'random runID') and CloseStatus = 'Failed' and (RunID = 'another ID')",
			expectErr: false,
			parsedQuery: &parsedQuery{
				emptyResult: true,
			},
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.emptyResult, parsedQuery.emptyResult)
		if !tc.parsedQuery.emptyResult {
			s.Equal(tc.parsedQuery, parsedQuery)
		}
	}
}
