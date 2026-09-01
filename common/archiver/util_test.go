package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/types"
)

type UtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (s *UtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *UtilSuite) TestHistoryMutated() {
	testCases := []struct {
		historyBatches []*types.History
		request        *ArchiveHistoryRequest
		isLast         bool
		isMutated      bool
	}{
		{
			historyBatches: []*types.History{
				{
					Events: []*types.HistoryEvent{
						{
							Version: 15,
						},
					},
				},
			},
			request: &ArchiveHistoryRequest{
				CloseFailoverVersion: 3,
			},
			isMutated: true,
		},
		{
			historyBatches: []*types.History{
				{
					Events: []*types.HistoryEvent{
						{
							ID:      33,
							Version: 10,
						},
					},
				},
				{
					Events: []*types.HistoryEvent{
						{
							ID:      49,
							Version: 10,
						},
						{
							ID:      50,
							Version: 10,
						},
					},
				},
			},
			request: &ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isLast:    true,
			isMutated: true,
		},
		{
			historyBatches: []*types.History{
				{
					Events: []*types.HistoryEvent{
						{
							Version: 9,
						},
					},
				},
			},
			request: &ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
			},
			isLast:    true,
			isMutated: true,
		},
		{
			historyBatches: []*types.History{
				{
					Events: []*types.HistoryEvent{
						{
							ID:      20,
							Version: 10,
						},
					},
				},
				{
					Events: []*types.HistoryEvent{
						{
							ID:      33,
							Version: 10,
						},
					},
				},
			},
			request: &ArchiveHistoryRequest{
				CloseFailoverVersion: 10,
				NextEventID:          34,
			},
			isLast:    true,
			isMutated: false,
		},
	}
	for _, tc := range testCases {
		s.Equal(tc.isMutated, IsHistoryMutated(tc.request, tc.historyBatches, tc.isLast, testlogger.New(s.T())))
	}
}
