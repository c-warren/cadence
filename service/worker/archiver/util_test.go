package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

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

func (s *UtilSuite) TestHashDeterminism() {
	testCases := []struct {
		instance interface{}
	}{
		{
			instance: "some random string",
		},
		{
			instance: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			instance: []string{"value1", "value2", "value3"},
		},
		{
			instance: ArchiveRequest{
				DomainID:    "some random domainID",
				ShardID:     0,
				BranchToken: []byte{1, 2, 3},
				NextEventID: int64(123),
				CloseStatus: types.WorkflowExecutionCloseStatusContinuedAsNew,
				Memo: &types.Memo{
					Fields: map[string][]byte{
						"memoKey1": []byte{1, 2, 3},
						"memoKey2": []byte{4, 5, 6},
					},
				},
				SearchAttributes: map[string][]byte{
					"customKey1": []byte{1, 2, 3},
					"customKey2": []byte{4, 5, 6},
				},
				Targets: []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
			},
		},
	}

	for _, tc := range testCases {
		expectedHash := hash(tc.instance)
		for i := 0; i != 100; i++ {
			s.Equal(expectedHash, hash(tc.instance))
		}
	}
}

func (s *UtilSuite) TestHashesEqual() {
	testCases := []struct {
		a     []uint64
		b     []uint64
		equal bool
	}{
		{
			a:     nil,
			b:     nil,
			equal: true,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2, 3},
			equal: true,
		},
		{
			a:     []uint64{1, 2},
			b:     []uint64{1, 2, 3},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: true,
		},
		{
			a:     []uint64{1, 2, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.equal, hashesEqual(tc.a, tc.b))
	}
}
