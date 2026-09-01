package pagination

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var fetchMap = map[PageToken][]Entity{
	0: nil,
	1: {},
	2: {"one", "two", "three"},
	3: {"four", "five", "six", "seven"},
	4: {"eight"},
}

type IteratorSuite struct {
	*require.Assertions
	suite.Suite
}

func TestIteratorSuite(t *testing.T) {
	suite.Run(t, new(IteratorSuite))
}

func (s *IteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *IteratorSuite) TestInitializedToEmpty() {
	fetchFn := func(_ context.Context, token PageToken) (Page, error) {
		if token.(int) == 2 {
			return Page{
				CurrentToken: token,
				NextToken:    nil,
				Entities:     nil,
			}, nil
		}
		return Page{
			CurrentToken: token,
			NextToken:    token.(int) + 1,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(context.Background(), 0, fetchFn)
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Equal(ErrIteratorFinished, err)
}

func (s *IteratorSuite) TestNonEmptyNoErrors() {
	fetchFn := func(_ context.Context, token PageToken) (Page, error) {
		var nextPageToken interface{} = token.(int) + 1
		if nextPageToken.(int) == 5 {
			nextPageToken = nil
		}
		return Page{
			CurrentToken: token,
			NextToken:    nextPageToken,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(context.Background(), 0, fetchFn)
	expectedResults := []string{"one", "two", "three", "four", "five", "six", "seven", "eight"}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		s.Equal(expectedResults[i], curr.(string))
		i++
	}
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Equal(ErrIteratorFinished, err)
}

func (s *IteratorSuite) TestNonEmptyWithErrors() {
	fetchFn := func(_ context.Context, token PageToken) (Page, error) {
		if token.(int) == 4 {
			return Page{}, errors.New("got error")
		}
		return Page{
			CurrentToken: token,
			NextToken:    token.(int) + 1,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(context.Background(), 0, fetchFn)
	expectedResults := []string{"one", "two", "three", "four", "five", "six", "seven"}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		s.Equal(expectedResults[i], curr.(string))
		i++
	}
	s.False(itr.HasNext())
	curr, err := itr.Next()
	s.Nil(curr)
	s.Equal("got error", err.Error())
}
