package collection

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	orderedMapSuite struct {
		*require.Assertions
		suite.Suite

		orderedMap OrderedMap
	}
)

func TestOrderedMapSuite(t *testing.T) {
	s := new(orderedMapSuite)
	suite.Run(t, s)
}

func (s *orderedMapSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.orderedMap = NewOrderedMap()
}

func (s *orderedMapSuite) TestBasicOperation() {
	key := "0001"
	value1 := boolType(true)
	value2 := intType(1234)

	s.Equal(0, s.orderedMap.Len())

	// remove a non-exist key
	s.orderedMap.Remove(key)

	// get a non-exist key
	v, ok := s.orderedMap.Get(key)
	s.False(ok)
	s.Nil(v)
	s.False(s.orderedMap.Contains(key))

	s.orderedMap.Put(key, value1)
	v, ok = s.orderedMap.Get(key)
	s.True(ok)
	s.Equal(value1, v)
	s.True(s.orderedMap.Contains(key))
	s.Equal(1, s.orderedMap.Len())

	s.orderedMap.Put(key, value2)
	v, ok = s.orderedMap.Get(key)
	s.True(ok)
	s.Equal(value2, v)
	s.True(s.orderedMap.Contains(key))
	s.Equal(1, s.orderedMap.Len())

	s.orderedMap.Remove(key)
	s.False(s.orderedMap.Contains(key))
	s.Equal(0, s.orderedMap.Len())
}

func (s *orderedMapSuite) TestIterator() {
	numItems := 10
	for i := 0; i != numItems; i++ {
		s.orderedMap.Put(i, intType(i))
	}

	iter := s.orderedMap.Iter()
	numIterated := 0
	for entry := range iter.Entries() {
		s.Equal(numIterated, entry.Key.(int))
		s.Equal(intType(numIterated), entry.Value.(intType))
		numIterated++
	}
	iter.Close()

	// remove all even numbers
	for i := 0; i != numItems; i++ {
		if i%2 == 0 {
			s.orderedMap.Remove(i)
		}
	}
	// add more odd numbers at the end
	for i := numItems; i != 2*numItems; i++ {
		if i%2 == 1 {
			s.orderedMap.Put(i, intType(i))
		}
	}

	iter = s.orderedMap.Iter()
	numIterated = 0
	for entry := range iter.Entries() {
		s.Equal(2*numIterated+1, entry.Key.(int))
		s.Equal(intType(2*numIterated+1), entry.Value.(intType))
		numIterated++
	}
	iter.Close()
}
