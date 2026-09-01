package collection

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	concurrentQueueSuite struct {
		*require.Assertions
		suite.Suite

		concurrentQueue *concurrentQueueImpl[int]
	}
)

func TestConcurrentQueueSuite(t *testing.T) {
	s := new(concurrentQueueSuite)
	suite.Run(t, s)
}

func (s *concurrentQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.concurrentQueue = NewConcurrentQueue[int]().(*concurrentQueueImpl[int])
}

func (s *concurrentQueueSuite) TearDownTest() {
}

func (s *concurrentQueueSuite) TestAddAndRemove() {
	s.Equal(0, s.concurrentQueue.Len())
	s.True(s.concurrentQueue.IsEmpty())
	_, err := s.concurrentQueue.Peek()
	s.Error(err)
	_, err = s.concurrentQueue.Remove()
	s.Error(err)

	numItems := 100
	items := make([]int, 0, numItems)
	for i := 0; i != 100; i++ {
		num := rand.Int()
		items = append(items, num)
		s.concurrentQueue.Add(num)
		s.Equal(i+1, s.concurrentQueue.Len())
	}
	s.False(s.concurrentQueue.IsEmpty())
	num, err := s.concurrentQueue.Peek()
	s.NoError(err)
	s.Equal(items[0], num)

	for i := 0; i != 100; i++ {
		num, err := s.concurrentQueue.Remove()
		s.NoError(err)
		s.Equal(items[i], num)
		s.Equal(numItems-i-1, s.concurrentQueue.Len())
	}
	s.True(s.concurrentQueue.IsEmpty())
	_, err = s.concurrentQueue.Peek()
	s.Error(err)
	_, err = s.concurrentQueue.Remove()
	s.Error(err)
}

func (s *concurrentQueueSuite) TestMultipleProducer() {
	concurrency := 10
	numItemsPerProducer := 10

	var wg sync.WaitGroup
	for i := 0; i != concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j != numItemsPerProducer; j++ {
				s.concurrentQueue.Add(rand.Int())
			}
		}()
	}
	wg.Wait()

	expectedLength := concurrency * numItemsPerProducer
	s.Equal(expectedLength, s.concurrentQueue.Len())
	s.False(s.concurrentQueue.IsEmpty())
	for i := 0; i != expectedLength; i++ {
		_, _ = s.concurrentQueue.Remove()
	}
}

func BenchmarkConcurrentQueue(b *testing.B) {
	queue := NewConcurrentQueue[testTask]()

	for i := 0; i < 100; i++ {
		go send(queue)
	}

	for n := 0; n < b.N; n++ {
		remove(queue)
	}
}
