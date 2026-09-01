package liveness

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/clock"
)

type (
	livenessSuite struct {
		suite.Suite
		*require.Assertions

		timeSource   clock.MockedTimeSource
		ttl          time.Duration
		shutdownFlag int32
		liveness     *Liveness
	}
)

func TestLivenessSuite(t *testing.T) {
	s := new(livenessSuite)
	suite.Run(t, s)
}

func (s *livenessSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ttl = 500 * time.Millisecond
	s.timeSource = clock.NewMockedTimeSource()
	s.shutdownFlag = 0
	s.liveness = NewLiveness(s.timeSource, s.ttl, func() {
		atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1)
		s.liveness.Stop()
	})
}

func (s *livenessSuite) TestIsAlive_No() {
	s.timeSource.Advance(s.ttl)
	alive := s.liveness.IsAlive()
	s.False(alive)
}

func (s *livenessSuite) TestIsAlive_Yes() {
	s.timeSource.Advance(s.ttl / 2)
	alive := s.liveness.IsAlive()
	s.True(alive)
}

func (s *livenessSuite) TestMarkAlive_Noop() {
	lastEventTime := s.liveness.lastEventTimeNano
	// not advanding time so markAlive will be a noop
	s.liveness.MarkAlive()
	s.Equal(lastEventTime, s.liveness.lastEventTimeNano)
}

func (s *livenessSuite) TestMarkAlive_Updated() {
	s.timeSource.Advance(time.Duration(1))
	s.liveness.MarkAlive()
	s.Equal(s.timeSource.Now().UnixNano(), s.liveness.lastEventTimeNano)
}

func (s *livenessSuite) TestEventLoop_Noop() {
	s.liveness.Start()
	defer s.liveness.Stop()

	// advance time ttl/2 and mark alive
	s.timeSource.Advance(s.ttl / 2)
	s.liveness.MarkAlive()
	s.True(s.liveness.IsAlive())

	// advance time ttl/2 more and validate still alive
	s.timeSource.Advance(s.ttl / 2)
	time.Sleep(100 * time.Millisecond) // give event loop time to run
	s.True(s.liveness.IsAlive())
	s.Equal(int32(0), atomic.LoadInt32(&s.shutdownFlag))
}

func (s *livenessSuite) TestEventLoop_Shutdown() {
	s.liveness.Start()
	defer s.liveness.Stop()

	s.timeSource.Advance(s.ttl)
	<-s.liveness.stopCh
	s.Equal(int32(1), atomic.LoadInt32(&s.shutdownFlag))
}
