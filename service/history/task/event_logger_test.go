package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
)

type (
	eventLoggerSuite struct {
		*require.Assertions
		suite.Suite

		mockLogger *log.MockLogger

		eventLogger *eventLoggerImpl
	}
)

func TestEventLoggerSuite(t *testing.T) {
	s := new(eventLoggerSuite)
	suite.Run(t, s)
}

func (s *eventLoggerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.mockLogger = log.NewMockLogger(gomock.NewController(s.T()))

	s.eventLogger = newEventLogger(
		s.mockLogger,
		clock.NewRealTimeSource(),
		defaultTaskEventLoggerSize,
	).(*eventLoggerImpl)
}

func (s *eventLoggerSuite) TestAddEvent() {
	for i := 0; i != defaultTaskEventLoggerSize*2; i++ {
		s.eventLogger.AddEvent("some random event", i)
		s.Equal((i+1)%defaultTaskEventLoggerSize, s.eventLogger.nextEventIdx)
	}

	for i := 0; i != defaultTaskEventLoggerSize; i++ {
		// check if old events got overwritten
		s.Equal(i+defaultTaskEventLoggerSize, s.eventLogger.events[i].details[0])
	}

	s.Len(s.eventLogger.events, defaultTaskEventLoggerSize)
}

func (s *eventLoggerSuite) TestFlushEvents() {
	for _, numEvents := range []int{0, defaultTaskEventLoggerSize / 2, defaultTaskEventLoggerSize, defaultTaskEventLoggerSize * 2} {
		for i := 0; i != numEvents; i++ {
			s.eventLogger.AddEvent("some random event")
		}

		expectedEventsFlushed := min(numEvents, defaultTaskEventLoggerSize)
		s.mockLogger.EXPECT().Info(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

		s.Equal(expectedEventsFlushed, s.eventLogger.FlushEvents("some random message"))
	}
}
