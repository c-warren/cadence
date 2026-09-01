package events

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

type (
	notifierSuite struct {
		suite.Suite
		*require.Assertions

		historyEventNotifier Notifier
	}
)

func TestHistoryEventNotifierSuite(t *testing.T) {
	s := new(notifierSuite)
	suite.Run(t, s)
}

func (s *notifierSuite) SetupSuite() {

}

func (s *notifierSuite) TearDownSuite() {

}

func (s *notifierSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.historyEventNotifier = NewNotifier(
		clock.NewRealTimeSource(),
		metrics.NewClient(tally.NoopScope, metrics.History, metrics.MigrationConfig{}),
		func(workflowID string) int {
			return len(workflowID)
		},
	)
	s.historyEventNotifier.Start()
}

func (s *notifierSuite) TearDownTest() {
	s.historyEventNotifier.Stop()
}

func (s *notifierSuite) TestSingleSubscriberWatchingEvents() {
	domainID := "domain ID"
	execution := &types.WorkflowExecution{
		WorkflowID: "workflow ID",
		RunID:      "run ID",
	}
	lastFirstEventID := int64(3)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := persistence.WorkflowStateCreated
	workflowCloseState := persistence.WorkflowCloseStatusNone
	versionHistory := persistence.VersionHistories{}
	historyEvent := NewNotification(
		domainID,
		execution,
		lastFirstEventID,
		nextEventID,
		previousStartedEventID,
		workflowState,
		workflowCloseState,
		&versionHistory,
	)
	timerChan := time.NewTimer(time.Second * 2).C

	subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowID(), execution.GetRunID()))
	s.Nil(err)

	go func() {
		<-timerChan
		s.historyEventNotifier.NotifyNewHistoryEvent(historyEvent)
	}()

	msg := <-channel
	s.Equal(historyEvent, msg)

	err = s.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowID(), execution.GetRunID()), subscriberID)
	s.Nil(err)
}

func (s *notifierSuite) TestMultipleSubscriberWatchingEvents() {
	domainID := "domain ID"
	execution := &types.WorkflowExecution{
		WorkflowID: "workflow ID",
		RunID:      "run ID",
	}

	lastFirstEventID := int64(3)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := persistence.WorkflowStateCreated
	workflowCloseState := persistence.WorkflowCloseStatusNone
	versionHistories := &persistence.VersionHistories{}
	historyEvent := NewNotification(domainID, execution, lastFirstEventID, nextEventID, previousStartedEventID, workflowState, workflowCloseState, versionHistories)
	timerChan := time.NewTimer(time.Second * 5).C

	subscriberCount := 100
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(subscriberCount)

	watchFunc := func() {
		subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowID(), execution.GetRunID()))
		s.Nil(err)

		timeourChan := time.NewTimer(time.Second * 10).C

		select {
		case msg := <-channel:
			s.Equal(historyEvent, msg)
		case <-timeourChan:
			s.Fail("subscribe to new events timeout")
		}
		err = s.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowID(), execution.GetRunID()), subscriberID)
		s.Nil(err)
		waitGroup.Done()
	}

	for count := 0; count < subscriberCount; count++ {
		go watchFunc()
	}

	<-timerChan
	s.historyEventNotifier.NotifyNewHistoryEvent(historyEvent)
	waitGroup.Wait()
}
