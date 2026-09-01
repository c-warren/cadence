package testing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/types"
)

type (
	historyEventTestSuit struct {
		suite.Suite
		generator Generator
	}
)

func TestHistoryEventTestSuite(t *testing.T) {
	suite.Run(t, new(historyEventTestSuit))
}

func (s *historyEventTestSuit) SetupSuite() {
	s.generator = InitializeHistoryEventGenerator("domain", 1)
}

func (s *historyEventTestSuit) SetupTest() {
	s.generator.Reset()
}

// This is a sample about how to use the generator
func (s *historyEventTestSuit) Test_HistoryEvent_Generator() {
	maxEventID := int64(0)
	maxVersion := int64(1)
	maxTaskID := int64(1)
	for i := 0; i < 10 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()

		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*types.HistoryEvent)
			if maxEventID != event.ID-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.ID
			if maxVersion > event.Version {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.Version
			if maxTaskID > event.TaskID {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.TaskID
			fmt.Println(e.GetName())
			fmt.Println(event.ID)
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertices())
	fmt.Println("==========================")
	branchGenerator1 := s.generator.DeepCopy()
	for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
		events := branchGenerator1.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*types.HistoryEvent)
			if maxEventID != event.ID-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.ID
			if maxVersion > event.Version {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.Version
			if maxTaskID > event.TaskID {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.TaskID
			fmt.Println(e.GetName())
			fmt.Println(event.ID)
		}
	}
	fmt.Println("==========================")
	history := s.generator.ListGeneratedVertices()
	maxEventID = history[len(history)-1].GetData().(*types.HistoryEvent).ID
	for i := 0; i < 10 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*types.HistoryEvent)
			if maxEventID != event.ID-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.ID
			if maxVersion > event.Version {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.Version
			if maxTaskID > event.TaskID {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.TaskID
			fmt.Println(e.GetName())
			fmt.Println(event.ID)
		}
	}
}
