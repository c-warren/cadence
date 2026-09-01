package event

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

var enabled = false

func init() {
	enabled = os.Getenv("MATCHING_LOG_EVENTS") == "true"
}

type E struct {
	persistence.TaskInfo
	TaskListName string
	TaskListKind *types.TaskListKind
	TaskListType int // persistence.TaskListTypeDecision or persistence.TaskListTypeActivity

	EventTime time.Time

	// EventName describes the event. It is used to query events in simulations so don't change existing event names.
	EventName string
	Host      string
	Payload   map[string]any
}

func Log(events ...E) {
	if !enabled {
		return
	}
	for _, e := range events {
		e.EventTime = time.Now()
		data, err := json.Marshal(e)
		if err != nil {
			fmt.Printf("failed to marshal event: %v", err)
		}

		fmt.Printf("Matching New Event: %s\n", data)
	}
}
