//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination events_reapplier_mock.go

package ndc

import (
	ctx "context"

	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/execution"
)

type (
	// EventsReapplier handles event re-application
	EventsReapplier interface {
		ReapplyEvents(
			ctx ctx.Context,
			msBuilder execution.MutableState,
			historyEvents []*types.HistoryEvent,
			runID string,
		) ([]*types.HistoryEvent, error)
	}

	eventsReapplierImpl struct {
		metricsClient metrics.Client
		logger        log.Logger
	}
)

var _ EventsReapplier = (*eventsReapplierImpl)(nil)

// NewEventsReapplier creates events reapplier
func NewEventsReapplier(
	metricsClient metrics.Client,
	logger log.Logger,
) EventsReapplier {

	return &eventsReapplierImpl{
		metricsClient: metricsClient,
		logger:        logger,
	}
}

func (r *eventsReapplierImpl) ReapplyEvents(
	ctx ctx.Context,
	msBuilder execution.MutableState,
	historyEvents []*types.HistoryEvent,
	runID string,
) ([]*types.HistoryEvent, error) {

	var reappliedEvents []*types.HistoryEvent
	for _, event := range historyEvents {
		switch event.GetEventType() {
		case types.EventTypeWorkflowExecutionSignaled:
			dedupResource := definition.NewEventReappliedID(runID, event.ID, event.Version)
			if msBuilder.IsResourceDuplicated(dedupResource) {
				// skip already applied event
				continue
			}
			reappliedEvents = append(reappliedEvents, event)
		}
	}

	if len(reappliedEvents) == 0 {
		return nil, nil
	}

	// sanity check workflow still running
	if !msBuilder.IsWorkflowExecutionRunning() {
		return nil, &types.InternalServiceError{
			Message: "unable to reapply events to closed workflow.",
		}
	}

	for _, event := range reappliedEvents {
		signal := event.GetWorkflowExecutionSignaledEventAttributes()
		if _, err := msBuilder.AddWorkflowExecutionSignaled(
			signal.GetSignalName(),
			signal.GetInput(),
			signal.GetIdentity(),
			"", // Do not set requestID for requests reapplied, because they have already been applied previously
		); err != nil {
			return nil, err
		}
		deDupResource := definition.NewEventReappliedID(runID, event.ID, event.Version)
		msBuilder.UpdateDuplicatedResource(deDupResource)
	}
	return reappliedEvents, nil
}
